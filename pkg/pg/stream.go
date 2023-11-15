package pg

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/scalecraft/plex-db/pkg/config"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

type handler struct {
	walData   []byte
	relations map[uint32]*pglogrepl.RelationMessageV2
	typeMap   *pgtype.Map
	inStream  *bool
}

type replicator struct {
	conn       *pgconn.PgConn
	cfg        config.Config
	rs         *pglogrepl.CreateReplicationSlotResult
	si         *pglogrepl.IdentifySystemResult
	ch         chan map[string]interface{}
	wg         *sync.WaitGroup
	sourceName string
	handler    handler
}

func Stream(cfg *config.Config, url string, ch chan map[string]interface{}, sourceName string) {
	var r replicator

	r.cfg = *cfg
	r.sourceName = sourceName
	r.ch = ch
	r.wg = &sync.WaitGroup{}
	r.conn = connect(url)
	r.createPublication()
	r.createReplicationSlot()
	r.startReplication()
	r.stream()
}

func (r *replicator) createPublication() {
	result := r.conn.Exec(
		context.Background(),
		fmt.Sprintf("drop publication if exists %s;", r.cfg.Method.Options.Publication),
	)
	_, err := result.ReadAll()

	if err != nil {
		log.Fatalf("Failed to drop publication %s", err)
	}

	result = r.conn.Exec(
		context.Background(),
		fmt.Sprintf("create publication %s for all tables", r.cfg.Method.Options.Publication),
	)
	_, err = result.ReadAll()

	if err != nil {
		log.Fatalf("Failed to create publication %s", err)
	}
}

func (r *replicator) createReplicationSlot() {

	// Put the database into streaming replication protocol
	// https://www.postgresql.org/docs/current/protocol-replication.html
	sysident, err := pglogrepl.IdentifySystem(context.Background(), r.conn)

	if err != nil {
		log.Fatalln("IdentifySystem failed:", err)
	}
	r.si = &sysident

	log.Println("SystemID:", r.si.SystemID, "Timeline:", r.si.Timeline, "XLogPos:", r.si.XLogPos, "DBName:", r.si.DBName)

	replicationSlotOptions := pglogrepl.CreateReplicationSlotOptions{
		Temporary: r.cfg.Method.Options.ReplicationSlotOptions.Temporary,
		Mode:      pglogrepl.LogicalReplication,
	}

	rs, err := pglogrepl.CreateReplicationSlot(
		context.Background(),
		r.conn,
		r.cfg.Method.Options.ReplicationSlotOptions.Name,
		r.cfg.Method.Options.Plugin,
		replicationSlotOptions,
	)

	if err != nil {
		log.Fatalf("Failed to create replication slot %s", err)

	}
	r.rs = &rs
}

func (r *replicator) startReplication() {
	// Read the snapshot from the replication slot
	err := pglogrepl.StartReplication(
		context.Background(),
		r.conn,
		r.cfg.Method.Options.ReplicationSlotOptions.Name,
		r.si.XLogPos,
		pglogrepl.StartReplicationOptions{PluginArgs: []string{
			"proto_version '2'",
			"publication_names 'plexdb_replication'",
			"messages 'true'",
			"streaming 'true'",
		}},
	)

	if err != nil {
		log.Fatalf("Failed to start replication %s", err)
	}
}

func decodeTextColumnData(mi *pgtype.Map, data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := mi.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

func parseColumns(columns []*pglogrepl.TupleDataColumn, message *pglogrepl.RelationMessageV2, typeMap *pgtype.Map) map[string]interface{} {
	values := map[string]interface{}{}

	for idx, col := range columns {
		colName := message.Columns[idx].Name
		switch col.DataType {
		case 'n': // null
			values[colName] = nil
		case 'u': // unchanged toast
			values[colName] = "Toast value not changed"
		case 't': //text
			val, err := decodeTextColumnData(typeMap, col.Data, message.Columns[idx].DataType)
			if err != nil {
				log.Fatalln("error decoding column data:", err)
			}
			values[colName] = val
		}
	}
	return values
}

func (r *replicator) process() {
	// Postgres logical replication message formats
	// https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
	logicalMsg, err := pglogrepl.ParseV2(r.handler.walData, *r.handler.inStream)

	if err != nil {
		log.Fatalf("Failed to parse logical replication message: %s", err)
	}

	switch logicalMsg := logicalMsg.(type) {

	case *pglogrepl.RelationMessageV2:
		r.handler.relations[logicalMsg.RelationID] = logicalMsg

	case *pglogrepl.BeginMessage:
		// Indicates the beginning of a group of changes in a transaction.
		// This is only sent for committed transactions.
		// You won't get any events from rolled back transactions.

	case *pglogrepl.CommitMessage:

	case *pglogrepl.InsertMessageV2:
		rel, ok := r.handler.relations[logicalMsg.RelationID]
		values := map[string]interface{}{}
		values["messageType"] = "insert"
		values["sourceName"] = r.sourceName

		if !ok {
			log.Fatalf("unknown relation ID %d", logicalMsg.RelationID)
		}

		values = parseColumns(logicalMsg.Tuple.Columns, rel, r.handler.typeMap)
		values["messageType"] = "insert"
		values["sourceName"] = r.sourceName

		r.ch <- values

	case *pglogrepl.UpdateMessageV2:
		rel, ok := r.handler.relations[logicalMsg.RelationID]
		values := map[string]interface{}{}
		values["messageType"] = "update"
		values["sourceName"] = r.sourceName

		if !ok {
			log.Fatalf("Unknown relation ID %d", logicalMsg.RelationID)
		}

		values = parseColumns(logicalMsg.NewTuple.Columns, rel, r.handler.typeMap)
		values["messageType"] = "update"
		values["sourceName"] = r.sourceName

		r.ch <- values

	case *pglogrepl.DeleteMessageV2:
		log.Printf("delete for xid %d\n", logicalMsg.Xid)
		// ...

	case *pglogrepl.TruncateMessageV2:
		log.Printf("truncate for xid %d\n", logicalMsg.Xid)
		// ...

	default:
		log.Printf("Unknown message type in pgoutput stream: %T", logicalMsg)
	}
}

func (r *replicator) stream() {

	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)
	r.handler.relations = map[uint32]*pglogrepl.RelationMessageV2{}
	r.handler.typeMap = pgtype.NewMap()

	// whenever we get StreamStartMessage we set inStream to true and then pass it to DecodeV2 function
	// 	on StreamStopMessage we set it back to false
	inStream := false
	r.handler.inStream = &inStream

	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			err := pglogrepl.SendStandbyStatusUpdate(
				context.Background(),
				r.conn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: r.si.XLogPos},
			)
			if err != nil {
				log.Fatalln("SendStandbyStatusUpdate failed:", err)
			}
			log.Printf("Sent Standby status message at %s\n", r.si.XLogPos.String())
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
		rawMsg, err := r.conn.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			log.Fatalln("ReceiveMessage failed:", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			log.Fatalf("received Postgres WAL error: %+v", errMsg)
		}

		msg := rawMsg.(*pgproto3.CopyData)

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				log.Fatalln("ParsePrimaryKeepaliveMessage failed:", err)
			}
			log.Println(
				"Primary Keepalive Message =>", "ServerWALEnd:",
				pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime,
				"ReplyRequested:", pkm.ReplyRequested,
			)
			if pkm.ServerWALEnd > r.si.XLogPos {
				r.si.XLogPos = pkm.ServerWALEnd
			}
			if pkm.ReplyRequested {
				nextStandbyMessageDeadline = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])

			if err != nil {
				log.Fatalln("ParseXLogData failed:", err)
			}

			r.handler.walData = xld.WALData

			log.Printf(
				"XLogData => WALStart %s ServerWALEnd %s ServerTime %s WALData:\n",
				xld.WALStart, xld.ServerWALEnd, xld.ServerTime,
			)
			r.process()

			if xld.WALStart > r.si.XLogPos {
				r.si.XLogPos = xld.WALStart
			}
		}
	}
}
