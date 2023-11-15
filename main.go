package main

import (
	plex "github.com/scalecraft/plex-db/cmd"
)

func main() {
	// config1 := replicator.Config{
	// 	Url:             "postgres://postgres:thisisnotarealpassword@localhost:54321/postgres?replication=database",
	// 	OutputPlugin:    "pgoutput",
	// 	PublicationName: "plexdb_replication",
	// 	SlotName:        "plexdb_replication",
	// 	PluginArguments: []string{
	// 		"proto_version '2'",
	// 		"publication_names 'plexdb_replication'",
	// 		"messages 'true'",
	// 		"streaming 'true'",
	// 	},
	// }

	// config2 := replicator.Config{
	// 	Url:             "postgres://postgres:thisisnotarealpassword@localhost:54322/postgres?replication=database",
	// 	OutputPlugin:    "pgoutput",
	// 	PublicationName: "plexdb_replication",
	// 	SlotName:        "plexdb_replication",
	// 	PluginArguments: []string{
	// 		"proto_version '2'",
	// 		"publication_names 'plexdb_replication'",
	// 		"messages 'true'",
	// 		"streaming 'true'",
	// 	},
	// }

	// config3 := replicator.Config{
	// 	Url:             "postgres://postgres:thisisnotarealpassword@localhost:54323/postgres?replication=database",
	// 	OutputPlugin:    "pgoutput",
	// 	PublicationName: "plexdb_replication",
	// 	SlotName:        "plexdb_replication",
	// 	PluginArguments: []string{
	// 		"proto_version '2'",
	// 		"publication_names 'plexdb_replication'",
	// 		"messages 'true'",
	// 		"streaming 'true'",
	// 	},
	// }

	// replicator.Replicate(&config1)
	// replicator.Replicate(&config2)
	// replicator.Replicate(&config3)
	plex.Execute()
}
