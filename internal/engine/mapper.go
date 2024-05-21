package engine

import (
	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/internal/pg"
)

func (p *ParsedQuery) Map(cfg *config.Config) error {
	if p.Source.Engine == "postgres" {
		result, err := pg.ExecuteRaw(p.QueryString, cfg, p.Source)
		if err != nil {
			return err
		}
		p.NodeResults = append(p.NodeResults, result...)
	}

	return nil
}
