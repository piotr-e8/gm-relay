package main

import (
	"context"

	"github.com/fiatjaf/eventstore/sqlite3"
	"github.com/nbd-wtf/go-nostr"
)

// MySQLiteBackend opakowuje oryginalny SQLite3Backend
type MySQLiteBackend struct {
	*sqlite3.SQLite3Backend
}

// Nadpisujemy queryEventsSql bez limitu kinds
func (b MySQLiteBackend) queryEventsSql(filter nostr.Filter, doCount bool) (string, []any, error) {
	conditions := make([]string, 0, 7)
	params := make([]any, 0, 20)

	// IDs
	if len(filter.IDs) > 0 {
		for _, v := range filter.IDs {
			params = append(params, v)
		}
		conditions = append(conditions, `id IN (`+sqlite3.MakePlaceHolders(len(filter.IDs))+`)`)
	}

	// Authors
	if len(filter.Authors) > 0 {
		for _, v := range filter.Authors {
			params = append(params, v)
		}
		conditions = append(conditions, `pubkey IN (`+sqlite3.MakePlaceHolders(len(filter.Authors))+`)`)
	}

	// Kinds - **tu już nie ma limitu 10**
	if len(filter.Kinds) > 0 {
		for _, v := range filter.Kinds {
			params = append(params, v)
		}
		conditions = append(conditions, `kind IN (`+sqlite3.MakePlaceHolders(len(filter.Kinds))+`)`)
	}

	// Tags, Since, Until, Search - możesz przepisać tak samo jak oryginał
	// ...

	// Limit
	limit := b.QueryLimit
	if filter.Limit > 0 && filter.Limit <= b.QueryLimit {
		limit = filter.Limit
	}
	params = append(params, limit)

	// Tworzymy zapytanie SQL
	var query string
	if doCount {
		query = `SELECT COUNT(*) FROM event WHERE ` + strings.Join(conditions, " AND ") + " LIMIT ?"
	} else {
		query = `SELECT id, pubkey, created_at, kind, tags, content, sig FROM event WHERE ` +
			strings.Join(conditions, " AND ") + " ORDER BY created_at DESC, id LIMIT ?"
	}

	return query, params, nil
}

// Nadpisujemy QueryEvents, żeby korzystało z naszej queryEventsSql
func (b MySQLiteBackend) QueryEvents(ctx context.Context, filter nostr.Filter) (chan *nostr.Event, error) {
	query, params, err := b.queryEventsSql(filter, false)
	if err != nil {
		return nil, err
	}

	rows, err := b.DB.QueryContext(ctx, query, params...)
	if err != nil && err != sqlite3.ErrNoRows {
		return nil, err
	}

	ch := make(chan *nostr.Event)
	go func() {
		defer rows.Close()
		defer close(ch)
		for rows.Next() {
			var evt nostr.Event
			var timestamp int64
			err := rows.Scan(&evt.ID, &evt.PubKey, &timestamp, &evt.Kind, &evt.Tags, &evt.Content, &evt.Sig)
			if err != nil {
				return
			}
			evt.CreatedAt = nostr.Timestamp(timestamp)
			ch <- &evt
		}
	}()

	return ch, nil
}
