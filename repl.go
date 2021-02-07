package gosql

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"text/tabwriter"

	"github.com/peterh/liner"
)

func REPL(db *DB, prompt string) error {
	l, hfile := liner.NewLiner(), os.ExpandEnv("$HOME/.gosql_history")
	l.SetCtrlCAborts(true)
	defer l.Close()
	if f, err := os.Open(hfile); err == nil {
		l.ReadHistory(f)
		f.Close()
	}
	defer func() {
		if f, err := os.Create(hfile); err != nil {
			log.Printf("Error writing %s: %s", hfile, err)
		} else {
			l.WriteHistory(f)
			f.Close()
		}
	}()

	query := ""
	for {
		line, err := l.Prompt(prompt)
		if err == liner.ErrPromptAborted {
			continue
		} else if err != nil {
			return err
		} else if len(strings.TrimSpace(line)) == 0 {
			continue
		}
		if len(query) > 0 && !strings.HasSuffix(query, " ") {
			query += " "
		}
		query += line
		l.AppendHistory(query)
		if strings.HasPrefix(query, ".schema") {
			if m := regexp.MustCompile(`.schema\s+(\w+)`).FindStringSubmatch(query); m != nil {
				query = fmt.Sprintf("select name, type from pragma_table_info('%s');", m[1])
			}
		}
		if !strings.HasSuffix(strings.TrimSpace(query), ";") {
			continue
		} else if rows, err := db.Query(query); err != nil {
			fmt.Fprintln(os.Stdout, "ERROR: ", err)
		} else if err := Table(os.Stdout, rows); err != nil {
			fmt.Fprintln(os.Stdout, "ERROR: ", err)
		}
		query = ""
	}
}

func Table(out io.Writer, rows *sql.Rows) error {
	s := &strings.Builder{}
	tw := tabwriter.NewWriter(s, 1, 1, 1, ' ', 0)
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		cancel()
	}()
	defer func() {
		signal.Stop(c)
		cancel()
	}()
	for rows.Next() {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{})
		}
		if err := rows.Scan(values...); err != nil {
			return err
		}
		row := ""
		for i := range columns {
			v := fmt.Sprintf("%v", *values[i].(*interface{}))
			if len(columns) > 1 {
				v = strings.ReplaceAll(v, "\t", "\\t")
				v = strings.ReplaceAll(v, "\n", "\\n")
				if len(v) >= 100 {
					v = v[:99] + "…"
				}
			}
			row += v + "\t"
		}
		fmt.Fprintln(tw, row)
	}
	if err := rows.Close(); err != nil {
		return err
	} else if err := tw.Flush(); err != nil {
		return err
	}
	for _, line := range strings.Split(s.String(), "\n") {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		fmt.Fprintln(out, line)
	}
	return nil
}
