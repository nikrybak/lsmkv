// Command lsmkv is a small command-line front end for an lsmkv database.
//
//	lsmkv -dir ./data put user:1 alice
//	lsmkv -dir ./data get user:1
//	lsmkv -dir ./data scan -prefix user:
//	lsmkv -dir ./data delete user:1
//	lsmkv -dir ./data stats
//	lsmkv -dir ./data serve -addr :8080
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/nikrybak/lsmkv"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "lsmkv:", err)
		os.Exit(1)
	}
}

const usage = `usage: lsmkv [-dir path] [-v] <command> [args]

commands:
  put <key> <value>   store a value
  get <key>           print a value
  delete <key>        remove a key
  scan [-prefix p | -start a -end b] [-limit n]
                      print key/value pairs in order
  stats               print engine statistics as JSON
  serve [-addr :8080] expose the database over HTTP`

func run(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("lsmkv", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("dir", "data", "database directory")
	verbose := fs.Bool("v", false, "log background events to stderr")
	fs.Usage = func() { fmt.Fprintln(stderr, usage) }
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() == 0 {
		fs.Usage()
		return errors.New("missing command")
	}
	opts := []lsmkv.Option{}
	if *verbose {
		opts = append(opts, lsmkv.WithLogger(slog.New(slog.NewTextHandler(stderr, nil))))
	}
	db, err := lsmkv.Open(*dir, opts...)
	if err != nil {
		return err
	}
	cmdErr := dispatch(db, fs.Arg(0), fs.Args()[1:], stdout, stderr)
	if err := db.Close(); err != nil && cmdErr == nil {
		cmdErr = err
	}
	return cmdErr
}

func dispatch(db *lsmkv.DB, cmd string, args []string, stdout, stderr io.Writer) error {
	need := func(n int) error {
		if len(args) != n {
			return fmt.Errorf("%s expects %d argument(s), got %d", cmd, n, len(args))
		}
		return nil
	}
	switch cmd {
	case "put":
		if err := need(2); err != nil {
			return err
		}
		return db.Put([]byte(args[0]), []byte(args[1]))
	case "get":
		if err := need(1); err != nil {
			return err
		}
		v, err := db.Get([]byte(args[0]))
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(stdout, "%s\n", v)
		return err
	case "delete":
		if err := need(1); err != nil {
			return err
		}
		return db.Delete([]byte(args[0]))
	case "scan":
		return scanCmd(db, args, stdout, stderr)
	case "stats":
		st, err := db.Stats()
		if err != nil {
			return err
		}
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(st)
	case "serve":
		return serveCmd(db, args, stderr)
	}
	return fmt.Errorf("unknown command %q", cmd)
}

func scanCmd(db *lsmkv.DB, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("scan", flag.ContinueOnError)
	fs.SetOutput(stderr)
	prefix := fs.String("prefix", "", "only keys with this prefix")
	start := fs.String("start", "", "first key (inclusive)")
	end := fs.String("end", "", "last key (exclusive)")
	limit := fs.Int("limit", 0, "stop after n entries (0 = no limit)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	it, err := db.Scan(keyRange(*prefix, *start, *end))
	if err != nil {
		return err
	}
	defer it.Close()
	for n := 0; (*limit == 0 || n < *limit) && it.Next(); n++ {
		if _, err := fmt.Fprintf(stdout, "%s\t%s\n", it.Key(), it.Value()); err != nil {
			return err
		}
	}
	return it.Err()
}

func keyRange(prefix, start, end string) lsmkv.KeyRange {
	if prefix != "" {
		return lsmkv.Prefix([]byte(prefix))
	}
	var r lsmkv.KeyRange
	if start != "" {
		r.Start = []byte(start)
	}
	if end != "" {
		r.End = []byte(end)
	}
	return r
}

func serveCmd(db *lsmkv.DB, args []string, stderr io.Writer) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", "127.0.0.1:8080", "listen address")
	if err := fs.Parse(args); err != nil {
		return err
	}
	srv := &http.Server{
		Addr:              *addr,
		Handler:           newServer(db),
		ReadHeaderTimeout: 5 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	fmt.Fprintf(stderr, "listening on %s\n", *addr)
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}
