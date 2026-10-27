package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/nikrybak/lsmkv"
)

// maxValueBytes bounds request bodies so a client cannot exhaust memory.
const maxValueBytes = 8 << 20

// newServer exposes db over a minimal HTTP API:
//
//	GET    /kv/{key}                    value bytes, 404 if absent
//	PUT    /kv/{key}                    body is the value
//	DELETE /kv/{key}
//	GET    /scan?prefix=&start=&end=&limit=   JSON array of {key, value}
//	GET    /stats                       engine statistics
func newServer(db *lsmkv.DB) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		v, err := db.Get([]byte(r.PathValue("key")))
		if err != nil {
			writeError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(v)
	})
	mux.HandleFunc("PUT /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxValueBytes))
		if err != nil {
			http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
			return
		}
		if err := db.Put([]byte(r.PathValue("key")), body); err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("DELETE /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		if err := db.Delete([]byte(r.PathValue("key"))); err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /scan", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		limit := 1000
		if s := q.Get("limit"); s != "" {
			n, err := strconv.Atoi(s)
			if err != nil || n <= 0 {
				http.Error(w, "limit must be a positive integer", http.StatusBadRequest)
				return
			}
			limit = n
		}
		it, err := db.Scan(keyRange(q.Get("prefix"), q.Get("start"), q.Get("end")))
		if err != nil {
			writeError(w, err)
			return
		}
		defer it.Close()
		type pair struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		out := []pair{}
		for len(out) < limit && it.Next() {
			out = append(out, pair{string(it.Key()), string(it.Value())})
		}
		if err := it.Err(); err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, out)
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		st, err := db.Stats()
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, st)
	})
	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, lsmkv.ErrNotFound):
		http.Error(w, "not found", http.StatusNotFound)
	case errors.Is(err, lsmkv.ErrClosed):
		http.Error(w, "database closed", http.StatusServiceUnavailable)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
