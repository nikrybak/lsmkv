package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nikrybak/lsmkv"
)

func TestCLI(t *testing.T) {
	dir := t.TempDir()
	tests := []struct {
		args    []string
		want    string
		wantErr bool
	}{
		{[]string{"put", "user:1", "alice"}, "", false},
		{[]string{"put", "user:2", "bob"}, "", false},
		{[]string{"put", "order:1", "book"}, "", false},
		{[]string{"get", "user:1"}, "alice\n", false},
		{[]string{"scan", "-prefix", "user:"}, "user:1\talice\nuser:2\tbob\n", false},
		{[]string{"scan", "-limit", "1"}, "order:1\tbook\n", false},
		{[]string{"delete", "user:1"}, "", false},
		{[]string{"get", "user:1"}, "", true},
		{[]string{"put", "only-key"}, "", true},
		{[]string{"bogus"}, "", true},
	}
	for _, tt := range tests {
		t.Run(strings.Join(tt.args, " "), func(t *testing.T) {
			var out, errOut bytes.Buffer
			err := run(append([]string{"-dir", dir}, tt.args...), &out, &errOut)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v (stderr %q)", err, tt.wantErr, errOut.String())
			}
			if out.String() != tt.want {
				t.Fatalf("stdout = %q, want %q", out.String(), tt.want)
			}
		})
	}
}

func TestHTTPServer(t *testing.T) {
	db, err := lsmkv.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	srv := httptest.NewServer(newServer(db))
	defer srv.Close()

	do := func(method, path, body string) (int, string) {
		t.Helper()
		req, err := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}

	steps := []struct {
		method, path, body string
		status             int
		want               string
	}{
		{"GET", "/kv/missing", "", 404, ""},
		{"PUT", "/kv/a", "1", 204, ""},
		{"PUT", "/kv/b", "2", 204, ""},
		{"GET", "/kv/a", "", 200, "1"},
		{"GET", "/scan?prefix=a", "", 200, `[{"key":"a","value":"1"}]`},
		{"GET", "/scan?limit=x", "", 400, ""},
		{"DELETE", "/kv/a", "", 204, ""},
		{"GET", "/kv/a", "", 404, ""},
		{"GET", "/scan", "", 200, `[{"key":"b","value":"2"}]`},
	}
	for _, s := range steps {
		status, body := do(s.method, s.path, s.body)
		if status != s.status {
			t.Fatalf("%s %s: status %d, want %d (%s)", s.method, s.path, status, s.status, body)
		}
		if s.want != "" && strings.TrimSpace(body) != s.want {
			t.Fatalf("%s %s: body %q, want %q", s.method, s.path, body, s.want)
		}
	}

	status, body := do("GET", "/stats", "")
	var st lsmkv.Stats
	if status != 200 || json.Unmarshal([]byte(body), &st) != nil || st.LastSequence != 3 {
		t.Fatalf("stats: %d %s", status, body)
	}
}
