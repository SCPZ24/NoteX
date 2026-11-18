package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptrace"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Bucket struct {
	Sent, NotSent, ActivePeak int
	Results                   map[string]map[string]int
	Lat                       map[string][]int64 `json:"-"`
}

var buckets [150]Bucket
var mu sync.Mutex
var start, end time.Time
var active, launched int
var first = map[string]any{}
var stop string
var wg sync.WaitGroup

func elapsed() float64 { return time.Since(start).Seconds() }
func sendRequest(ctx context.Context, op, url, method, token, key string, id int) {
	defer wg.Done()
	var body io.Reader
	if op == "post" {
		body = strings.NewReader(`{"content":"mixed load"}`)
	}
	req, _ := http.NewRequestWithContext(ctx, method, url, body)
	req.Header.Set("Authorization", "Bearer "+token)
	if op == "post" {
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", key)
	}
	var sent time.Time
	idx := -1
	tr := httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
		mu.Lock()
		defer mu.Unlock()
		if idx != -1 || info.Err != nil {
			return
		}
		t := time.Now()
		if !end.IsZero() || t.Sub(start) >= 150*time.Second {
			return
		}
		sent = t
		idx = int(t.Sub(start).Seconds())
		b := &buckets[idx]
		b.Sent++
		active++
		if active > b.ActivePeak {
			b.ActivePeak = active
		}
	}}
	r, e := client.Do(req.WithContext(httptrace.WithClientTrace(ctx, &tr)))
	status := 0
	if e == nil {
		status = r.StatusCode
		_, e = io.Copy(io.Discard, r.Body)
		r.Body.Close()
	}
	mu.Lock()
	defer mu.Unlock()
	launched--
	if idx < 0 {
		buckets[id].NotSent++
		return
	}
	active--
	b := &buckets[idx]
	result := "ok"
	d := time.Since(sent)
	switch {
	case !end.IsZero():
		result = "pending"
	case errors.Is(e, context.DeadlineExceeded) || d > 2*time.Second:
		result = "timeout"
	case e != nil:
		result = "connection"
	case !(status == 200 && op != "post" || status == 201 && op == "post" || status == 202 && (op == "like" || op == "unlike")):
		result = strconv.Itoa(status)
	}
	if result == "ok" {
		b.Lat[op] = append(b.Lat[op], d.Microseconds())
	} else if result != "pending" && len(first) == 0 {
		first = map[string]any{"op": op, "result": result, "elapsed": elapsed()}
	}
	if b.Results[op] == nil {
		b.Results[op] = map[string]int{}
	}
	b.Results[op][result]++
}

var client = &http.Client{Transport: &http.Transport{MaxIdleConns: 1024, MaxIdleConnsPerHost: 1024, DisableCompression: true}, Timeout: 2 * time.Second}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	raw, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	var fixture struct {
		Readers []int
		Posts   []string
		Source  string
		Seed    int64
	}
	if err = json.Unmarshal(raw, &fixture); err != nil {
		panic(err)
	}
	posts := fixture.Posts
	tokens := map[int]string{}
	for _, id := range append([]int{1, 2, 3}, fixture.Readers...) {
		tokens[id] = fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s/%d", fixture.Source, id))))
	}
	for i := range buckets {
		buckets[i].Lat = make(map[string][]int64)
		buckets[i].Results = map[string]map[string]int{}
	}
	rng := rand.New(rand.NewSource(fixture.Seed))
	ops := []string{}
	for i, n := range []int{40, 25, 15, 5, 8, 3, 2, 2} {
		for j := 0; j < n; j++ {
			ops = append(ops, []string{"feed", "detail", "like", "unlike", "post", "follow", "unfollow", "me"}[i])
		}
	}
	start = time.Now()
	nextcheck := 0
	streak := 0
	serial := 0
	laststage := 0
	var boundary = [5]int{}
outer:
	for stage, rate := range []int{512, 1024, 2048, 4096, 8192} {
		for k := 0; k < rate*30; k++ {
			target := start.Add(time.Duration(stage)*30*time.Second + time.Duration(k)*time.Second/time.Duration(rate))
			if d := time.Until(target); d > 0 {
				time.Sleep(d)
			}
			sec := int(elapsed())
			if sec >= 150 {
				break outer
			}
			if sec/30 != laststage {
				mu.Lock()
				boundary[laststage] = active
				mu.Unlock()
				laststage = sec / 30
			}
			if k%16 == 0 {
				if raw, e := os.ReadFile(os.Args[2]); e == nil {
					stop = string(raw)
					break outer
				}
			}
			for sec >= nextcheck+3 {
				mu.Lock()
				b := &buckets[nextcheck]
				ok := 0
				for _, r := range b.Results {
					ok += r["ok"]
				}
				if b.Sent > 0 && 2*ok < b.Sent {
					streak++
				} else {
					streak = 0
				}
				mu.Unlock()
				nextcheck++
				if streak == 3 {
					stop = fmt.Sprintf("three low-success buckets from %d", nextcheck-3)
					break outer
				}
			}
			if serial%100 == 0 {
				rng.Shuffle(len(ops), func(i, j int) { ops[i], ops[j] = ops[j], ops[i] })
			}
			op := ops[serial%100]
			serial++
			user := fixture.Readers[rng.Intn(len(fixture.Readers))]
			p := posts[rng.Intn(len(posts))]
			author := 1 + rng.Intn(3)
			port := 18400
			path := "/posts/" + p
			method := "GET"
			switch op {
			case "feed":
				port = 18402
				path = "/feed?limit=20"
			case "like", "unlike":
				path += "/like"
				method = "PUT"
				if op == "unlike" {
					method = "DELETE"
				}
			case "post":
				user = author
				path = "/posts"
				method = "POST"
			case "follow", "unfollow":
				path = "/users/" + strconv.Itoa(author) + "/follow"
				method = "PUT"
				if op == "unfollow" {
					method = "DELETE"
				}
			case "me":
				path = "/me"
			}
			mu.Lock()
			if launched >= 1024 || sec/30 != stage {
				buckets[sec].NotSent++
				mu.Unlock()
				continue
			}
			launched++
			mu.Unlock()
			wg.Add(1)
			go sendRequest(ctx, op, fmt.Sprintf("http://127.0.0.1:%d/api/v1%s", port, path), method, tokens[user], fmt.Sprintf("m-%d", serial), sec)
		}
	}
	mu.Lock()
	end = time.Now()
	boundary[laststage] = active
	mu.Unlock()
	cancel()
	wg.Wait()
	stages := []map[string]any{}
	for s := 0; s <= laststage; s++ {
		byOp := make(map[string][]int64)
		var all []int64
		for _, b := range buckets[s*30 : (s+1)*30] {
			for op, values := range b.Lat {
				all = append(all, values...)
				byOp[op] = append(byOp[op], values...)
			}
		}
		percentiles := func(values []int64) map[string]float64 {
			result := make(map[string]float64)
			if len(values) > 0 {
				sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
				for _, q := range []int{95, 99} {
					result[fmt.Sprintf("p%d_ms", q)] = float64(values[(len(values)*q+99)/100-1]) / 1000
				}
			}
			return result
		}
		perOp := make(map[string]map[string]float64)
		for op, values := range byOp {
			perOp[op] = percentiles(values)
		}
		total := percentiles(all)
		stages = append(stages, map[string]any{"stage": s, "p95_ms": total["p95_ms"], "p99_ms": total["p99_ms"], "by_op": perOp, "boundary_inflight": boundary[s]})
	}
	json.NewEncoder(os.Stdout).Encode(map[string]any{"start": start, "end": end, "seconds": end.Sub(start).Seconds(), "stop": stop, "first_error": first, "buckets": buckets, "stages": stages})
}
