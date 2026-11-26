package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Fixture struct {
	Readers []int
	Posts   []string
	Source  string
	Seed    int64
}
type Result struct {
	Write  bool
	Status int
	Micros int64
}

func main() {
	var f Fixture
	raw, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	if err = json.Unmarshal(raw, &f); err != nil {
		panic(err)
	}
	rate, _ := strconv.Atoi(os.Args[2])
	seconds, _ := strconv.Atoi(os.Args[3])
	replay := os.Args[4] == "replay"
	rng := rand.New(rand.NewSource(f.Seed))
	client := http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 1024, MaxIdleConns: 1024}, Timeout: 2 * time.Second}
	tokens := map[int]string{}
	for _, u := range f.Readers {
		tokens[u] = fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s/%d", f.Source, u))))
	}
	slots := make(chan struct{}, 1024)
	group := sync.WaitGroup{}
	results := make([]Result, rate*seconds)
	// Only the write-count replay orders overlapping operations on the same relationship.
	previous := map[string]chan struct{}{}
	type relation struct {
		post int64
		user int
	}
	expected := map[relation]bool{}
	ops := make([]int, 100)
	for i := range ops {
		ops[i] = i
	}
	start := time.Now()
	for i := range results {
		if i%100 == 0 {
			rng.Shuffle(100, func(a, b int) { ops[a], ops[b] = ops[b], ops[a] })
		}
		op := ops[i%100]
		writer := op < 80
		user := f.Readers[rng.Intn(len(f.Readers))]
		post := f.Posts[1+rng.Intn(len(f.Posts)-1)]
		if op < 48 || (op >= 60 && op < 76) {
			post = f.Posts[0]
		}
		if !writer {
			post = f.Posts[rng.Intn(len(f.Posts))]
		}
		method := "GET"
		path := "/api/v1/posts/" + post
		if writer {
			path += "/like"
			method = "PUT"
			if op >= 60 {
				method = "DELETE"
			}
		}
		due := start.Add(time.Duration(float64(i) * float64(time.Second) / float64(rate)))
		time.Sleep(time.Until(due))
		results[i].Write = writer
		select {
		case slots <- struct{}{}:
		default:
			continue
		}
		req, err := http.NewRequest(method, "http://127.0.0.1:18400"+path, nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+tokens[user])
		var wait, done chan struct{}
		if replay && writer {
			postID, err := strconv.ParseInt(post, 10, 64)
			if err != nil {
				panic(err)
			}
			pair := relation{postID, user}
			if method == "PUT" {
				expected[pair] = true
			} else {
				delete(expected, pair)
			}
			key := fmt.Sprintf("%s/%d", post, user)
			wait = previous[key]
			done = make(chan struct{})
			previous[key] = done
		}
		group.Add(1)
		go func() {
			defer group.Done()
			defer func() { <-slots }()
			if done != nil {
				defer close(done)
			}
			if wait != nil {
				<-wait
			}
			r, e := client.Do(req)
			status := -1
			if e == nil {
				_, e = io.Copy(io.Discard, r.Body)
				r.Body.Close()
				if e == nil {
					status = r.StatusCode
				}
			}
			results[i] = Result{writer, status, time.Since(due).Microseconds()}
		}()
	}
	group.Wait()
	hist := map[int]int{}
	var lat []int64
	total, miss, success, fail := 0, 0, 0, 0
	for _, r := range results {
		hist[r.Status]++
		if !r.Write {
			continue
		}
		total++
		switch r.Status {
		case 0:
			miss++
		case 200, 202:
			success++
			lat = append(lat, r.Micros)
		default:
			fail++
		}
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	p95 := 0.
	if len(lat) > 0 {
		p95 = float64(lat[(len(lat)*95+99)/100-1]) / 1000
	}
	summary := map[string]any{"rate": rate, "seconds": seconds, "wall": time.Since(start).Seconds(), "write_planned": total, "write_success": success, "write_failed": fail, "write_unsent": miss, "write_p95_ms": p95, "all_status": hist}
	if replay {
		keys := make([]relation, 0, len(expected))
		for pair := range expected {
			keys = append(keys, pair)
		}
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].post == keys[j].post {
				return keys[i].user < keys[j].user
			}
			return keys[i].post < keys[j].post
		})
		lines := make([]string, len(keys))
		for i, k := range keys {
			lines[i] = fmt.Sprintf("%d\t%d", k.post, k.user)
		}
		summary["expected_state_hash"] = fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(lines, "\n"))))
	}
	if err := json.NewEncoder(os.Stdout).Encode(summary); err != nil {
		panic(err)
	}
}
