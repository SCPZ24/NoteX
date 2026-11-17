package outbox

import (
	"encoding/json"
	"testing"
)

func TestShardedEnvelopeAndLegacyReplay(t *testing.T) {
	event := Event{ID: 7, Type: "feed.chunk", CreatedOn: 123, Payload: json.RawMessage(`{"task_id":7}`)}
	for _, count := range []int{0, 4} {
		event.ShardCount = count
		names, err := event.destinations("test")
		if err != nil {
			t.Fatal(err)
		}
		want := "test.feed.chunk"
		if count > 0 {
			want = "test.feed.chunk.s000"
		}
		if names[0] != want {
			t.Fatalf("got %s want %s", names[0], want)
		}
		body, _ := json.Marshal(event)
		if _, err := decodeDelivery(want, "7", body); err != nil {
			t.Fatal(err)
		}
		if count > 0 {
			if _, err := decodeDelivery("test.feed.chunk.s001", "7", body); err == nil {
				t.Fatal("accepted wrong shard")
			}
		}
	}
	event.Shard = 4
	if _, err := event.destinations("test"); err == nil {
		t.Fatal("accepted shard outside topology")
	}
}
