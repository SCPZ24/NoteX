package feed

import (
	"reflect"
	"testing"
)

func TestPartitionRecipientsFreezesReaderRouting(t *testing.T) {
	var recipients []recipient
	for id := int64(1); id <= 501; id++ {
		recipients = append(recipients, recipient{UserID: id, RelationID: id + 1000})
	}
	for _, count := range []int{0, 1, 4, 32} {
		chunks := partitionRecipients(100, recipients, count)
		seen := map[int64]bool{}
		for _, chunk := range chunks {
			if chunk.PostID != 100 || chunk.ShardCount != count || len(chunk.Recipients) > chunkSize || len(chunk.Recipients) == 0 {
				t.Fatalf("invalid chunk: %+v", chunk)
			}
			for _, target := range chunk.Recipients {
				if seen[target.UserID] {
					t.Fatal("duplicate recipient")
				}
				seen[target.UserID] = true
				if count > 0 && int(target.UserID%int64(count)) != chunk.Shard {
					t.Fatal("mixed shard chunk")
				}
			}
		}
		if len(seen) != 501 || len(chunks)+1 > planPageSize+2 {
			t.Fatal("lost recipients or oversized handoff")
		}
		if !reflect.DeepEqual(chunks, partitionRecipients(100, recipients, count)) {
			t.Fatal("unstable child order")
		}
	}
}
