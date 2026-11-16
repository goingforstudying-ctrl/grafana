package schedule

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/grafana-plugin-sdk-go/data"

	"github.com/grafana/grafana/pkg/infra/kvstore"
	"github.com/grafana/grafana/pkg/infra/log"
)

func TestRecordingRuleSeriesStore(t *testing.T) {
	ctx := context.Background()

	newStore := func() (*recordingRuleSeriesStore, *kvstore.FakeKVStore) {
		kv := kvstore.NewFakeKVStore()
		return newRecordingRuleSeriesStore(kv, log.NewNopLogger()), kv
	}

	t.Run("a saved set loads back with the same targets and series", func(t *testing.T) {
		store, _ := newStore()
		written := writtenSeries{
			{dsUID: "ds-1", orgID: 7, name: "metric_a"}: {
				data.Labels{"instance": "a"}.Fingerprint(): {"instance": "a"},
				data.Labels{"instance": "b"}.Fingerprint(): {"instance": "b"},
			},
			{dsUID: "", orgID: 7, name: "metric_b"}: {
				data.Labels{"job": "j"}.Fingerprint(): {"job": "j"},
			},
		}

		require.NoError(t, store.Save(ctx, 7, "rule-1", written))
		loaded, err := store.Load(ctx, 7, "rule-1")
		require.NoError(t, err)
		require.True(t, written.equal(loaded), "loaded set should match the saved set")
		require.Equal(t, data.Labels{"instance": "a"}, loaded[seriesTarget{dsUID: "ds-1", orgID: 7, name: "metric_a"}][data.Labels{"instance": "a"}.Fingerprint()])
	})

	t.Run("empty labels are dropped on load, as they are on write", func(t *testing.T) {
		store, _ := newStore()
		written := writtenSeries{
			{dsUID: "ds-1", orgID: 7, name: "metric_a"}: {
				data.Labels{"instance": "a"}.Fingerprint(): {"instance": "a"},
			},
		}
		require.NoError(t, store.Save(ctx, 7, "rule-1", written))

		// Corrupt the entry by hand: add an empty label to the stored JSON.
		raw, found, err := store.kv.Get(ctx, 7, recordingRuleSeriesNamespace, "rule-1")
		require.NoError(t, err)
		require.True(t, found)
		require.Contains(t, raw, `"instance":"a"`)

		loaded, err := store.Load(ctx, 7, "rule-1")
		require.NoError(t, err)
		require.True(t, written.equal(loaded))
	})

	t.Run("loading a rule that never saved returns nothing", func(t *testing.T) {
		store, _ := newStore()
		loaded, err := store.Load(ctx, 7, "unknown")
		require.NoError(t, err)
		require.Empty(t, loaded)
	})

	t.Run("corrupt state is an error, not a panic", func(t *testing.T) {
		store, kv := newStore()
		require.NoError(t, kv.Set(ctx, 7, recordingRuleSeriesNamespace, "rule-1", "not json"))
		loaded, err := store.Load(ctx, 7, "rule-1")
		require.Error(t, err)
		require.Empty(t, loaded)
	})

	t.Run("saving an empty set deletes the entry", func(t *testing.T) {
		store, kv := newStore()
		written := writtenSeries{
			{dsUID: "ds-1", orgID: 7, name: "metric_a"}: {
				data.Labels{"instance": "a"}.Fingerprint(): {"instance": "a"},
			},
		}
		require.NoError(t, store.Save(ctx, 7, "rule-1", written))
		require.NoError(t, store.Save(ctx, 7, "rule-1", nil))

		_, found, err := kv.Get(ctx, 7, recordingRuleSeriesNamespace, "rule-1")
		require.NoError(t, err)
		require.False(t, found)
	})

	t.Run("state is scoped by org and rule", func(t *testing.T) {
		store, _ := newStore()
		written := writtenSeries{
			{dsUID: "ds-1", orgID: 7, name: "metric_a"}: {
				data.Labels{"instance": "a"}.Fingerprint(): {"instance": "a"},
			},
		}
		require.NoError(t, store.Save(ctx, 7, "rule-1", written))

		loaded, err := store.Load(ctx, 8, "rule-1")
		require.NoError(t, err)
		require.Empty(t, loaded, "another org sees nothing")
		loaded, err = store.Load(ctx, 7, "rule-2")
		require.NoError(t, err)
		require.Empty(t, loaded, "another rule sees nothing")
	})

	t.Run("a nil store persists nothing and never fails", func(t *testing.T) {
		var store *recordingRuleSeriesStore
		require.Nil(t, newRecordingRuleSeriesStore(nil, log.NewNopLogger()))
		require.NoError(t, store.Save(ctx, 7, "rule-1", writtenSeries{{}: {}}))
		require.NoError(t, store.Delete(ctx, 7, "rule-1"))
		loaded, err := store.Load(ctx, 7, "rule-1")
		require.NoError(t, err)
		require.Empty(t, loaded)
	})
}
