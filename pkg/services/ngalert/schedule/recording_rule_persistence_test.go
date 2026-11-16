package schedule

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/grafana/pkg/infra/kvstore"
)

// countingKVStore records how often the store is written, so tests can assert that
// persistence happens once per change and not on every tick.
type countingKVStore struct {
	kvstore.KVStore
	sets int
	dels int
}

func (c *countingKVStore) Set(ctx context.Context, orgID int64, namespace string, key string, value string) error {
	c.sets++
	return c.KVStore.Set(ctx, orgID, namespace, key, value)
}

func (c *countingKVStore) Del(ctx context.Context, orgID int64, namespace string, key string) error {
	c.dels++
	return c.KVStore.Del(ctx, orgID, namespace, key)
}

func (h *staleHarness) persisted(t *testing.T) (string, bool) {
	t.Helper()
	raw, found, err := h.kv.Get(context.Background(), 123, recordingRuleSeriesNamespace, "stale-test-rule")
	require.NoError(t, err)
	return raw, found
}

func TestRecordingRuleSeriesPersistence(t *testing.T) {
	a := map[string]string{"instance": "a"}
	b := map[string]string{"instance": "b"}
	withRuleLabels := func(m map[string]string) map[string]string {
		return map[string]string{"instance": m["instance"], "rule": "label"}
	}

	t.Run("a restarted rule stales the series that disappeared while it was down", func(t *testing.T) {
		h := newStaleHarness(t)
		h.ev.resp = seriesResponse(a, b)
		h.tick()

		// A new rule over the same store, with no memory of the first one.
		h2 := newStaleHarnessWithKV(t, h.kv)
		h2.ev.resp = seriesResponse(a)
		ts := h2.tick()

		require.Len(t, h2.stale, 1)
		require.Equal(t, markerTime(ts), h2.stale[0].t)
		require.Equal(t, []map[string]string{withRuleLabels(b)}, h2.staleSeries(0))
	})

	t.Run("a graceful shutdown writes no markers but keeps the state for the next run", func(t *testing.T) {
		h := newStaleHarness(t)
		h.ev.resp = seriesResponse(a)
		h.tick()

		h.cancelParent()
		require.NoError(t, h.rule.Run())
		require.Empty(t, h.stale, "shutdown writes no markers, another replica may still write the series")

		_, found := h.persisted(t)
		require.True(t, found, "the state survives the shutdown")

		h2 := newStaleHarnessWithKV(t, h.kv)
		h2.ev.resp = seriesResponse(b)
		ts := h2.tick()
		require.Len(t, h2.stale, 1)
		require.Equal(t, markerTime(ts), h2.stale[0].t)
		require.Equal(t, []map[string]string{withRuleLabels(a)}, h2.staleSeries(0))
	})

	t.Run("a rule that moves mid-run stales nothing, and the new owner takes over the state", func(t *testing.T) {
		h := newStaleHarness(t)
		h.ev.resp = seriesResponse(a, b)
		h.tick()

		// The old owner stops without owning the group anymore: no markers, state kept.
		h.stop(errors.New("replica does not own the rule group anymore"))
		require.Empty(t, h.stale)
		_, found := h.persisted(t)
		require.True(t, found, "the moved rule leaves its state for the new owner")

		// The new owner starts with the moved state and stales what leaves the result.
		h2 := newStaleHarnessWithKV(t, h.kv)
		h2.ev.resp = seriesResponse(a)
		h2.tick()
		require.Len(t, h2.stale, 1)
		require.Equal(t, []map[string]string{withRuleLabels(b)}, h2.staleSeries(0))
	})

	t.Run("deleting a rule writes markers and removes the persisted state", func(t *testing.T) {
		h := newStaleHarness(t)
		h.ev.resp = seriesResponse(a)
		h.tick()
		_, found := h.persisted(t)
		require.True(t, found)

		h.stop(errRuleDeleted)
		require.Len(t, h.stale, 1)
		_, found = h.persisted(t)
		require.False(t, found, "the deleted rule leaves no state behind")
	})

	t.Run("a rule deleted before its first tick still stales the restored series", func(t *testing.T) {
		h := newStaleHarness(t)
		h.ev.resp = seriesResponse(a, b)
		h.tick()

		// The rule restarts and is deleted before it ever evaluates.
		h2 := newStaleHarnessWithKV(t, h.kv)
		h2.rule.clock.(interface{ Add(time.Duration) }).Add(time.Hour)
		h2.stop(errRuleDeleted)

		require.Len(t, h2.stale, 1)
		require.ElementsMatch(t, []map[string]string{withRuleLabels(a), withRuleLabels(b)}, h2.staleSeries(0))
		require.Equal(t, h2.rule.clock.Now(), h2.stale[0].t, "with no tick of its own, the marker is now")
		_, found := h2.persisted(t)
		require.False(t, found)
	})

	t.Run("the store is written once per change, not on every tick", func(t *testing.T) {
		kv := &countingKVStore{KVStore: kvstore.NewFakeKVStore()}
		h := newStaleHarnessWithKV(t, kv)
		h.ev.resp = seriesResponse(a, b)
		h.tick()
		require.Equal(t, 1, kv.sets)

		h.tick()
		h.tick()
		require.Equal(t, 1, kv.sets, "an unchanged result is not persisted again")

		h.ev.resp = seriesResponse(a)
		h.tick()
		require.Equal(t, 2, kv.sets, "b was staled, so the remembered set changed")

		h.ev.resp = seriesResponse(a, b)
		h.tick()
		require.Equal(t, 3, kv.sets)

		h.model.IsPaused = true
		h.tick()
		require.Equal(t, 1, kv.dels, "everything is staled on pause, so the entry is deleted")
		h.tick()
		require.Equal(t, 1, kv.dels, "a second paused tick changes nothing")
	})

	t.Run("a corrupt persisted state is replaced by the next write", func(t *testing.T) {
		h := newStaleHarness(t)
		require.NoError(t, h.kv.Set(context.Background(), 123, recordingRuleSeriesNamespace, "stale-test-rule", "not json"))

		// The rule evaluates anyway and overwrites the corrupt entry.
		h.ev.resp = seriesResponse(a)
		h.tick()
		require.Equal(t, "ok", h.rule.Status().Health)

		h2 := newStaleHarnessWithKV(t, h.kv)
		h2.ev.resp = seriesResponse(b)
		h2.tick()
		require.Len(t, h2.stale, 1, "the fresh state was persisted over the corrupt one")
		require.Equal(t, []map[string]string{withRuleLabels(a)}, h2.staleSeries(0))
	})
}
