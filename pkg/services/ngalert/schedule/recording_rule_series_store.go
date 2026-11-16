package schedule

import (
	"context"
	"encoding/json"
	"time"

	"github.com/grafana/grafana/pkg/infra/kvstore"
	"github.com/grafana/grafana/pkg/infra/log"
)

// recordingRuleSeriesNamespace is the kvstore namespace for the series a recording rule
// last wrote. The store is shared, so a rule that starts again, after a restart or on
// another instance, still knows which series it may need to stale.
const recordingRuleSeriesNamespace = "ngalert.recording-rule-series"

// seriesStoreTimeout bounds store calls so a slow database never stalls an evaluation for long.
const seriesStoreTimeout = 10 * time.Second

// persistedWrittenSeries is the JSON shape kept in the store.
type persistedWrittenSeries struct {
	Targets []persistedSeriesTarget `json:"targets"`
}

type persistedSeriesTarget struct {
	DatasourceUID string              `json:"ds_uid,omitempty"`
	Metric        string              `json:"metric"`
	Series        []map[string]string `json:"series"`
}

// recordingRuleSeriesStore persists the series a recording rule last wrote.
// A nil store is valid and persists nothing.
type recordingRuleSeriesStore struct {
	kv     kvstore.KVStore
	logger log.Logger
}

func newRecordingRuleSeriesStore(kv kvstore.KVStore, logger log.Logger) *recordingRuleSeriesStore {
	if kv == nil {
		return nil
	}
	return &recordingRuleSeriesStore{
		kv:     kv,
		logger: logger.New("ngalert.scheduler.recording-rule-series"),
	}
}

// Load returns the persisted series of a rule, or nil when the rule has none.
func (s *recordingRuleSeriesStore) Load(ctx context.Context, orgID int64, ruleUID string) (writtenSeries, error) {
	if s == nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(ctx, seriesStoreTimeout)
	defer cancel()
	raw, found, err := s.kv.Get(ctx, orgID, recordingRuleSeriesNamespace, ruleUID)
	if err != nil || !found {
		return nil, err
	}

	var stored persistedWrittenSeries
	if err := json.Unmarshal([]byte(raw), &stored); err != nil {
		return nil, err
	}

	var out writtenSeries
	for _, target := range stored.Targets {
		if len(target.Series) == 0 {
			continue
		}
		set := make(seriesSet, len(target.Series))
		for _, labels := range target.Series {
			l := withoutEmptyValues(labels)
			set[l.Fingerprint()] = l
		}
		if out == nil {
			out = make(writtenSeries)
		}
		out[seriesTarget{dsUID: target.DatasourceUID, orgID: orgID, name: target.Metric}] = set
	}
	return out, nil
}

// Save persists the series of a rule. An empty set deletes the entry instead.
func (s *recordingRuleSeriesStore) Save(ctx context.Context, orgID int64, ruleUID string, written writtenSeries) error {
	if s == nil {
		return nil
	}
	if len(written) == 0 {
		return s.Delete(ctx, orgID, ruleUID)
	}

	stored := persistedWrittenSeries{Targets: make([]persistedSeriesTarget, 0, len(written))}
	for target, set := range written {
		series := make([]map[string]string, 0, len(set))
		for _, labels := range set {
			series = append(series, map[string]string(labels))
		}
		stored.Targets = append(stored.Targets, persistedSeriesTarget{
			DatasourceUID: target.dsUID,
			Metric:        target.name,
			Series:        series,
		})
	}
	raw, err := json.Marshal(stored)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, seriesStoreTimeout)
	defer cancel()
	return s.kv.Set(ctx, orgID, recordingRuleSeriesNamespace, ruleUID, string(raw))
}

// Delete drops the persisted series of a rule.
func (s *recordingRuleSeriesStore) Delete(ctx context.Context, orgID int64, ruleUID string) error {
	if s == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, seriesStoreTimeout)
	defer cancel()
	return s.kv.Del(ctx, orgID, recordingRuleSeriesNamespace, ruleUID)
}
