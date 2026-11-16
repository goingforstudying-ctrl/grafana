package legacy

import (
	"context"
	"errors"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/grafana/pkg/apimachinery/identity"
	"github.com/grafana/grafana/pkg/apimachinery/utils"
	"github.com/grafana/grafana/pkg/infra/log"
	"github.com/grafana/grafana/pkg/services/folder"
	"github.com/grafana/grafana/pkg/services/folder/foldertest"
	"github.com/grafana/grafana/pkg/services/ngalert/models"
	rulestore "github.com/grafana/grafana/pkg/services/ngalert/store/rules"
	"github.com/grafana/grafana/pkg/services/ngalert/tests/fakes"
	"github.com/grafana/grafana/pkg/services/user"
)

// fakeListAuthz embeds the authorization interface so only the methods used by
// ListAlertRules need to be implemented.
type fakeListAuthz struct {
	ruleAccessControlService
	CanReadAllRulesFunc   func(ctx context.Context, user identity.Requester) (bool, error)
	HasAccessInFolderFunc func(ctx context.Context, user identity.Requester, folder models.Namespaced) (bool, error)
}

func (f *fakeListAuthz) CanReadAllRules(ctx context.Context, user identity.Requester) (bool, error) {
	return f.CanReadAllRulesFunc(ctx, user)
}

func (f *fakeListAuthz) HasAccessInFolder(ctx context.Context, user identity.Requester, folder models.Namespaced) (bool, error) {
	return f.HasAccessInFolderFunc(ctx, user, folder)
}

type fakeUpdateAuthz struct {
	ruleAccessControlService
}

func (fakeUpdateAuthz) CanWriteAllRules(context.Context, identity.Requester) (bool, error) {
	return true, nil
}

type transactionManagerFunc func(context.Context, func(context.Context) error) error

func (f transactionManagerFunc) InTransaction(ctx context.Context, work func(context.Context) error) error {
	return f(ctx, work)
}

func TestUpdateAlertRulePersistencePreconditions(t *testing.T) {
	const baselineVersion = int64(7)
	stored := models.RuleGen.GenerateRef()
	stored.Version = baselineVersion
	stored.NotificationSettings = nil
	updated := stored.Copy()
	updated.Title += " updated"
	requester := &user.SignedInUser{OrgID: stored.OrgID}

	t.Run("transaction retry preserves the original baseline", func(t *testing.T) {
		stored := stored.Copy()
		store := fakes.NewRuleStore(t)
		store.Rules[stored.OrgID] = []*models.AlertRule{stored}
		retry := errors.New("retry transaction")
		var persisted *models.AlertRule
		store.Hook = func(cmd any) error {
			updates, ok := cmd.([]models.UpdateRule)
			if !ok {
				return nil
			}
			if updates[0].Existing.Version != baselineVersion {
				return rulestore.ErrOptimisticLock
			}
			if persisted == nil {
				persisted = stored.Copy()
				updates[0].Existing.Version++
				updates[0].Existing.Title = "temporary title"
				return retry
			}
			persisted = updates[0].New.Copy()
			return nil
		}
		xact := transactionManagerFunc(func(ctx context.Context, work func(context.Context) error) error {
			if err := work(ctx); !errors.Is(err, retry) {
				return err
			}
			return work(ctx)
		})
		service := &LegacyRuleService{
			ruleStore: store, provenanceStore: fakes.NewFakeProvisioningStore(),
			xact: xact, authz: fakeUpdateAuthz{},
		}
		_, err := service.UpdateAlertRule(context.Background(), requester, *updated, utils.ManagerProperties{})
		require.NoError(t, err)
		require.Equal(t, updated.Title, persisted.Title)
		require.Equal(t, baselineVersion, stored.Version)
	})

	t.Run("optimistic lock failure after the read is preserved", func(t *testing.T) {
		stored := stored.Copy()
		store := fakes.NewRuleStore(t)
		store.Rules[stored.OrgID] = []*models.AlertRule{stored}
		store.Hook = func(cmd any) error {
			if _, ok := cmd.([]models.UpdateRule); ok {
				return rulestore.ErrOptimisticLock
			}
			return nil
		}
		provenance := fakes.NewFakeProvisioningStore()
		provenance.SetManagerPropertiesFunc = func(context.Context, models.Provisionable, int64, utils.ManagerProperties) error {
			t.Fatal("failed rule persistence must not change manager properties")
			return nil
		}
		service := &LegacyRuleService{ruleStore: store, provenanceStore: provenance, xact: store, authz: fakeUpdateAuthz{}}
		_, err := service.UpdateAlertRule(context.Background(), requester, *updated, utils.ManagerProperties{})
		require.ErrorIs(t, err, rulestore.ErrOptimisticLock)
	})
}

func TestListAlertRules(t *testing.T) {
	orgID := rand.Int63()
	u := &user.SignedInUser{OrgID: orgID}
	groupKey1 := models.GenerateGroupKey(orgID)
	groupKey2 := models.GenerateGroupKey(orgID)
	gen := models.RuleGen
	rules1 := gen.With(gen.WithGroupKey(groupKey1), gen.WithUniqueGroupIndex()).GenerateManyRef(3)
	models.RulesGroup(rules1).SortByGroupIndex()
	rules2 := gen.With(gen.WithGroupKey(groupKey2), gen.WithUniqueGroupIndex()).GenerateManyRef(4)
	models.RulesGroup(rules2).SortByGroupIndex()
	allRules := append(rules1, rules2...)

	fs := foldertest.NewFakeService()
	fs.AddFolder(&folder.Folder{OrgID: orgID, UID: groupKey1.NamespaceUID, Title: "folder1"})
	fs.AddFolder(&folder.Folder{OrgID: orgID, UID: groupKey2.NamespaceUID, Title: "folder2"})

	initServiceWithData := func(t *testing.T) (*LegacyRuleService, *fakes.RuleStore, *fakeListAuthz) {
		ruleStore := fakes.NewRuleStore(t)
		ruleStore.Rules = map[int64][]*models.AlertRule{orgID: allRules}
		ac := &fakeListAuthz{
			HasAccessInFolderFunc: func(ctx context.Context, user identity.Requester, folder models.Namespaced) (bool, error) {
				return true, nil
			},
		}
		service := &LegacyRuleService{
			folderService:   fs,
			ruleStore:       ruleStore,
			provenanceStore: fakes.NewFakeProvisioningStore(),
			log:             log.New("testing"),
			authz:           ac,
		}
		return service, ruleStore, ac
	}

	t.Run("when user can read all rules", func(t *testing.T) {
		t.Run("should return all rules", func(t *testing.T) {
			service, _, ac := initServiceWithData(t)
			ac.CanReadAllRulesFunc = func(ctx context.Context, user identity.Requester) (bool, error) {
				return true, nil
			}

			rules, _, token, err := service.ListAlertRules(context.Background(), u, ListAlertRulesOptions{})
			require.NoError(t, err)
			require.Len(t, rules, len(allRules))
			require.Empty(t, token)
		})
	})

	t.Run("when user cannot read all rules", func(t *testing.T) {
		t.Run("should return only rules in accessible folders", func(t *testing.T) {
			service, _, ac := initServiceWithData(t)
			ac.CanReadAllRulesFunc = func(ctx context.Context, user identity.Requester) (bool, error) {
				return false, nil
			}
			ac.HasAccessInFolderFunc = func(ctx context.Context, user identity.Requester, folder models.Namespaced) (bool, error) {
				return folder.GetNamespaceUID() == groupKey2.NamespaceUID, nil
			}

			rules, _, token, err := service.ListAlertRules(context.Background(), u, ListAlertRulesOptions{})
			require.NoError(t, err)
			require.Len(t, rules, len(rules2))
			for _, r := range rules {
				require.Equal(t, groupKey2.NamespaceUID, r.NamespaceUID)
			}
			require.Empty(t, token)
		})

		t.Run("should return no rules when no folders are accessible", func(t *testing.T) {
			service, ruleStore, ac := initServiceWithData(t)
			ac.CanReadAllRulesFunc = func(ctx context.Context, user identity.Requester) (bool, error) {
				return false, nil
			}
			ac.HasAccessInFolderFunc = func(ctx context.Context, user identity.Requester, folder models.Namespaced) (bool, error) {
				return false, nil
			}

			rules, _, token, err := service.ListAlertRules(context.Background(), u, ListAlertRulesOptions{})
			require.NoError(t, err)
			require.Empty(t, rules)
			require.Empty(t, token)
			require.Empty(t, ruleStore.RecordedOps)
		})

		t.Run("Include when user cannot read requested folder should return no rules", func(t *testing.T) {
			service, ruleStore, ac := initServiceWithData(t)
			ac.CanReadAllRulesFunc = func(ctx context.Context, user identity.Requester) (bool, error) {
				return false, nil
			}
			ac.HasAccessInFolderFunc = func(ctx context.Context, user identity.Requester, folder models.Namespaced) (bool, error) {
				return folder.GetNamespaceUID() == groupKey2.NamespaceUID, nil
			}

			rules, _, token, err := service.ListAlertRules(context.Background(), u, ListAlertRulesOptions{
				FolderFilter: ListRuleStringFilter{Include: []string{"inaccessible-folder"}},
			})
			require.NoError(t, err)
			require.Empty(t, rules)
			require.Empty(t, token)
			require.Empty(t, ruleStore.RecordedOps)
		})
	})
}
