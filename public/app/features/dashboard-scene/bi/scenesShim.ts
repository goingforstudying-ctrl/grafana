import { type AdHocFilterWithLabels, type SceneObject } from '@grafana/scenes';

/**
 * Local copy of the Scenes `DataRequestFiltersEnricher` interface.
 *
 * The @grafana/scenes version Grafana compiles against (9.2.2) does not have it yet. It is added by
 * grafana/scenes#1671, "feat(SceneQueryRunner): let the scene root adjust ad hoc filters per request".
 * Remove this file and import the interface from @grafana/scenes once a release with it is pinned here.
 */
export interface DataRequestFiltersEnricher {
  // Called with the ad hoc filters a SceneQueryRunner (source) is about to send. Return the filters to send instead.
  enrichDataRequestFilters(source: SceneObject, filters: AdHocFilterWithLabels[]): AdHocFilterWithLabels[];
}
