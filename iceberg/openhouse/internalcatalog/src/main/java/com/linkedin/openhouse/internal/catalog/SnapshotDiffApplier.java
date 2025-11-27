package com.linkedin.openhouse.internal.catalog;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.getCanonicalFieldName;

import com.linkedin.openhouse.cluster.metrics.micrometer.MetricsReporter;
import com.linkedin.openhouse.internal.catalog.exception.InvalidIcebergSnapshotException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableMetadata;

/**
 * Service responsible for applying snapshot changes to Iceberg table metadata.
 *
 * <p>The main entry point applySnapshots() has a clear flow: parse input → compute diff → validate
 * → apply.
 */
@AllArgsConstructor
@Slf4j
public class SnapshotDiffApplier {

  private final MetricsReporter metricsReporter;

  /**
   * Applies snapshot updates from metadata properties. Simple and clear: parse input, compute diff,
   * validate, apply, record metrics, build.
   *
   * @param existingMetadata The existing table metadata (may be null for table creation)
   * @param providedMetadata The new metadata with properties containing snapshot updates
   * @return Updated metadata with snapshots applied
   * @throws NullPointerException if providedMetadata is null
   */
  public TableMetadata applySnapshots(
      TableMetadata existingMetadata, TableMetadata providedMetadata) {
    // Validate at system boundary
    Objects.requireNonNull(providedMetadata, "providedMetadata cannot be null");

    String snapshotsJson = providedMetadata.properties().get(CatalogConstants.SNAPSHOTS_JSON_KEY);
    Map<String, SnapshotRef> providedRefs =
        Optional.ofNullable(providedMetadata.properties().get(CatalogConstants.SNAPSHOTS_REFS_KEY))
            .map(SnapshotsUtil::parseSnapshotRefs)
            .orElse(new HashMap<>());

    if (snapshotsJson == null) {
      return providedMetadata;
    }

    // Parse input
    List<Snapshot> providedSnapshots = SnapshotsUtil.parseSnapshots(null, snapshotsJson);

    List<Snapshot> existingSnapshots =
        existingMetadata != null ? existingMetadata.snapshots() : Collections.emptyList();
    Map<String, SnapshotRef> existingRefs =
        existingMetadata != null ? existingMetadata.refs() : Collections.emptyMap();

    // Compute diff (all maps created once in factory method)
    SnapshotAnalysis analysis =
        SnapshotAnalysis.analyze(
            existingMetadata,
            providedMetadata,
            existingSnapshots,
            providedSnapshots,
            existingRefs,
            providedRefs);

    // Validate, apply, record metrics (in correct order)
    validate(analysis);
    TableMetadata result = apply(analysis);
    recordMetrics(analysis);
    return result;
  }

  /**
   * Validates all snapshot changes before applying them to table metadata.
   *
   * @param analysis The snapshot analysis result
   * @throws InvalidIcebergSnapshotException if any validation check fails
   */
  void validate(SnapshotAnalysis analysis) {
    validateCurrentSnapshotNotDeleted(analysis);
    validateDeletedSnapshotsNotReferenced(analysis);
  }

  /**
   * Validates that the current snapshot is not deleted without providing replacement snapshots.
   *
   * @param analysis The snapshot analysis result
   * @throws InvalidIcebergSnapshotException if the current snapshot is being deleted without
   *     replacements
   */
  private void validateCurrentSnapshotNotDeleted(SnapshotAnalysis analysis) {
    if (analysis.getExistingMetadata() == null
        || analysis.getExistingMetadata().currentSnapshot() == null) {
      return;
    }
    if (!analysis.getNewSnapshots().isEmpty()) {
      return;
    }
    long latestSnapshotId = analysis.getExistingMetadata().currentSnapshot().snapshotId();
    // Check if the last deleted snapshot is the current one (snapshots are ordered by time)
    if (!analysis.getDeletedSnapshots().isEmpty()
        && analysis.getDeletedSnapshots().stream()
            .anyMatch(s -> s.snapshotId() == latestSnapshotId)) {
      throw new InvalidIcebergSnapshotException(
          String.format(
              "Cannot delete the current snapshot %s without adding replacement snapshots.",
              latestSnapshotId));
    }
  }

  /**
   * Validates that snapshots being deleted are not still referenced by any branches or tags. This
   * prevents data loss and maintains referential integrity by ensuring that all branch and tag
   * pointers reference valid snapshots that will continue to exist after the commit.
   *
   * @param analysis The snapshot analysis result
   * @throws InvalidIcebergSnapshotException if any deleted snapshot is still referenced by a branch
   *     or tag
   */
  private void validateDeletedSnapshotsNotReferenced(SnapshotAnalysis analysis) {
    Map<Long, List<String>> referencedIdsToRefs =
        analysis.getProvidedRefs().entrySet().stream()
            .collect(
                Collectors.groupingBy(
                    e -> e.getValue().snapshotId(),
                    Collectors.mapping(Map.Entry::getKey, Collectors.toList())));

    Set<Long> deletedIds =
        analysis.getDeletedSnapshots().stream()
            .map(Snapshot::snapshotId)
            .collect(Collectors.toSet());

    List<String> invalidDeleteDetails =
        deletedIds.stream()
            .filter(referencedIdsToRefs::containsKey)
            .map(
                id ->
                    String.format(
                        "snapshot %s (referenced by: %s)",
                        id, String.join(", ", referencedIdsToRefs.get(id))))
            .collect(Collectors.toList());

    if (!invalidDeleteDetails.isEmpty()) {
      throw new InvalidIcebergSnapshotException(
          String.format(
              "Cannot delete snapshots that are still referenced by branches/tags: %s",
              String.join("; ", invalidDeleteDetails)));
    }
  }

  /**
   * Applies snapshot changes to the table metadata.
   *
   * <p>Application strategy:
   *
   * <p>[1] Remove deleted snapshots to clean up old data
   *
   * <p>[2] Remove stale branch references that are no longer needed
   *
   * <p>[3] Add unreferenced snapshots (auto-append to MAIN for backward compat, then staged WAP)
   *
   * <p>[4] Set branch pointers for all provided refs. Uses pre-computed deduplication set to
   * determine whether to call setBranchSnapshot (for new snapshots) or setRef (for existing
   * snapshots being fast-forwarded or cherry-picked).
   *
   * <p>[5] Set properties using pre-computed snapshot lists for tracking
   *
   * <p>This order ensures referential integrity is maintained throughout the operation.
   *
   * @param analysis The snapshot analysis result
   */
  TableMetadata apply(SnapshotAnalysis analysis) {
    TableMetadata.Builder builder = TableMetadata.buildFrom(analysis.getProvidedMetadata());

    // [1] Remove deletions
    if (!analysis.getDeletedSnapshots().isEmpty()) {
      Set<Long> deletedIds =
          analysis.getDeletedSnapshots().stream()
              .map(Snapshot::snapshotId)
              .collect(Collectors.toSet());
      builder.removeSnapshots(deletedIds);
    }
    analysis.getStaleRefs().forEach(builder::removeRef);

    // [2] Prepare snapshot actions: Map snapshot ID -> List of branches to set
    Map<Long, List<String>> branchesToSet = new HashMap<>();

    // a) From explicit branch updates
    analysis
        .getNewSnapshotsByBranch()
        .forEach(
            (branch, snapshots) ->
                snapshots.forEach(
                    s ->
                        branchesToSet
                            .computeIfAbsent(s.snapshotId(), k -> new ArrayList<>())
                            .add(branch)));

    // b) From auto-append (backward compatibility)
    analysis
        .getAutoAppendedToMainSnapshots()
        .forEach(
            s ->
                branchesToSet
                    .computeIfAbsent(s.snapshotId(), k -> new ArrayList<>())
                    .add(SnapshotRef.MAIN_BRANCH));

    // [3] Collect all snapshots to add (staged + branch + auto-append)
    List<Snapshot> allSnapshotsToAdd = new ArrayList<>();

    // Add staged/WAP snapshots
    analysis.getNewStagedSnapshots().stream()
        .filter(s -> analysis.getSnapshotIdsToAdd().contains(s.snapshotId()))
        .forEach(allSnapshotsToAdd::add);

    // Add branch/lineage snapshots
    analysis.getNewSnapshotsByBranch().values().stream()
        .flatMap(List::stream)
        .filter(s -> analysis.getSnapshotIdsToAdd().contains(s.snapshotId()))
        .forEach(allSnapshotsToAdd::add);

    // Add auto-appended snapshots
    analysis.getAutoAppendedToMainSnapshots().forEach(allSnapshotsToAdd::add);

    // [4] Apply snapshots sorted by sequence number to satisfy Iceberg validation
    Set<Long> processedSnapshotIds = new HashSet<>();
    allSnapshotsToAdd.stream()
        .filter(s -> processedSnapshotIds.add(s.snapshotId())) // Deduplicate by ID
        .sorted(java.util.Comparator.comparingLong(Snapshot::sequenceNumber))
        .forEach(
            snapshot -> {
              List<String> branches = branchesToSet.get(snapshot.snapshotId());
              boolean existsInMetadata =
                  analysis.getMetadataSnapshotIds().contains(snapshot.snapshotId());
              boolean isDeleted =
                  analysis.getDeletedSnapshots().stream()
                      .anyMatch(s -> s.snapshotId() == snapshot.snapshotId());

              if (branches != null && !branches.isEmpty()) {
                // Referenced by one or more branches
                if (existsInMetadata && !isDeleted) {
                  // Already exists, just set ref to avoid adding different object instance with
                  // same ID
                  branches.forEach(
                      branch ->
                          builder.setRef(
                              branch, SnapshotRef.branchBuilder(snapshot.snapshotId()).build()));
                } else {
                  // New, add and set ref
                  // Use setBranchSnapshot for the first branch to add the snapshot
                  // Use setRef for subsequent branches to avoid adding the same snapshot twice
                  String firstBranch = branches.get(0);
                  builder.setBranchSnapshot(snapshot, firstBranch);
                  for (int i = 1; i < branches.size(); i++) {
                    builder.setRef(
                        branches.get(i), SnapshotRef.branchBuilder(snapshot.snapshotId()).build());
                  }
                }
              } else {
                // Staged (unreferenced)
                // Only add if not already present (or if it was deleted and we are restoring it)
                if (!existsInMetadata || isDeleted) {
                  builder.addSnapshot(snapshot);
                }
              }
            });

    // [5] Set refs for non-new snapshots (tags, fast-forwards)
    analysis
        .getProvidedRefs()
        .forEach(
            (refName, ref) -> {
              // Skip if handled above (has new snapshots)
              if (analysis.getNewSnapshotsByBranch().containsKey(refName)
                  && !analysis.getNewSnapshotsByBranch().get(refName).isEmpty()) {
                return;
              }

              // For refs without new snapshots (fast-forward, cherry-pick, or tags)
              Snapshot refSnapshot = analysis.getProvidedSnapshotByIds().get(ref.snapshotId());
              if (refSnapshot == null) {
                throw new InvalidIcebergSnapshotException(
                    String.format(
                        "Ref %s references non-existent snapshot %s", refName, ref.snapshotId()));
              }
              builder.setRef(refName, ref);
            });

    // [6] Set properties
    setSnapshotProperties(builder, analysis);

    return builder.build();
  }

  /**
   * Sets snapshot-related properties on the metadata builder.
   *
   * <p>Records snapshot IDs in table properties for tracking and cleanup of temporary input
   * properties used for snapshot transfer.
   *
   * @param builder The metadata builder to set properties on
   * @param analysis The snapshot analysis result
   */
  private void setSnapshotProperties(TableMetadata.Builder builder, SnapshotAnalysis analysis) {
    // First, remove the temporary transfer properties
    builder.removeProperties(
        new HashSet<>(
            java.util.Arrays.asList(
                CatalogConstants.SNAPSHOTS_JSON_KEY, CatalogConstants.SNAPSHOTS_REFS_KEY)));

    // Then set the tracking properties
    if (!analysis.getMainBranchSnapshotsForMetrics().isEmpty()) {
      builder.setProperties(
          Collections.singletonMap(
              getCanonicalFieldName(CatalogConstants.APPENDED_SNAPSHOTS),
              formatSnapshotIds(analysis.getMainBranchSnapshotsForMetrics())));
    }
    if (!analysis.getNewStagedSnapshots().isEmpty()) {
      builder.setProperties(
          Collections.singletonMap(
              getCanonicalFieldName(CatalogConstants.STAGED_SNAPSHOTS),
              formatSnapshotIds(analysis.getNewStagedSnapshots())));
    }
    if (!analysis.getCherryPickedSnapshots().isEmpty()) {
      builder.setProperties(
          Collections.singletonMap(
              getCanonicalFieldName(CatalogConstants.CHERRY_PICKED_SNAPSHOTS),
              formatSnapshotIds(analysis.getCherryPickedSnapshots())));
    }
    if (!analysis.getDeletedSnapshots().isEmpty()) {
      builder.setProperties(
          Collections.singletonMap(
              getCanonicalFieldName(CatalogConstants.DELETED_SNAPSHOTS),
              formatSnapshotIds(analysis.getDeletedSnapshots())));
    }
  }

  /**
   * Records metrics for snapshot operations.
   *
   * <p>Tracks counts of: - Regular snapshots added (new commits and cherry-pick results) - Staged
   * snapshots (WAP) - Cherry-picked source snapshots - Deleted snapshots
   *
   * @param analysis The snapshot analysis result
   */
  void recordMetrics(SnapshotAnalysis analysis) {
    // Count only MAIN branch snapshots for backward compatibility
    int appendedCount =
        analysis
            .getNewSnapshotsByBranch()
            .getOrDefault(SnapshotRef.MAIN_BRANCH, Collections.emptyList())
            .size();
    recordMetricWithDatabaseTag(
        InternalCatalogMetricsConstant.SNAPSHOTS_ADDED_CTR,
        appendedCount,
        analysis.getDatabaseId());
    recordMetricWithDatabaseTag(
        InternalCatalogMetricsConstant.SNAPSHOTS_STAGED_CTR,
        analysis.getNewStagedSnapshots().size(),
        analysis.getDatabaseId());
    recordMetricWithDatabaseTag(
        InternalCatalogMetricsConstant.SNAPSHOTS_CHERRY_PICKED_CTR,
        analysis.getCherryPickedSnapshots().size(),
        analysis.getDatabaseId());
    recordMetricWithDatabaseTag(
        InternalCatalogMetricsConstant.SNAPSHOTS_DELETED_CTR,
        analysis.getDeletedSnapshots().size(),
        analysis.getDatabaseId());
  }

  /**
   * Helper method to record a metric with database tag if count is greater than zero.
   *
   * @param metricName The name of the metric to record
   * @param count The count value to record
   * @param databaseId The database ID to tag the metric with
   */
  private void recordMetricWithDatabaseTag(String metricName, int count, String databaseId) {
    if (count > 0) {
      // Only add database tag if databaseId is present; otherwise record metric without tag
      if (databaseId != null) {
        this.metricsReporter.count(
            metricName, count, InternalCatalogMetricsConstant.DATABASE_TAG, databaseId);
      } else {
        this.metricsReporter.count(metricName, count);
      }
    }
  }

  /**
   * Helper method to format a list of snapshots into a comma-separated string of snapshot IDs.
   *
   * @param snapshots List of snapshots to format
   * @return Comma-separated string of snapshot IDs
   */
  private static String formatSnapshotIds(List<Snapshot> snapshots) {
    return snapshots.stream()
        .map(s -> Long.toString(s.snapshotId()))
        .collect(Collectors.joining(","));
  }
}
