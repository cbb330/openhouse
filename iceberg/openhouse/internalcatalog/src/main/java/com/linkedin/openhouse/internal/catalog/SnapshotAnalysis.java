package com.linkedin.openhouse.internal.catalog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Value;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.TableMetadata;

/**
 * Value object holding the results of analyzing a snapshot update.
 *
 * <p>Contains categorized snapshots (new, deleted, staged, cherry-picked) and computed changes.
 * Encapsulates the categorization logic separate from application logic.
 */
@Value
@Builder
class SnapshotAnalysis {
  // Input state
  TableMetadata existingMetadata;
  TableMetadata providedMetadata;
  String databaseId;
  Map<String, SnapshotRef> providedRefs;

  // Computed maps
  Map<Long, Snapshot> providedSnapshotByIds;
  List<Snapshot> newSnapshots;
  List<Snapshot> deletedSnapshots;
  Set<Long> metadataSnapshotIds;

  // Categorized snapshots
  List<Snapshot> newStagedSnapshots;
  List<Snapshot> cherryPickedSnapshots;

  // Changes
  Map<String, SnapshotRef> branchUpdates;
  Map<String, List<Snapshot>> newSnapshotsByBranch;
  Set<String> staleRefs;
  List<Snapshot> unreferencedNewSnapshots;

  // Application state
  List<Snapshot> autoAppendedToMainSnapshots;
  List<Snapshot> mainBranchSnapshotsForMetrics;
  Set<Long> snapshotIdsToAdd;

  /**
   * Analyzes snapshot updates to compute categorization and changes.
   *
   * @param existingMetadata The existing table metadata (may be null)
   * @param providedMetadata The new metadata with properties containing snapshot updates
   * @param existingSnapshots Snapshots currently in the table
   * @param providedSnapshots Snapshots provided in the update
   * @param existingRefs Snapshot refs currently in the table
   * @param providedRefs Snapshot refs provided in the update
   * @return A SnapshotAnalysis object with all computed state
   */
  static SnapshotAnalysis analyze(
      TableMetadata existingMetadata,
      TableMetadata providedMetadata,
      List<Snapshot> existingSnapshots,
      List<Snapshot> providedSnapshots,
      Map<String, SnapshotRef> existingRefs,
      Map<String, SnapshotRef> providedRefs) {

    // Compute all index maps once
    Map<Long, Snapshot> providedSnapshotByIds =
        providedSnapshots.stream()
            .collect(Collectors.toMap(Snapshot::snapshotId, s -> s, (s1, s2) -> s1));
    Map<Long, Snapshot> existingSnapshotByIds =
        existingSnapshots.stream().collect(Collectors.toMap(Snapshot::snapshotId, s -> s));

    // Compute changes
    List<Snapshot> newSnapshots =
        providedSnapshots.stream()
            .filter(s -> !existingSnapshotByIds.containsKey(s.snapshotId()))
            .collect(Collectors.toList());
    List<Snapshot> deletedSnapshots =
        existingSnapshots.stream()
            .filter(s -> !providedSnapshotByIds.containsKey(s.snapshotId()))
            .collect(Collectors.toList());

    // Categorize snapshots - process in dependency order
    // 1. Cherry-picked has highest priority (includes WAP being published)
    // 2. WAP snapshots (staged, not published)
    // 3. Regular snapshots (everything else)
    Set<Long> existingBranchRefIds =
        existingRefs.values().stream().map(SnapshotRef::snapshotId).collect(Collectors.toSet());
    Set<Long> providedBranchRefIds =
        providedRefs.values().stream().map(SnapshotRef::snapshotId).collect(Collectors.toSet());

    List<Snapshot> cherryPickedSnapshots =
        computeCherryPickedSnapshots(
            providedSnapshots, existingSnapshotByIds, existingBranchRefIds, providedBranchRefIds);
    Set<Long> cherryPickedIds =
        cherryPickedSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());

    // Compute WAP snapshots (those with explicit wap.id property)
    List<Snapshot> wapSnapshots =
        computeWapSnapshots(
            providedSnapshots, cherryPickedIds, existingBranchRefIds, providedBranchRefIds);
    Set<Long> wapIds = wapSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());

    // Regular snapshots are everything except cherry-picked and WAP
    List<Snapshot> regularSnapshots =
        computeRegularSnapshots(providedSnapshots, cherryPickedIds, wapIds);

    // Compute branch updates (refs that have changed)
    Map<String, SnapshotRef> branchUpdates =
        providedRefs.entrySet().stream()
            .filter(
                entry -> {
                  SnapshotRef existing = existingRefs.get(entry.getKey());
                  return existing == null || existing.snapshotId() != entry.getValue().snapshotId();
                })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    // Compute new snapshots by branch (only regular snapshots)
    Set<Long> regularSnapshotIds =
        regularSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
    Map<String, List<Snapshot>> newSnapshotsByBranch =
        computeNewSnapshotsByBranch(
            branchUpdates, providedSnapshotByIds, existingSnapshotByIds, regularSnapshotIds);

    // Compute derived changes for multi-branch support
    Set<String> staleRefs = new HashSet<>(existingRefs.keySet());
    staleRefs.removeAll(providedRefs.keySet());

    // Collect all snapshot IDs that are in branch lineages (will be added via setBranchSnapshot)
    Set<Long> snapshotsInBranchLineages =
        newSnapshotsByBranch.values().stream()
            .flatMap(List::stream)
            .map(Snapshot::snapshotId)
            .collect(Collectors.toSet());

    // Compute existing snapshot IDs after deletion
    Set<Long> deletedIds =
        deletedSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
    Set<Long> existingAfterDeletionIds = new HashSet<>(existingSnapshotByIds.keySet());
    existingAfterDeletionIds.removeAll(deletedIds);

    // Compute metadata snapshot IDs
    Set<Long> metadataSnapshotIds =
        providedMetadata.snapshots().stream().map(Snapshot::snapshotId).collect(Collectors.toSet());

    List<Snapshot> unreferencedNewSnapshots =
        providedSnapshots.stream()
            .filter(
                s ->
                    !existingAfterDeletionIds.contains(s.snapshotId())
                        && !providedBranchRefIds.contains(s.snapshotId())
                        && !snapshotsInBranchLineages.contains(s.snapshotId()))
            .collect(Collectors.toList());

    // Staged snapshots = unreferenced new snapshots that are not cherry-picked or WAP
    // These are snapshots created with .stageOnly() without explicit wap.id
    List<Snapshot> stagedSnapshots =
        unreferencedNewSnapshots.stream()
            .filter(s -> !cherryPickedIds.contains(s.snapshotId()))
            .filter(s -> !wapIds.contains(s.snapshotId()))
            .collect(Collectors.toList());

    // Combine WAP and staged snapshots for tracking purposes
    List<Snapshot> newStagedSnapshots = new ArrayList<>();
    newStagedSnapshots.addAll(wapSnapshots);
    newStagedSnapshots.addAll(stagedSnapshots);
    Set<Long> allStagedIds =
        newStagedSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());

    // Compute auto-appended snapshots for MAIN branch (backward compatibility)
    final List<Snapshot> autoAppendedToMainSnapshots;
    if (providedRefs.isEmpty() && !unreferencedNewSnapshots.isEmpty()) {
      autoAppendedToMainSnapshots =
          unreferencedNewSnapshots.stream()
              .filter(s -> !allStagedIds.contains(s.snapshotId()))
              .collect(Collectors.toList());
    } else {
      autoAppendedToMainSnapshots = Collections.emptyList();
    }

    // Main branch snapshots for metrics (includes auto-appended)
    List<Snapshot> mainForMetrics =
        new ArrayList<>(
            newSnapshotsByBranch.getOrDefault(SnapshotRef.MAIN_BRANCH, Collections.emptyList()));
    mainForMetrics.addAll(autoAppendedToMainSnapshots);

    // Global set of snapshot IDs to add (deduplicated upfront)
    Set<Long> snapshotIdsToAdd = new HashSet<>();
    autoAppendedToMainSnapshots.stream().map(Snapshot::snapshotId).forEach(snapshotIdsToAdd::add);
    newSnapshotsByBranch.values().stream()
        .flatMap(List::stream)
        .map(Snapshot::snapshotId)
        .forEach(snapshotIdsToAdd::add);
    newStagedSnapshots.stream().map(Snapshot::snapshotId).forEach(snapshotIdsToAdd::add);

    // Extract database ID from metadata properties
    String databaseId =
        providedMetadata.properties().get(CatalogConstants.OPENHOUSE_DATABASEID_KEY);

    return SnapshotAnalysis.builder()
        .existingMetadata(existingMetadata)
        .providedMetadata(providedMetadata)
        .databaseId(databaseId)
        .providedRefs(providedRefs)
        .providedSnapshotByIds(providedSnapshotByIds)
        .newSnapshots(newSnapshots)
        .deletedSnapshots(deletedSnapshots)
        .metadataSnapshotIds(metadataSnapshotIds)
        .newStagedSnapshots(newStagedSnapshots)
        .cherryPickedSnapshots(cherryPickedSnapshots)
        .branchUpdates(branchUpdates)
        .newSnapshotsByBranch(newSnapshotsByBranch)
        .staleRefs(staleRefs)
        .unreferencedNewSnapshots(unreferencedNewSnapshots)
        .autoAppendedToMainSnapshots(autoAppendedToMainSnapshots)
        .mainBranchSnapshotsForMetrics(mainForMetrics)
        .snapshotIdsToAdd(snapshotIdsToAdd)
        .build();
  }

  private static List<Snapshot> computeWapSnapshots(
      List<Snapshot> providedSnapshots,
      Set<Long> excludeCherryPicked,
      Set<Long> existingBranchRefIds,
      Set<Long> providedBranchRefIds) {
    Set<Long> allBranchRefIds =
        java.util.stream.Stream.concat(existingBranchRefIds.stream(), providedBranchRefIds.stream())
            .collect(Collectors.toSet());

    // WAP snapshots are identified by the presence of wap.id property
    return providedSnapshots.stream()
        .filter(s -> !excludeCherryPicked.contains(s.snapshotId()))
        .filter(
            s ->
                s.summary() != null
                    && s.summary().containsKey(SnapshotSummary.STAGED_WAP_ID_PROP)
                    && !allBranchRefIds.contains(s.snapshotId()))
        .collect(Collectors.toList());
  }

  private static List<Snapshot> computeCherryPickedSnapshots(
      List<Snapshot> providedSnapshots,
      Map<Long, Snapshot> existingSnapshotByIds,
      Set<Long> existingBranchRefIds,
      Set<Long> providedBranchRefIds) {
    Set<Long> cherryPickSourceIds =
        providedSnapshots.stream()
            .filter(
                s ->
                    s.summary() != null
                        && s.summary().containsKey(SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP))
            .map(s -> Long.parseLong(s.summary().get(SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP)))
            .collect(Collectors.toSet());

    return providedSnapshots.stream()
        .filter(
            provided -> {
              // Only consider EXISTING snapshots as cherry-picked
              Snapshot existing = existingSnapshotByIds.get(provided.snapshotId());
              if (existing == null) {
                return false;
              }

              // Parent changed (moved to different branch)
              if (!Objects.equals(provided.parentId(), existing.parentId())) {
                return true;
              }

              // Is source of cherry-pick
              if (cherryPickSourceIds.contains(provided.snapshotId())) {
                return true;
              }

              // WAP snapshot being published (staged → branch)
              boolean hasWapId =
                  provided.summary() != null
                      && provided.summary().containsKey(SnapshotSummary.STAGED_WAP_ID_PROP);
              boolean wasStaged = !existingBranchRefIds.contains(provided.snapshotId());
              boolean isNowOnBranch = providedBranchRefIds.contains(provided.snapshotId());
              return hasWapId && wasStaged && isNowOnBranch;
            })
        .collect(Collectors.toList());
  }

  private static List<Snapshot> computeRegularSnapshots(
      List<Snapshot> providedSnapshots, Set<Long> excludeCherryPicked, Set<Long> excludeWap) {
    return providedSnapshots.stream()
        .filter(s -> !excludeCherryPicked.contains(s.snapshotId()))
        .filter(s -> !excludeWap.contains(s.snapshotId()))
        .collect(Collectors.toList());
  }

  private static Map<String, List<Snapshot>> computeNewSnapshotsByBranch(
      Map<String, SnapshotRef> branchUpdates,
      Map<Long, Snapshot> providedSnapshotByIds,
      Map<Long, Snapshot> existingSnapshotByIds,
      Set<Long> regularSnapshotIds) {

    Map<String, List<Snapshot>> result = new HashMap<>();

    branchUpdates.forEach(
        (branchName, newRef) -> {
          java.util.Deque<Snapshot> branchNewSnapshots = new java.util.ArrayDeque<>();
          long currentId = newRef.snapshotId();

          // Walk backwards from new branch head until we hit an existing snapshot (merge base)
          while (currentId != -1) {
            Snapshot snapshot = providedSnapshotByIds.get(currentId);
            if (snapshot == null) {
              break;
            }

            // Stop at existing snapshot (merge base or old branch head)
            if (existingSnapshotByIds.containsKey(currentId)) {
              break;
            }

            // Collect new regular snapshots in chronological order (add to front)
            if (regularSnapshotIds.contains(currentId)) {
              branchNewSnapshots.addFirst(snapshot);
            }

            currentId = snapshot.parentId() != null ? snapshot.parentId() : -1;
          }

          if (!branchNewSnapshots.isEmpty()) {
            result.put(branchName, new ArrayList<>(branchNewSnapshots));
          }
        });

    return result;
  }
}
