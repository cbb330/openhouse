package com.linkedin.openhouse.javaclient;

import org.apache.iceberg.BaseMetadataTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create a missing session branch from current main so a CI read cannot silently fall through to
 * live {@code main}. Iceberg WAP only honors {@code spark.wap.branch} when the ref exists.
 */
public final class SessionBranchRefs {

  private static final Logger LOG = LoggerFactory.getLogger(SessionBranchRefs.class);
  static final String WAP_ENABLED = "write.wap.enabled";

  private static final ThreadLocal<Boolean> ENSURING = ThreadLocal.withInitial(() -> Boolean.FALSE);

  private SessionBranchRefs() {}

  /**
   * Enable WAP and create {@code branch} from the current main snapshot when missing.
   *
   * @return true if WAP was enabled or a ref was created (caller should reload)
   */
  public static boolean ensureFromMain(Table table, String branch) {
    if (table == null || branch == null || branch.isEmpty()) {
      return false;
    }
    if (table instanceof BaseMetadataTable) {
      return false;
    }
    if (Boolean.TRUE.equals(ENSURING.get())) {
      return false;
    }
    ENSURING.set(Boolean.TRUE);
    try {
      boolean changed = false;
      if (!"true".equalsIgnoreCase(table.properties().getOrDefault(WAP_ENABLED, ""))) {
        table.updateProperties().set(WAP_ENABLED, "true").commit();
        table.refresh();
        changed = true;
      }
      if (!table.refs().containsKey(branch)) {
        Snapshot current = table.currentSnapshot();
        if (current == null) {
          LOG.info("Cannot create branch {} on {} yet: no snapshots", branch, table.name());
          return changed;
        }
        LOG.info(
            "Creating missing Iceberg branch {} on {} from current main", branch, table.name());
        table.manageSnapshots().createBranch(branch, current.snapshotId()).commit();
        changed = true;
      }
      return changed;
    } finally {
      ENSURING.set(Boolean.FALSE);
    }
  }
}
