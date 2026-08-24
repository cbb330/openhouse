package com.linkedin.openhouse.javaclient;

import org.apache.iceberg.BaseMetadataTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WAP helpers for a session branch. Iceberg creates a missing {@code spark.wap.branch} ref on
 * commit; catalogs should {@link #enableWap} rather than pre-create. {@link #ensureFromMain} is
 * still used when a branch must exist so DROP can empty it.
 */
public final class SessionBranchRefs {

  private static final Logger LOG = LoggerFactory.getLogger(SessionBranchRefs.class);
  static final String WAP_ENABLED = "write.wap.enabled";

  private static final ThreadLocal<Boolean> ENSURING = ThreadLocal.withInitial(() -> Boolean.FALSE);

  private SessionBranchRefs() {}

  /**
   * Turn on {@code write.wap.enabled} so Iceberg honors {@code spark.wap.branch}. Does not create
   * refs.
   *
   * @return true if the property was set (caller should reload)
   */
  public static boolean enableWap(Table table) {
    if (table == null || table instanceof BaseMetadataTable) {
      return false;
    }
    if (Boolean.TRUE.equals(ENSURING.get())) {
      return false;
    }
    if ("true".equalsIgnoreCase(table.properties().getOrDefault(WAP_ENABLED, ""))) {
      return false;
    }
    ENSURING.set(Boolean.TRUE);
    try {
      LOG.info("Enabling write.wap.enabled on {}", table.name());
      table.updateProperties().set(WAP_ENABLED, "true").commit();
      table.refresh();
      return true;
    } finally {
      ENSURING.set(Boolean.FALSE);
    }
  }

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
