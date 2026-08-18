package com.linkedin.openhouse.spark;

import com.linkedin.openhouse.javaclient.SessionBranchRefs;
import com.linkedin.openhouse.javaclient.SessionWapBranch;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

/**
 * Iceberg SparkCatalog that honors {@code spark.wap.branch} for OpenHouse DDL.
 *
 * <p>Configure: {@code
 * spark.sql.catalog.openhouse=com.linkedin.openhouse.spark.GitForDataSparkCatalog}
 *
 * <p>DROP resets the session branch to empty, stamps REST delete, and leaves the table. CREATE on
 * an existing table is ensure-branch, not TableAlreadyExists. RENAME stamps REST and the server
 * no-ops. ALTER is not filtered here; the client sanitizes schema/properties so they do not move
 * main.
 */
public class GitForDataSparkCatalog extends SparkCatalog {
  private static final Logger LOG = LoggerFactory.getLogger(GitForDataSparkCatalog.class);
  public static final String CONF_KEY = "spark.wap.branch";
  public static final String WAP_ENABLED = "write.wap.enabled";

  static Optional<String> sessionBranch() {
    try {
      Option<String> value = SparkSession.active().conf().getOption(CONF_KEY);
      if (value.isEmpty()) {
        return Optional.empty();
      }
      String trimmed = value.get().trim();
      return trimmed.isEmpty() ? Optional.empty() : Optional.of(trimmed);
    } catch (Throwable t) {
      return Optional.empty();
    }
  }

  @Override
  public boolean dropTable(Identifier ident) {
    SessionWapBranch.push(sessionBranch().orElse(null));
    try {
      return super.dropTable(ident);
    } finally {
      SessionWapBranch.pop();
    }
  }

  @Override
  public boolean purgeTable(Identifier ident) {
    // Iceberg purge deletes snapshot files. A branch shares main's files, so purge under a
    // session branch must empty the branch the same way DROP does, not expire prod data.
    if (sessionBranch().isPresent()) {
      return dropTable(ident);
    }
    SessionWapBranch.push(null);
    try {
      return super.purgeTable(ident);
    } finally {
      SessionWapBranch.pop();
    }
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties)
      throws TableAlreadyExistsException {
    Optional<String> branch = sessionBranch();
    if (branch.isPresent()) {
      Map<String, String> withWap = new HashMap<>(properties);
      withWap.put(WAP_ENABLED, "true");
      try {
        Table created = super.createTable(ident, schema, transforms, withWap);
        ensureBranch(ident, branch.get());
        return created;
      } catch (TableAlreadyExistsException e) {
        LOG.warn(
            "{} is set; CREATE TABLE {} already exists, ensuring branch {}",
            CONF_KEY,
            ident,
            branch.get());
        ensureBranch(ident, branch.get());
        try {
          return loadTable(ident);
        } catch (NoSuchTableException nste) {
          throw e;
        }
      } catch (AlreadyExistsException e) {
        LOG.warn(
            "{} is set; CREATE TABLE {} already exists, ensuring branch {}",
            CONF_KEY,
            ident,
            branch.get());
        ensureBranch(ident, branch.get());
        try {
          return loadTable(ident);
        } catch (NoSuchTableException nste) {
          throw new TableAlreadyExistsException(ident);
        }
      }
    }
    return super.createTable(ident, schema, transforms, properties);
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    Optional<String> branch = sessionBranch();
    SessionWapBranch.push(branch.orElse(null));
    try {
      invalidateTable(ident);
      Table table = super.loadTable(ident);
      if (branch.isPresent() && ensureLoadedBranch(table, branch.get())) {
        invalidateTable(ident);
        return super.loadTable(ident);
      }
      return table;
    } finally {
      SessionWapBranch.pop();
    }
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    SessionWapBranch.push(sessionBranch().orElse(null));
    try {
      return super.alterTable(ident, changes);
    } finally {
      SessionWapBranch.pop();
    }
  }

  @Override
  public void renameTable(Identifier from, Identifier to)
      throws NoSuchTableException, TableAlreadyExistsException {
    SessionWapBranch.push(sessionBranch().orElse(null));
    try {
      super.renameTable(from, to);
    } finally {
      SessionWapBranch.pop();
    }
  }

  private boolean ensureLoadedBranch(Table sparkTable, String branch) {
    if (!(sparkTable instanceof SparkTable)) {
      return false;
    }
    return SessionBranchRefs.ensureFromMain(((SparkTable) sparkTable).table(), branch);
  }

  private void ensureBranch(Identifier ident, String branch) {
    SparkSession spark = SparkSession.active();
    String table = name() + "." + ident.namespace()[0] + "." + ident.name();
    spark.sql("ALTER TABLE " + table + " SET TBLPROPERTIES ('" + WAP_ENABLED + "'='true')");
    try {
      spark.sql("ALTER TABLE " + table + " CREATE BRANCH " + branch);
    } catch (Exception e) {
      LOG.info("Branch {} already exists on {}: {}", branch, table, e.getMessage());
    }
  }
}
