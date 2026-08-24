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
 * Iceberg Spark catalog jobs should use. When {@code spark.wap.branch} is set, DDL and DML stamp
 * that branch on Tables REST ({@code X-OH-Wap-Branch} / {@code X-Iceberg-Ref}) and turn on {@code
 * write.wap.enabled}. Iceberg creates a missing WAP ref on commit. DROP / RENAME are no-ops on the
 * house table for a non-main branch (see {@code OpenHouseCatalog}).
 *
 * <p>Configure: {@code
 * spark.sql.catalog.openhouse=com.linkedin.openhouse.spark.OpenHouseSparkCatalog}
 */
public class OpenHouseSparkCatalog extends SparkCatalog {
  private static final Logger LOG = LoggerFactory.getLogger(OpenHouseSparkCatalog.class);
  public static final String CONF_KEY = "spark.wap.branch";
  public static final String WAP_ENABLED = "write.wap.enabled";

  @Override
  public boolean useNullableQuerySchema() {
    return false;
  }

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
    Optional<String> branch = sessionBranch();
    if (!branch.isPresent()) {
      return super.dropTable(ident);
    }
    SessionWapBranch.push(branch.get());
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
    return super.purgeTable(ident);
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties)
      throws TableAlreadyExistsException {
    Optional<String> branch = sessionBranch();
    if (!branch.isPresent()) {
      return super.createTable(ident, schema, transforms, properties);
    }
    Map<String, String> withWap = new HashMap<>(properties);
    withWap.put(WAP_ENABLED, "true");
    try {
      return super.createTable(ident, schema, transforms, withWap);
    } catch (TableAlreadyExistsException e) {
      LOG.warn("{} is set; CREATE TABLE {} already exists, enabling WAP", CONF_KEY, ident);
      try {
        return loadTable(ident);
      } catch (NoSuchTableException nste) {
        e.initCause(nste);
        throw e;
      }
    } catch (AlreadyExistsException e) {
      LOG.warn("{} is set; CREATE TABLE {} already exists, enabling WAP", CONF_KEY, ident);
      try {
        return loadTable(ident);
      } catch (NoSuchTableException nste) {
        TableAlreadyExistsException exists = new TableAlreadyExistsException(ident);
        exists.initCause(nste);
        throw exists;
      }
    }
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    Optional<String> branch = sessionBranch();
    if (!branch.isPresent()) {
      return super.loadTable(ident);
    }
    SessionWapBranch.push(branch.get());
    try {
      invalidateTable(ident);
      Table table = super.loadTable(ident);
      if (enableWap(table)) {
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
    Optional<String> branch = sessionBranch();
    if (!branch.isPresent()) {
      return super.alterTable(ident, changes);
    }
    SessionWapBranch.push(branch.get());
    try {
      return super.alterTable(ident, changes);
    } finally {
      SessionWapBranch.pop();
    }
  }

  @Override
  public void renameTable(Identifier from, Identifier to)
      throws NoSuchTableException, TableAlreadyExistsException {
    Optional<String> branch = sessionBranch();
    if (!branch.isPresent()) {
      super.renameTable(from, to);
      return;
    }
    SessionWapBranch.push(branch.get());
    try {
      super.renameTable(from, to);
    } finally {
      SessionWapBranch.pop();
    }
  }

  private static boolean enableWap(Table sparkTable) {
    if (!(sparkTable instanceof SparkTable)) {
      return false;
    }
    return SessionBranchRefs.enableWap(((SparkTable) sparkTable).table());
  }
}
