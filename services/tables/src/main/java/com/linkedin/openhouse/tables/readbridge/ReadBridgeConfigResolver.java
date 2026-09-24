package com.linkedin.openhouse.tables.readbridge;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.readbridge.ColumnDefaultException.Origin;
import com.linkedin.openhouse.tables.readbridge.ColumnDefaultException.Reason;
import com.linkedin.openhouse.tables.toggle.TableFeatureToggle;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;

/**
 * Stamps per-table {@code config} for read-bridge capabilities. Owns policy (feature id, ramp,
 * keys); deployments supply data via {@link ColumnDefaultsSource}.
 */
public class ReadBridgeConfigResolver {

  /** Capability id; also names {@code <id>.enabled} and the config key prefix below. */
  public static final String COLUMN_DEFAULT_FEATURE_ID = "read-bridge.column-default";

  /** Client contract: {@code openhouse.read-bridge.column-default.<fieldId>}. */
  public static final String COLUMN_DEFAULT_PREFIX = "openhouse." + COLUMN_DEFAULT_FEATURE_ID + ".";

  private final ColumnDefaultsSource columnDefaultsSource;

  private final TableFeatureToggle featureToggle;

  public ReadBridgeConfigResolver(
      ColumnDefaultsSource columnDefaultsSource, TableFeatureToggle featureToggle) {
    this.columnDefaultsSource =
        Objects.requireNonNull(columnDefaultsSource, "columnDefaultsSource");
    this.featureToggle = Objects.requireNonNull(featureToggle, "featureToggle");
  }

  /** Stamps stored defaults; an unusable source or ramp lookup fails the read. */
  public Map<String, String> resolve(TableDto tableDto) throws ColumnDefaultException {
    Map<Integer, String> byId;
    try {
      byId = stampedColumnDefaults(tableDto);
    } catch (ColumnDefaultException e) {
      throw e.withOrigin(Origin.STORED);
    }
    if (byId.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, String> config = new HashMap<>();
    byId.forEach((fieldId, json) -> config.put(COLUMN_DEFAULT_PREFIX + fieldId, json));
    return config;
  }

  /**
   * Stamps keyed by Iceberg field-id. Empty when there is no source or the table is not ramped.
   * Toggle or source failures propagate on both reads and writes.
   *
   * @throws ColumnDefaultException if the source or ramp lookup cannot answer
   */
  public Map<Integer, String> stampedColumnDefaults(TableDto tableDto)
      throws ColumnDefaultException {
    Objects.requireNonNull(tableDto, "tableDto");
    try {
      return columnDefaultsByFieldId(tableDto);
    } catch (RuntimeException e) {
      throw new ColumnDefaultException(Reason.INTERNAL, tableDto, e);
    }
  }

  /**
   * Write-path ramp. Toggle failure throws. {@code ColumnDefaultsSource.NONE} is never ramped, so
   * the toggle is not consulted.
   *
   * @throws ColumnDefaultException if the ramp lookup cannot answer
   */
  public boolean isRampedForCommit(TableDto tableDto) throws ColumnDefaultException {
    Objects.requireNonNull(tableDto, "tableDto");
    return isColumnDefaultRamped(tableDto);
  }

  private Map<Integer, String> columnDefaultsByFieldId(TableDto tableDto)
      throws ColumnDefaultException {
    if (columnDefaultsSource == ColumnDefaultsSource.NONE) {
      return Collections.emptyMap();
    }
    if (!isColumnDefaultRamped(tableDto)) {
      return Collections.emptyMap();
    }
    Map<Integer, JsonNode> columnDefaults =
        Objects.requireNonNull(
            columnDefaultsSource.defaults(tableDto), "Column-default source returned null");
    if (columnDefaults.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<Integer, String> byId = new HashMap<>();
    columnDefaults.forEach(
        (fieldId, value) ->
            byId.put(
                Objects.requireNonNull(fieldId, "Column-default field id is null"),
                Objects.requireNonNull(value, "Column-default value is null").toString()));
    return byId;
  }

  /**
   * Uses {@link TableFeatureToggle#isFeatureActivatedWithOverride} so {@code
   * read-bridge.column-default.enabled} can opt in/out without HTS. A lookup failure rejects the
   * operation rather than silently omitting defaults.
   */
  private boolean isColumnDefaultRamped(TableDto tableDto) throws ColumnDefaultException {
    if (columnDefaultsSource == ColumnDefaultsSource.NONE) {
      return false;
    }
    try {
      return featureToggle.isFeatureActivatedWithOverride(tableDto, COLUMN_DEFAULT_FEATURE_ID);
    } catch (WebClientRequestException e) {
      throw new ColumnDefaultException(Reason.UNAVAILABLE, tableDto, e);
    } catch (WebClientResponseException e) {
      Reason reason =
          e.getStatusCode().is5xxServerError()
                  || e.getRawStatusCode() == 429
                  || e.getRawStatusCode() == 408
              ? Reason.UNAVAILABLE
              : Reason.INTERNAL;
      throw new ColumnDefaultException(reason, tableDto, e);
    } catch (RuntimeException e) {
      throw new ColumnDefaultException(Reason.INTERNAL, tableDto, e);
    }
  }
}
