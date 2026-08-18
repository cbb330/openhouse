package com.linkedin.openhouse.javaclient;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;

/**
 * M1 commit sanitizer: restore main's schema, spec, sort, user properties, and policies so a
 * branched session does not move prod metadata. {@code current-snapshot-id} is not touched —
 * Iceberg {@code toBranch} owns snapshot isolation. Identity fields are ignored, not refused.
 *
 * <p>There is no sidecar overlay file and no {@code gfd.ref.*} leak onto main. After reload the
 * branch looks like prod again. {@link #project} / {@link #capture} stay for unit tests and the
 * later clone metadata file (M3).
 *
 * <p>Reserved {@code gfd.ref.*} keys are stripped from the presented map even when no branch is set
 * (migration from the property-map leak). {@code write.wap.enabled} may pass through.
 */
final class BranchOverlay {

  static final String KEY_PREFIX = "gfd.ref.";
  static final String WAP_ENABLED = "write.wap.enabled";

  private static final Gson GSON = new Gson();
  private static final Type STRING_MAP = new TypeToken<Map<String, String>>() {}.getType();

  private static final Set<String> IDENTITY_PROPERTIES;

  static {
    Set<String> identity = new HashSet<String>();
    identity.add("location");
    identity.add("write.metadata.path");
    identity.add("openhouse.encrypted");
    identity.add("openhouse.tableType");
    identity.add("openhouse.tableId");
    identity.add("openhouse.tableUUID");
    identity.add("openhouse.tableUri");
    identity.add("openhouse.databaseId");
    identity.add("openhouse.clusterId");
    identity.add("openhouse.tableCreator");
    identity.add("openhouse.isTableReplicated");
    identity.add("openhouse.tableLocation");
    IDENTITY_PROPERTIES = Collections.unmodifiableSet(identity);
  }

  private BranchOverlay() {}

  static String schemaKey(String branch) {
    return KEY_PREFIX + branch + ".schema";
  }

  static String specKey(String branch) {
    return KEY_PREFIX + branch + ".spec";
  }

  static String sortKey(String branch) {
    return KEY_PREFIX + branch + ".sort";
  }

  static String propertiesKey(String branch) {
    return KEY_PREFIX + branch + ".properties";
  }

  static boolean isReservedKey(String key) {
    return key != null && key.startsWith(KEY_PREFIX);
  }

  static boolean isIdentityProperty(String key) {
    return key != null && IDENTITY_PROPERTIES.contains(key);
  }

  static TableMetadata project(TableMetadata durable, String branch) {
    return project(durable, branch, bagFromProperties(durable, branch));
  }

  static TableMetadata project(TableMetadata durable, String branch, Bag bag) {
    if (durable == null) {
      return null;
    }
    if (branch == null || branch.isEmpty()) {
      return hideReservedKeys(durable);
    }
    if (bag == null || bag.isEmpty()) {
      return hideReservedKeys(durable);
    }

    TableMetadata.Builder builder = TableMetadata.buildFrom(durable);
    Schema schema = durable.schema();
    if (bag.schema != null) {
      schema = SchemaParser.fromJson(bag.schema);
      if (!schema.sameSchema(durable.schema())) {
        builder.setCurrentSchema(schema, Math.max(durable.lastColumnId(), schema.highestFieldId()));
      }
    }
    if (bag.spec != null) {
      builder.setDefaultPartitionSpec(PartitionSpecParser.fromJson(schema, bag.spec));
    }
    if (bag.sort != null) {
      builder.setDefaultSortOrder(SortOrderParser.fromJson(schema, bag.sort));
    }

    Map<String, String> stored = durable.properties();
    Map<String, String> projected = new HashMap<>(stored);
    projected.putAll(bag.properties == null ? Collections.emptyMap() : bag.properties);
    Set<String> reserved = new HashSet<>();
    for (String key : stored.keySet()) {
      if (isReservedKey(key)) {
        reserved.add(key);
      }
    }
    projected.keySet().removeIf(BranchOverlay::isReservedKey);
    builder.setProperties(projected);
    if (!reserved.isEmpty()) {
      builder.removeProperties(reserved);
    }
    return withMetadataLocation(durable, builder.build());
  }

  /**
   * Persist the projected view onto a {@link Bag} for the ref / overlay file. Does not mutate
   * metadata.
   */
  static Bag capture(TableMetadata durable, TableMetadata projected, String branch) {
    if (durable == null || projected == null || branch == null || branch.isEmpty()) {
      return Bag.empty();
    }
    Bag bag = new Bag();
    bag.schema = SchemaParser.toJson(projected.schema(), false);
    bag.spec = PartitionSpecParser.toJson(projected.spec(), false);
    bag.sort = SortOrderParser.toJson(projected.sortOrder());
    bag.properties = overlayPropertyBag(durable, projected);
    return bag;
  }

  /**
   * Return metadata whose schema/spec/sort and user properties (including policies) are main's.
   * Identity changes are ignored. Leftover {@code gfd.ref.*} keys are stripped so the house-table
   * request does not leak overlay storage. {@code write.wap.enabled} may pass through.
   */
  static TableMetadata sanitize(TableMetadata durable, TableMetadata projected, String branch) {
    if (durable == null || projected == null || branch == null || branch.isEmpty()) {
      return projected;
    }

    Set<String> reserved = new HashSet<>();
    for (String key : durable.properties().keySet()) {
      if (isReservedKey(key)) {
        reserved.add(key);
      }
    }
    TableMetadata.Builder builder = TableMetadata.buildFrom(durable);
    if (projected.properties().containsKey(WAP_ENABLED)) {
      Map<String, String> wap = new HashMap<>();
      wap.put(WAP_ENABLED, projected.properties().get(WAP_ENABLED));
      builder.setProperties(wap);
    }
    if (!reserved.isEmpty()) {
      builder.removeProperties(reserved);
    }
    return withMetadataLocation(durable, builder.build());
  }

  static Bag bagFromProperties(TableMetadata durable, String branch) {
    if (durable == null || branch == null || branch.isEmpty()) {
      return Bag.empty();
    }
    Map<String, String> stored = durable.properties();
    Bag bag = new Bag();
    bag.schema = stored.get(schemaKey(branch));
    bag.spec = stored.get(specKey(branch));
    bag.sort = stored.get(sortKey(branch));
    bag.properties = parsePropertyBag(stored.get(propertiesKey(branch)));
    return bag;
  }

  /** Strip {@code gfd.ref.*} from the presented property map. Storage on disk is unchanged. */
  static TableMetadata hideReservedKeys(TableMetadata metadata) {
    if (metadata == null) {
      return null;
    }
    Set<String> reserved = new HashSet<>();
    for (String key : metadata.properties().keySet()) {
      if (isReservedKey(key)) {
        reserved.add(key);
      }
    }
    if (reserved.isEmpty()) {
      return metadata;
    }
    return withMetadataLocation(
        metadata, TableMetadata.buildFrom(metadata).removeProperties(reserved).build());
  }

  /**
   * {@link TableMetadata.Builder#build()} clears {@code metadata-file}. OpenHouse uses that path as
   * {@code baseTableVersion}; a projected current() without it makes the next snapshot commit 500.
   */
  private static TableMetadata withMetadataLocation(TableMetadata source, TableMetadata built) {
    if (source == null || built == null) {
      return built;
    }
    String location = source.metadataFileLocation();
    if (location == null || Objects.equals(location, built.metadataFileLocation())) {
      return built;
    }
    return TableMetadataParser.fromJson(location, TableMetadataParser.toJson(built));
  }

  private static Map<String, String> overlayPropertyBag(
      TableMetadata durable, TableMetadata projected) {
    Map<String, String> bag = new HashMap<>();
    for (Map.Entry<String, String> entry : projected.properties().entrySet()) {
      String key = entry.getKey();
      if (isReservedKey(key) || isIdentityProperty(key) || WAP_ENABLED.equals(key)) {
        continue;
      }
      if (!Objects.equals(durable.properties().get(key), entry.getValue())) {
        bag.put(key, entry.getValue());
      }
    }
    return bag;
  }

  private static Map<String, String> parsePropertyBag(String json) {
    if (json == null || json.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, String> parsed = GSON.fromJson(json, STRING_MAP);
    return parsed == null ? Collections.emptyMap() : parsed;
  }

  /** Overlay payload stored next to the metadata file (and later on clone metadata). */
  static final class Bag {
    String schema;
    String spec;
    String sort;
    Map<String, String> properties;

    static Bag empty() {
      return new Bag();
    }

    boolean isEmpty() {
      return schema == null
          && spec == null
          && sort == null
          && (properties == null || properties.isEmpty());
    }

    String toJson() {
      return GSON.toJson(this);
    }

    static Bag fromJson(String json) {
      if (json == null || json.isEmpty()) {
        return empty();
      }
      Bag parsed = GSON.fromJson(json, Bag.class);
      return parsed == null ? empty() : parsed;
    }
  }
}
