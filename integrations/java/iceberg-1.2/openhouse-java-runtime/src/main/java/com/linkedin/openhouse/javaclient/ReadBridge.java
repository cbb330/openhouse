package com.linkedin.openhouse.javaclient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;

/**
 * Read-time bridge: overlays Iceberg V3 read semantics onto loaded metadata for tables/clients that
 * don't yet carry them natively, using behavior the server delivers in the per-table {@code
 * config}. Today it applies per-column initial-defaults; further V3 features can be backported
 * through the same entry point as they are added.
 *
 * <p>Client end of the read-bridge wire contract — mirror of the server encoder {@code
 * ReadBridgeConfigResolver} (services/tables). The contract is flat, namespaced config keys (no
 * envelope/POJO): {@code openhouse.read-bridge.column-default.<fieldId> = <single-value-json>}.
 *
 * <h3>Decode before IO; mark bridge failures unrecoverable</h3>
 *
 * <p>{@link #from(Map)} decodes the config; {@link #apply(TableMetadata)} overlays the result onto
 * loaded metadata. {@link OpenHouseTableOperations#loadMetadata} calls {@code from} <em>before</em>
 * reading the metadata file so a malformed config never touches storage, then {@code apply} after.
 * Both steps throw {@link IllegalStateException} on invariant violations; the loader wraps those as
 * Iceberg's {@code Tasks.UnrecoverableException} so {@code Tasks.retry(20)} around the metadata
 * read does not burn ~90s re-reading the file to reproduce a deterministic failure.
 *
 * <p>A read-bridge entry is produced by the server encoder from typed {@code JsonNode}s keyed by
 * integer field-id, so its value always round-trips through {@code readTree} and its suffix always
 * parses as an int. A decode failure on a <em>known</em> entry is therefore a bug or transport
 * corruption, not an expected runtime state, and this fails loud rather than silently degrading to
 * NULL. An <em>unknown</em> key (a newer server feature this client doesn't recognize) is ignored,
 * preserving forward compatibility. With nothing to bridge, metadata is returned unchanged.
 *
 * <p>Note this guarantees only that a stamped value is <em>well-formed</em>, not that it is the
 * <em>correct</em> default for its column — that semantic (default-to-schema) consistency is a
 * write-time concern owned by whatever server path sources the defaults.
 *
 * <p>{@link #sanitize} strips {@code initial-default} on field-ids this bridge stamped so an
 * overlay cannot persist. Unstamped ids keep the writer's defaults. Bridge XOR native: the encoder
 * does not stamp a field-id that already has an on-disk default. Apply patches {@code
 * initial-default} onto schema field objects by field-id; a default that cannot bind throws.
 */
final class ReadBridge {

  /** Mirror of {@code ReadBridgeConfigResolver.COLUMN_DEFAULT_PREFIX}. */
  static final String COLUMN_DEFAULT_PREFIX = "openhouse.read-bridge.column-default.";

  /** Nothing to bridge; {@link #apply(TableMetadata)} returns metadata untouched. */
  static final ReadBridge INERT = new ReadBridge(Collections.emptyMap());

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String SCHEMAS = "schemas";
  private static final String ID = "id";
  private static final String INITIAL_DEFAULT = "initial-default";

  /** JSON strings, not JsonNodes — Jackson is relocated in the shaded client. */
  private final Map<Integer, String> columnDefaults;

  private ReadBridge(Map<Integer, String> columnDefaults) {
    this.columnDefaults = columnDefaults;
  }

  /**
   * Decodes the read-bridge behavior the server stamped into {@code config}, returning {@link
   * #INERT} when there is nothing to bridge.
   *
   * @throws IllegalStateException if an entry this client owns is malformed (encoder bug or
   *     transport corruption); unknown keys are ignored.
   */
  static ReadBridge from(Map<String, String> config) {
    Map<Integer, String> columnDefaults = decodeColumnDefaults(config);
    return columnDefaults.isEmpty() ? INERT : new ReadBridge(columnDefaults);
  }

  /**
   * Applies the bridged read-time behavior onto {@code raw}, returning the transformed metadata (or
   * {@code raw} when there is nothing to bridge).
   */
  TableMetadata apply(TableMetadata raw) {
    if (columnDefaults.isEmpty()) {
      return raw;
    }
    // TODO(read-bridge): overlay columnDefaults onto raw.schemas() via withSchemaOverlay; future V3
    // features bridged from config are applied here too. Two failure categories apply there, as
    // here: a capability gap we don't yet support degrades to NULL, while an invariant violation
    // (e.g. a default that can't bind to its column) fails loud.
    return raw;
  }

  /**
   * Strip {@code initial-default} on field-ids this bridge stamped. Name, type, nullability, doc,
   * order, write-default, and unstamped ids stay on {@code metadata}.
   */
  TableMetadata sanitize(TableMetadata metadata) {
    if (columnDefaults.isEmpty() || metadata == null) {
      return metadata;
    }
    ObjectNode root = metadataJson(metadata);
    boolean changed = false;
    for (JsonNode field : fieldObjects(root)) {
      if (columnDefaults.containsKey(field.get(ID).asInt())
          && ((ObjectNode) field).remove(INITIAL_DEFAULT) != null) {
        changed = true;
      }
    }
    return changed ? fromMetadataJson(metadata, root) : metadata;
  }

  /** The decoded {@code field-id -> initial-default} entries. Package-visible for testing. */
  Map<Integer, String> columnDefaults() {
    return columnDefaults;
  }

  /**
   * Decodes {@code field-id -> initial-default} from the {@code
   * openhouse.read-bridge.column-default.*} config entries; empty when there are none. On a known
   * entry, the server encoder guarantees an integer field-id and a value that round-trips through
   * {@code readTree}, so a non-integer field-id or an unparseable value is an encoder bug or
   * transport corruption — it throws rather than degrading. Unknown keys are ignored above.
   */
  private static Map<Integer, String> decodeColumnDefaults(Map<String, String> config) {
    if (config == null) {
      return Collections.emptyMap();
    }
    Map<Integer, String> byFieldId = new HashMap<>();
    for (Map.Entry<String, String> entry : config.entrySet()) {
      if (!entry.getKey().startsWith(COLUMN_DEFAULT_PREFIX)) {
        continue;
      }
      try {
        int fieldId = Integer.parseInt(entry.getKey().substring(COLUMN_DEFAULT_PREFIX.length()));
        // Validate JSON; keep the original string so apply can bind without a relocated JsonNode.
        MAPPER.readTree(entry.getValue());
        byFieldId.put(fieldId, entry.getValue());
      } catch (RuntimeException | JsonProcessingException e) {
        // The server encoder stamps an int field-id and a JsonNode value that round-trips through
        // readTree, so reaching here means an encoder bug or transport corruption, not an expected
        // state. Fail loud so it is caught, rather than silently reading NULL.
        throw new IllegalStateException(
            "read-bridge: unusable "
                + COLUMN_DEFAULT_PREFIX
                + " entry "
                + entry.getKey()
                + "="
                + entry.getValue(),
            e);
      }
    }
    return byFieldId;
  }

  /** Schema field objects are the JSON nodes that carry {@code id}. */
  private static List<JsonNode> fieldObjects(ObjectNode metadata) {
    JsonNode schemas = metadata.get(SCHEMAS);
    if (schemas == null || !schemas.isArray()) {
      throw new IllegalStateException(
          "read-bridge: metadata JSON missing required '" + SCHEMAS + "' array");
    }
    List<JsonNode> fields = new ArrayList<>();
    for (JsonNode schema : schemas) {
      List<JsonNode> found = schema.findParents(ID);
      if (found != null) {
        fields.addAll(found);
      }
    }
    return fields;
  }

  private static ObjectNode metadataJson(TableMetadata metadata) {
    JsonNode root = readTree(TableMetadataParser.toJson(metadata));
    if (root == null || !root.isObject()) {
      throw new IllegalStateException("read-bridge: table metadata JSON is not an object");
    }
    return (ObjectNode) root;
  }

  private static TableMetadata fromMetadataJson(TableMetadata metadata, ObjectNode root) {
    try {
      return TableMetadataParser.fromJson(
          metadata.metadataFileLocation(), MAPPER.writeValueAsString(root));
    } catch (IllegalStateException e) {
      throw e;
    } catch (RuntimeException | JsonProcessingException e) {
      throw new IllegalStateException("read-bridge: failed to rebuild table metadata schemas", e);
    }
  }

  private static JsonNode readTree(String json) {
    try {
      return MAPPER.readTree(json);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("read-bridge: invalid json", e);
    }
  }
}
