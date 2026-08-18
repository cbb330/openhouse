package com.linkedin.openhouse.javaclient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.jupiter.api.Test;

class BranchOverlayTest {

  private static final String BRANCH = "ci";

  @Test
  void projectWithoutBranchOrOverlayReturnsSameInstance() {
    TableMetadata durable = tableWith(mainSchema(), Collections.emptyMap());
    assertSame(durable, BranchOverlay.project(durable, null));
    assertSame(durable, BranchOverlay.project(durable, BRANCH));
  }

  @Test
  void projectAppliesSchemaAndUserPropertiesFromBag() {
    Schema evolved = evolvedSchema();
    BranchOverlay.Bag bag = new BranchOverlay.Bag();
    bag.schema = SchemaParser.toJson(evolved, false);
    bag.properties = Collections.singletonMap("gfd.marker", "branched");
    TableMetadata durable = tableWith(mainSchema(), Collections.emptyMap());

    TableMetadata projected = BranchOverlay.project(durable, BRANCH, bag);

    assertTrue(projected.schema().sameSchema(evolved));
    assertEquals("branched", projected.properties().get("gfd.marker"));
    assertFalse(projected.schema().sameSchema(durable.schema()));
    assertEquals(durable.metadataFileLocation(), projected.metadataFileLocation());
  }

  @Test
  void projectStillReadsLeftoverPropertyKeysForMigration() {
    Schema evolved = evolvedSchema();
    Map<String, String> stored = new HashMap<String, String>();
    stored.put(BranchOverlay.schemaKey(BRANCH), SchemaParser.toJson(evolved, false));
    stored.put(BranchOverlay.propertiesKey(BRANCH), "{\"gfd.marker\":\"branched\"}");
    TableMetadata durable = tableWith(mainSchema(), stored);

    TableMetadata projected = BranchOverlay.project(durable, BRANCH);

    assertTrue(projected.schema().sameSchema(evolved));
    assertEquals("branched", projected.properties().get("gfd.marker"));
    assertFalse(projected.properties().keySet().stream().anyMatch(BranchOverlay::isReservedKey));
  }

  @Test
  void sanitizeDoesNotWriteReservedKeys() {
    TableMetadata durable = tableWith(mainSchema(), Collections.emptyMap());
    Map<String, String> projectedProps = new HashMap<String, String>(durable.properties());
    projectedProps.put("gfd.marker", "branched");
    projectedProps.put("avro.schema.literal", "{\"type\":\"record\"}");
    TableMetadata projected =
        TableMetadata.buildFrom(durable)
            .setCurrentSchema(evolvedSchema(), 3)
            .setProperties(projectedProps)
            .build();

    BranchOverlay.Bag bag = BranchOverlay.capture(durable, projected, BRANCH);
    TableMetadata sanitized = BranchOverlay.sanitize(durable, projected, BRANCH);

    assertTrue(sanitized.schema().sameSchema(durable.schema()));
    assertFalse(sanitized.properties().keySet().stream().anyMatch(BranchOverlay::isReservedKey));
    assertTrue(SchemaParser.fromJson(bag.schema).sameSchema(evolvedSchema()));
    assertEquals("branched", bag.properties.get("gfd.marker"));
    assertTrue(bag.properties.containsKey("avro.schema.literal"));
    assertFalse(sanitized.properties().containsKey("gfd.marker"));
  }

  @Test
  void sanitizeRoundTripProjectSeesOverlayNotMain() {
    TableMetadata durable = tableWith(mainSchema(), Collections.emptyMap());
    Map<String, String> projectedProps = new HashMap<String, String>(durable.properties());
    projectedProps.put("gfd.marker", "branched");
    TableMetadata projected =
        TableMetadata.buildFrom(durable)
            .setCurrentSchema(evolvedSchema(), 3)
            .setProperties(projectedProps)
            .build();

    BranchOverlay.Bag bag = BranchOverlay.capture(durable, projected, BRANCH);
    TableMetadata persisted = BranchOverlay.sanitize(durable, projected, BRANCH);
    TableMetadata mainView = BranchOverlay.project(persisted, null);
    TableMetadata branchView = BranchOverlay.project(persisted, BRANCH, bag);

    assertTrue(mainView.schema().sameSchema(mainSchema()));
    assertFalse(mainView.properties().containsKey("gfd.marker"));
    assertFalse(
        mainView.properties().keySet().stream().anyMatch(BranchOverlay::isReservedKey),
        "main view must not present reserved overlay keys");
    assertTrue(branchView.schema().sameSchema(evolvedSchema()));
    assertEquals("branched", branchView.properties().get("gfd.marker"));
    assertFalse(persisted.properties().keySet().stream().anyMatch(BranchOverlay::isReservedKey));
  }

  @Test
  void projectHidesReservedKeysWithoutBranch() {
    Map<String, String> stored = new HashMap<String, String>();
    stored.put(BranchOverlay.schemaKey(BRANCH), SchemaParser.toJson(evolvedSchema(), false));
    stored.put(BranchOverlay.propertiesKey(BRANCH), "{\"gfd.marker\":\"branched\"}");
    TableMetadata durable = tableWith(mainSchema(), stored);

    TableMetadata mainView = BranchOverlay.project(durable, null);

    assertTrue(mainView.schema().sameSchema(mainSchema()));
    assertFalse(mainView.properties().containsKey("gfd.marker"));
    assertFalse(mainView.properties().keySet().stream().anyMatch(BranchOverlay::isReservedKey));
  }

  @Test
  void sanitizeOverlaysPolicyAndDoesNotRefuse() {
    TableMetadata durable = tableWith(mainSchema(), Collections.emptyMap());
    Map<String, String> projectedProps = new HashMap<String, String>(durable.properties());
    String policy = "{\"retention\":{\"count\":180,\"granularity\":\"DAY\"}}";
    projectedProps.put("updated.openhouse.policy", policy);
    projectedProps.put("policies", policy);
    TableMetadata projected =
        TableMetadata.buildFrom(durable).setProperties(projectedProps).build();

    BranchOverlay.Bag bag = BranchOverlay.capture(durable, projected, BRANCH);
    TableMetadata sanitized = BranchOverlay.sanitize(durable, projected, BRANCH);
    TableMetadata branchView = BranchOverlay.project(sanitized, BRANCH, bag);
    TableMetadata mainView = BranchOverlay.project(sanitized, null);

    assertEquals(policy, branchView.properties().get("updated.openhouse.policy"));
    assertEquals(policy, branchView.properties().get("policies"));
    assertFalse(mainView.properties().containsKey("updated.openhouse.policy"));
    assertFalse(mainView.properties().containsKey("policies"));
    assertFalse(sanitized.properties().containsKey("updated.openhouse.policy"));
    assertFalse(sanitized.properties().keySet().stream().anyMatch(BranchOverlay::isReservedKey));
    assertTrue(bag.properties.containsKey("updated.openhouse.policy"));
  }

  @Test
  void sanitizeIgnoresLocationChange() {
    TableMetadata durable = tableWith(mainSchema(), Collections.emptyMap());
    Map<String, String> projectedProps = new HashMap<String, String>(durable.properties());
    projectedProps.put("location", "/tmp/nope");
    TableMetadata projected =
        TableMetadata.buildFrom(durable).setProperties(projectedProps).build();

    BranchOverlay.Bag bag = BranchOverlay.capture(durable, projected, BRANCH);
    TableMetadata sanitized = BranchOverlay.sanitize(durable, projected, BRANCH);

    assertEquals(durable.location(), sanitized.location());
    assertFalse(
        sanitized.properties().containsKey("location")
            && "/tmp/nope".equals(sanitized.properties().get("location")));
    assertFalse(bag.properties.containsKey("location"));
  }

  @Test
  void sanitizeStripsLeftoverReservedKeys() {
    Map<String, String> stored = new HashMap<String, String>();
    stored.put(BranchOverlay.schemaKey(BRANCH), SchemaParser.toJson(evolvedSchema(), false));
    TableMetadata durable = tableWith(mainSchema(), stored);
    TableMetadata projected = TableMetadata.buildFrom(durable).build();

    TableMetadata sanitized = BranchOverlay.sanitize(durable, projected, BRANCH);

    assertFalse(sanitized.properties().keySet().stream().anyMatch(BranchOverlay::isReservedKey));
  }

  @Test
  void sparkSessionClassNameIsNotRelocated() {
    assertEquals("org.apache.spark.sql.SparkSession", SessionWapBranch.sparkSessionClassName());
  }

  private static Schema mainSchema() {
    return new Schema(
        NestedField.optional(1, "id", Types.IntegerType.get()),
        NestedField.optional(2, "data", Types.StringType.get()));
  }

  private static Schema evolvedSchema() {
    return new Schema(
        NestedField.optional(1, "id", Types.IntegerType.get()),
        NestedField.optional(2, "data", Types.StringType.get()),
        NestedField.optional(3, "extra", Types.StringType.get()));
  }

  private static TableMetadata tableWith(Schema schema, Map<String, String> properties) {
    TableMetadata created =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), "file:/tmp/gfd-overlay", properties);
    return TableMetadataParser.fromJson(
        "file:/tmp/gfd-overlay/metadata.json", TableMetadataParser.toJson(created));
  }
}
