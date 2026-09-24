package com.linkedin.openhouse.tables.readbridge;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.readbridge.ColumnDefaultException.Origin;
import com.linkedin.openhouse.tables.readbridge.ColumnDefaultException.Reason;
import com.linkedin.openhouse.tables.toggle.TableFeatureToggle;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;

public class ReadBridgeConfigResolverTest {

  private static final String PREFIX = ReadBridgeConfigResolver.COLUMN_DEFAULT_PREFIX;

  /** A toggle that ramps everything, so a test isolates the encoder rather than the ramp. */
  private static final TableFeatureToggle ALL_ON =
      new TableFeatureToggle() {
        @Override
        public boolean isFeatureActivated(String databaseId, String tableId, String featureId) {
          return true;
        }
      };

  private static ReadBridgeConfigResolver resolverFor(ColumnDefaultsSource source) {
    return new ReadBridgeConfigResolver(source, ALL_ON);
  }

  private static ColumnDefaultsSource oneDefault() {
    return tableDto -> Collections.singletonMap(5, TextNode.valueOf("US"));
  }

  /** A table carrying an explicit self-service opt-in/opt-out property. */
  private static TableDto tableWithOverride(String value) {
    return TableDto.builder()
        .databaseId("db")
        .tableId("tbl")
        .tableProperties(
            Collections.singletonMap(
                ReadBridgeConfigResolver.COLUMN_DEFAULT_FEATURE_ID
                    + TableFeatureToggle.ENABLED_PROPERTY_SUFFIX,
                value))
        .build();
  }

  /** Gate 1: no deployment-supplied source => inert, and crucially no toggle lookup at all. */
  @Test
  public void testInertAndSkipsToggleWhenNoSourceSupplied() throws ColumnDefaultException {
    TableFeatureToggle toggle = mock(TableFeatureToggle.class);
    ReadBridgeConfigResolver resolver =
        new ReadBridgeConfigResolver(ColumnDefaultsSource.NONE, toggle);

    Assertions.assertTrue(resolver.resolve(mock(TableDto.class)).isEmpty());
    // The toggle is a remote HouseTables call on the table-load path; it must not be made.
    verifyNoInteractions(toggle);
  }

  @Test
  public void testToggleLookupFailureFailsTheRead() {
    TableFeatureToggle unavailable =
        (databaseId, tableId, featureId) -> {
          throw WebClientResponseException.create(
              503, "Unavailable", HttpHeaders.EMPTY, new byte[0], StandardCharsets.UTF_8);
        };

    ColumnDefaultException thrown =
        Assertions.assertThrows(
            ColumnDefaultException.class,
            () ->
                new ReadBridgeConfigResolver(oneDefault(), unavailable)
                    .resolve(TableDto.builder().databaseId("db").tableId("tbl").build()));

    Assertions.assertEquals(Reason.UNAVAILABLE, thrown.getReason());
    Assertions.assertEquals(Origin.STORED, thrown.getOrigin());
  }

  @Test
  public void testSourceFailureFailsTheRead() {
    ColumnDefaultsSource exploding =
        tableDto -> {
          throw new IllegalStateException("encoder exploded");
        };

    ColumnDefaultException thrown =
        Assertions.assertThrows(
            ColumnDefaultException.class,
            () ->
                resolverFor(exploding)
                    .resolve(TableDto.builder().databaseId("db").tableId("tbl").build()));

    Assertions.assertEquals(Reason.INTERNAL, thrown.getReason());
    Assertions.assertEquals(Origin.STORED, thrown.getOrigin());
  }

  /** Write path must not commit when the source cannot answer. */
  @Test
  public void testUnexpectedSourceFailureIsInternalNotInvalidInput() {
    ColumnDefaultsSource exploding =
        tableDto -> {
          throw new IllegalStateException("encoder exploded");
        };

    ColumnDefaultException thrown =
        Assertions.assertThrows(
            ColumnDefaultException.class,
            () ->
                resolverFor(exploding)
                    .stampedColumnDefaults(
                        TableDto.builder().databaseId("db").tableId("tbl").build()));
    Assertions.assertEquals(Reason.INTERNAL, thrown.getReason());
  }

  @Test
  public void testDeclaredSourceFailureRejectsReadsAndWrites() {
    TableDto table = TableDto.builder().databaseId("db").tableId("tbl").build();
    ColumnDefaultsSource invalid =
        input -> {
          throw new ColumnDefaultException(Reason.INVALID_VALUE, input, null);
        };
    ReadBridgeConfigResolver resolver = resolverFor(invalid);
    ColumnDefaultException readFailure =
        Assertions.assertThrows(ColumnDefaultException.class, () -> resolver.resolve(table));
    Assertions.assertEquals(Reason.INVALID_VALUE, readFailure.getReason());
    Assertions.assertEquals(Origin.STORED, readFailure.getOrigin());
    ColumnDefaultException thrown =
        Assertions.assertThrows(
            ColumnDefaultException.class, () -> resolver.stampedColumnDefaults(table));
    Assertions.assertEquals(Reason.INVALID_VALUE, thrown.getReason());
  }

  @Test
  public void testInvalidSourceEntryCannotProducePartialDefaults() {
    ColumnDefaultsSource invalid =
        table -> {
          Map<Integer, JsonNode> defaults = new LinkedHashMap<>();
          defaults.put(5, TextNode.valueOf("US"));
          defaults.put(7, null);
          return defaults;
        };
    TableDto table = TableDto.builder().databaseId("db").tableId("tbl").build();
    ReadBridgeConfigResolver resolver = resolverFor(invalid);
    Assertions.assertEquals(
        Reason.INTERNAL,
        Assertions.assertThrows(ColumnDefaultException.class, () -> resolver.resolve(table))
            .getReason());
    Assertions.assertEquals(
        Reason.INTERNAL,
        Assertions.assertThrows(
                ColumnDefaultException.class, () -> resolver.stampedColumnDefaults(table))
            .getReason());
  }

  @Test
  public void testConnectionFailureIsUnavailable() {
    TableFeatureToggle unavailable =
        (databaseId, tableId, featureId) -> {
          throw new WebClientRequestException(
              new IOException("connection refused"),
              HttpMethod.GET,
              URI.create("https://housetables.invalid/"),
              HttpHeaders.EMPTY);
        };
    ColumnDefaultException thrown =
        Assertions.assertThrows(
            ColumnDefaultException.class,
            () ->
                new ReadBridgeConfigResolver(oneDefault(), unavailable)
                    .stampedColumnDefaults(
                        TableDto.builder().databaseId("db").tableId("tbl").build()));
    Assertions.assertEquals(Reason.UNAVAILABLE, thrown.getReason());
  }

  @Test
  public void testUpstreamPermissionFailureIsInternalNotRetryableOutage() {
    TableFeatureToggle denied =
        (databaseId, tableId, featureId) -> {
          throw WebClientResponseException.create(
              403, "Forbidden", HttpHeaders.EMPTY, new byte[0], StandardCharsets.UTF_8);
        };
    ColumnDefaultException thrown =
        Assertions.assertThrows(
            ColumnDefaultException.class,
            () ->
                new ReadBridgeConfigResolver(oneDefault(), denied)
                    .stampedColumnDefaults(
                        TableDto.builder().databaseId("db").tableId("tbl").build()));
    Assertions.assertEquals(Reason.INTERNAL, thrown.getReason());
  }

  /** Write path must not commit when the ramp lookup cannot answer. */
  @Test
  public void testWritePathToggleFailureFailsClosed() {
    TableFeatureToggle exploding =
        new TableFeatureToggle() {
          @Override
          public boolean isFeatureActivated(String databaseId, String tableId, String featureId) {
            throw WebClientResponseException.create(
                503, "Unavailable", HttpHeaders.EMPTY, new byte[0], StandardCharsets.UTF_8);
          }
        };

    ColumnDefaultException thrown =
        Assertions.assertThrows(
            ColumnDefaultException.class,
            () ->
                new ReadBridgeConfigResolver(oneDefault(), exploding)
                    .stampedColumnDefaults(
                        TableDto.builder().databaseId("db").tableId("tbl").build()));
    Assertions.assertEquals(Reason.UNAVAILABLE, thrown.getReason());
  }

  /** Gate 3: a table the ramp has not activated is not bridged, and its source is never asked. */
  @Test
  public void testUnrampedTableIsNotBridgedAndSourceNotConsulted() throws ColumnDefaultException {
    ColumnDefaultsSource source = mock(ColumnDefaultsSource.class);
    TableFeatureToggle allOff =
        new TableFeatureToggle() {
          @Override
          public boolean isFeatureActivated(String databaseId, String tableId, String featureId) {
            return false;
          }
        };

    Assertions.assertTrue(
        new ReadBridgeConfigResolver(source, allOff)
            .resolve(TableDto.builder().databaseId("db").tableId("tbl").build())
            .isEmpty());
    // Deriving defaults can be expensive (a deployment may parse a schema); gate first.
    verifyNoInteractions(source);
  }

  /** The self-service property opts a table in even when the server-managed ramp says no. */
  @Test
  public void testTablePropertyOptsInOverServerToggle() throws ColumnDefaultException {
    // CALLS_REAL_METHODS so the override-honoring default reads the table property; stub the
    // server-side form so an accidental HTS call would return false.
    TableFeatureToggle toggle = mock(TableFeatureToggle.class, CALLS_REAL_METHODS);
    when(toggle.isFeatureActivated(anyString(), anyString(), anyString())).thenReturn(false);

    Map<String, String> config =
        new ReadBridgeConfigResolver(oneDefault(), toggle).resolve(tableWithOverride("true"));

    Assertions.assertEquals("\"US\"", config.get(PREFIX + "5"));
    // Explicit opt-in is decided from the table property alone; no HouseTables round-trip.
    verify(toggle, never()).isFeatureActivated(anyString(), anyString(), anyString());
  }

  /** ...and opts it out even when the server-managed ramp says yes. */
  @Test
  public void testTablePropertyOptsOutOverServerToggle() throws ColumnDefaultException {
    Assertions.assertTrue(resolverFor(oneDefault()).resolve(tableWithOverride("false")).isEmpty());
  }

  /**
   * Source present and table ramped, but the source has nothing to stamp — still empty config. Not
   * the same as {@link ColumnDefaultsSource#NONE}: the toggle ran and the source was asked.
   */
  @Test
  public void testEmptyWhenSourceReturnsNoDefaults() throws ColumnDefaultException {
    ColumnDefaultsSource emptySource = mock(ColumnDefaultsSource.class);
    when(emptySource.defaults(any())).thenReturn(Collections.emptyMap());

    Assertions.assertTrue(
        resolverFor(emptySource)
            .resolve(TableDto.builder().databaseId("db").tableId("tbl").build())
            .isEmpty());
    verify(emptySource).defaults(any());
  }

  /**
   * The capability's feature id, its self-service property and its wire keys are one token. Pinned
   * as literals because all three are external contracts: the id is stored in HouseTables toggle
   * rules, the property is set on customer tables, and the prefix is mirrored by the client
   * decoder. Deriving them from each other keeps them consistent; asserting the literals keeps a
   * refactor from silently renaming all three at once.
   */
  @Test
  public void testFeatureIdPropertyAndKeysAreOneToken() {
    Assertions.assertEquals(
        "read-bridge.column-default", ReadBridgeConfigResolver.COLUMN_DEFAULT_FEATURE_ID);
    Assertions.assertEquals(
        "read-bridge.column-default.enabled",
        ReadBridgeConfigResolver.COLUMN_DEFAULT_FEATURE_ID
            + TableFeatureToggle.ENABLED_PROPERTY_SUFFIX);
    Assertions.assertEquals(
        "openhouse.read-bridge.column-default.", ReadBridgeConfigResolver.COLUMN_DEFAULT_PREFIX);
  }

  @Test
  public void testStampsColumnDefaultEntry() throws ColumnDefaultException {
    ColumnDefaultsSource source = tableDto -> Collections.singletonMap(5, TextNode.valueOf("US"));
    Map<String, String> config = resolverFor(source).resolve(mock(TableDto.class));
    // value is the single-value JSON for the default ("US" -> "\"US\"").
    Assertions.assertEquals("\"US\"", config.get(PREFIX + "5"));
  }

  @Test
  public void testStampsAllColumnDefaultsAsSeparateEntries() throws ColumnDefaultException {
    ColumnDefaultsSource source =
        tableDto -> {
          Map<Integer, JsonNode> defaults = new LinkedHashMap<>();
          defaults.put(5, TextNode.valueOf("US"));
          defaults.put(7, IntNode.valueOf(0));
          return defaults;
        };
    Map<String, String> config = resolverFor(source).resolve(mock(TableDto.class));
    Assertions.assertEquals(2, config.size());
    Assertions.assertEquals("\"US\"", config.get(PREFIX + "5"));
    Assertions.assertEquals("0", config.get(PREFIX + "7"));
  }
}
