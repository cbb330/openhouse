# Iceberg REST Catalog Implementation Plan (Subset, Contract-First)

## Objective

Add Iceberg REST Catalog support directly in OpenHouse so standard clients can work without custom adapters.

Phase 1 is intentionally read-only and targets immediate compatibility for:

- PyIceberg `load_table`
- PyIceberg `list_tables`
- PyIceberg `table_exists`
- Engines that use the same Iceberg REST read paths (DuckDB, Trino, Spark)

## Implementation (Current)

The phase-1 contract-first wiring is implemented as:

- Upstream spec (codegen source):
  - `spec/iceberg-rest-catalog-open-api.yaml` — full upstream Iceberg REST OpenAPI spec (1.10)
  - Used by codegen to generate `CatalogApiApi` and `ConfigurationApiApi` interfaces
  - Unimplemented endpoints inherit 501 (Not Implemented) defaults from generated interfaces
- Subset spec (documentation):
  - `spec/openhouse-iceberg-rest-readonly-v1.yaml` — documents the read-only subset OpenHouse actually implements
- Build/codegen/lint gate in `services/tables/build.gradle`:
  - `setUpOpenApiCliForIcebergRest` — downloads openapi-generator-cli
  - `validateIcebergRestOpenApiSpec` — validates spec syntax
  - `generateIcebergRestOpenApiServer` — generates Spring interfaces with Iceberg type mappings
  - `compileJava` depends on generated interfaces
  - `check` depends on OpenAPI validation
  - Post-processing regex replaces Iceberg 1.10-only `@RequestBody` types with `Object` for 1.5.2 compatibility
- Type mappings (Polaris pattern):
  - `importMappings` and `typeMappings` map spec schema names to real Iceberg library types
  - No model classes are generated (`models=false`); interfaces reference existing Iceberg request/response types
- Controller contract implementation:
  - `services/tables/src/main/java/com/linkedin/openhouse/tables/controller/IcebergRestCatalogController.java`
  - Implements generated `CatalogApiApi` and `ConfigurationApiApi` interfaces
  - Overrides only `getConfig`, `listTables`, `loadTable`, and `tableExists`
  - Routes through `TablesService` for auth and `OpenHouseInternalCatalog` for table loading
- Serialization:
  - `IcebergRestHttpMessageConverter` — custom `AbstractHttpMessageConverter<RESTResponse>` using Iceberg's `RESTSerializers` (kebab-case JSON)
  - `IcebergRestSerdeConfig` — registers the converter via `WebMvcConfigurer.extendMessageConverters`
  - `IcebergRestSerde` — central ObjectMapper configuration with Iceberg serializer modules
- Exception handling:
  - `IcebergRestExceptionHandler` — scoped `@RestControllerAdvice` returning Iceberg `ErrorResponse` format
- Runtime compatibility coverage:
  - Round-trip integration tests (21 tests) using real Iceberg `RESTCatalog` client:
    `services/tables/src/test/java/com/linkedin/openhouse/tables/e2e/h2/IcebergRestCatalogRoundTripTest.java`
  - MockMvc unit tests (6 tests):
    `services/tables/src/test/java/com/linkedin/openhouse/tables/mock/controller/IcebergRestCatalogControllerTest.java`
  - PyIceberg smoke test:
    `integrations/python/dataloader/scripts/iceberg_rest_catalog_smoke.py`
- CI automation:
  - `.github/workflows/build-run-tests.yml` runs:
    - `./gradlew clean build :services:tables:check`
    - PyIceberg smoke test script

Justification for each component:

- Full upstream spec as codegen source:
  - Generates interfaces for all Iceberg REST operations, giving free 501 defaults for unimplemented endpoints.
  - Avoids maintaining a hand-curated subset spec for codegen (the subset spec is documentation only).
  - When write endpoints are added in phase 2+, only the controller needs updating — no spec changes required.
- Generated interfaces + Iceberg type mappings:
  - Compile-time enforcement that route/method/signature drift fails build.
  - Reuses real Iceberg types (no generated model classes) following the Polaris pattern.
- `check` dependency on spec validation:
  - Ensures OpenAPI syntax/contract checks are in the standard CI gate.
- Custom message converter:
  - Iceberg REST uses kebab-case JSON with custom serializers that differ from OpenHouse's Jackson config.
  - Separate converter avoids interfering with existing OpenHouse API serialization.
- Runtime smoke tests:
  - Validates wire behavior that compile-time checks cannot prove (`HEAD`, status codes, client interoperability).

## Why This Plan

OpenHouse already has partial read-only Iceberg REST endpoints in:

- `services/tables/src/main/java/com/linkedin/openhouse/tables/controller/IcebergRestCatalogController.java`

But current contract enforcement is weak because:

- OpenAPI docs in `docs/specs/*.md` are generated documentation, not canonical source.
- Controllers are handwritten and not required to implement generated API interfaces.
- CI does not have a contract-specific gate for Iceberg REST behavior.

This plan moves to a contract-first model similar to Polaris:

1. Canonical OpenAPI in repo
2. Generated server API interfaces and models
3. Compile and runtime compatibility gates in CI

## Phase 1 Scope (Read-Only Only)

Implement and enforce only these Iceberg REST operations:

- `GET /v1/config`
- `GET /v1/namespaces/{namespace}/tables`
- `GET /v1/namespaces/{namespace}/tables/{table}`
- `HEAD /v1/namespaces/{namespace}/tables/{table}`

Reason for `HEAD`: stock PyIceberg uses it for `table_exists`.

## Phase 1 Non-Goals

Do not implement these in phase 1:

- `create_table`
- `drop_table`
- `purge_table`
- `rename_table`
- `commit_table`
- `create_table_transaction`
- `register_table`
- views APIs (`drop_view`, `list_views`, `view_exists`)
- namespace mutation APIs (`list_namespaces`, `create_namespace`, `drop_namespace`, `update_namespace_properties`, `load_namespace_properties`)

For unsupported Iceberg REST operations, return `501 Not Implemented` with Iceberg-style error payload.

## Contract Strategy

### 1) Spec as source of truth

- Upstream spec (codegen source): `spec/iceberg-rest-catalog-open-api.yaml`
  - Full Iceberg REST OpenAPI spec vendored from upstream (Apache Iceberg 1.10).
  - Used by the openapi-generator to produce Spring interfaces.
  - Unimplemented operations get auto-generated 501 defaults.
- Subset spec (documentation): `spec/openhouse-iceberg-rest-readonly-v1.yaml`
  - Documents which endpoints OpenHouse actually implements in phase 1.
  - Not used by codegen — purely for human reference and PR review.
- Keep `docs/specs/*.md` as generated docs only.

Justification:

- Using the full upstream spec avoids maintaining a curated subset for codegen and gives free 501 defaults.
- Contract diffs are visible in PRs.
- Builds are reproducible and do not depend on remote spec availability.
- Branches/tags remain self-contained.

### 2) Server implements generated API interfaces

- Generate Spring server interfaces from the upstream spec during build (no model generation).
- Map spec schema names to real Iceberg library types via `importMappings`/`typeMappings` (Polaris pattern).
- Controller implements generated `CatalogApiApi` and `ConfigurationApiApi`, overriding only read-only methods.
- Keep business logic in existing handlers/services.

Justification:

- Compile-time contract enforcement for endpoint signatures, params, and DTO wiring.
- Reusing real Iceberg types avoids generated model classes and ensures wire-format compatibility.
- Clear separation of concerns:
  - generated API surface = contract
  - handwritten handlers/services = business logic

## CI Enforcement Strategy

### 3) `./gradlew check` as single contract gate

Wire `check` to include:

- OpenAPI generation task
- Java compile task for generated-interface implementation
- OpenAPI validation/lint task

Justification:

- `compileJava` alone only verifies shape/signature compatibility.
- Runtime HTTP behavior is not statically provable.
- `check` becomes the static contract gate, while runtime client compatibility remains a CI workflow gate.

## Why Both Static and Runtime Checks Are Required

### Static (compile-time) checks catch

- Missing interface implementation
- Signature mismatches
- Model type mismatches
- Route method contract mismatches at interface level

### Runtime checks catch

- HTTP status code behavior
- Required headers
- Error payload shape/content
- Serialized JSON field names on the wire
- `HEAD` behavior (`200/204/404` and empty response body semantics)
- Real client interoperability (PyIceberg against live service)

## Implementation Breakdown

### A) Spec and generation

1. Vendor upstream Iceberg REST OpenAPI spec:
   - `spec/iceberg-rest-catalog-open-api.yaml` (full spec, codegen source)
2. Add read-only subset spec for documentation:
   - `spec/openhouse-iceberg-rest-readonly-v1.yaml` (not used by codegen)
3. Add Gradle codegen tasks in `services/tables/build.gradle`:
   - `setUpOpenApiCliForIcebergRest` — downloads openapi-generator-cli
   - `validateIcebergRestOpenApiSpec` — validates spec syntax
   - `generateIcebergRestOpenApiServer` — generates Spring interfaces with Iceberg type mappings
   - Post-processing regex replaces Iceberg 1.10-only `@RequestBody` types with `Object` for 1.5.2 compatibility
4. Add generated sources to `compileJava`, ensure `compileJava` depends on codegen task

### B) Controller alignment

1. `IcebergRestCatalogController` implements generated `CatalogApiApi` and `ConfigurationApiApi`
2. Overrides only: `getConfig`, `listTables`, `loadTable`, `tableExists`
3. Delegates to:
   - `TablesService` for auth-aware table listing and access checks
   - `OpenHouseInternalCatalog` + `CatalogHandlers.loadTable` for table metadata
   - `TablesApiValidator` for input validation
4. `IcebergRestExceptionHandler` — scoped `@RestControllerAdvice` returning Iceberg `ErrorResponse` format
5. `IcebergRestHttpMessageConverter` + `IcebergRestSerdeConfig` — custom serde using Iceberg's `RESTSerializers`

### C) Test coverage

1. Round-trip integration tests (21 tests) using real Iceberg `RESTCatalog` client:
   - `IcebergRestCatalogRoundTripTest` — list tables, load table schema/partition/metadata, HEAD exists/not-found, error handling, cross-namespace isolation
2. MockMvc unit tests (6 tests):
   - `IcebergRestCatalogControllerTest` — config, list, load, load not found, head exists, head not found
3. Python compatibility smoke test using stock PyIceberg `RestCatalog`:
   - verifies `table_exists`, `list_tables`, `load_table`

### D) CI automation

1. `.github/workflows/build-run-tests.yml` runs:
   - `./gradlew clean build :services:tables:check`
   - PyIceberg smoke test script

## Deliverables

- Upstream Iceberg REST spec vendored in-repo (codegen source)
- Read-only subset spec for documentation
- Generated server API interfaces (no models) with Iceberg type mappings
- Controller implementing `CatalogApiApi` + `ConfigurationApiApi` (read-only overrides only)
- Custom serde for Iceberg REST wire format (kebab-case JSON via `RESTSerializers`)
- Scoped exception handler returning Iceberg `ErrorResponse` format
- `HEAD` support for table existence
- OpenAPI validation and codegen wired into `check`
- Round-trip integration tests (21) + MockMvc unit tests (6) + PyIceberg smoke test
- CI gate proving compatibility

## Acceptance Criteria

- `./gradlew :services:tables:check` fails on contract drift.
- `./gradlew check` in CI enforces spec + compile + OpenAPI validation.
- CI workflow enforces runtime compatibility via PyIceberg smoke test.
- Stock PyIceberg can perform:
  - `load_table`
  - `list_tables`
  - `table_exists`
  without custom OpenHouse adapter logic.

## Follow-Up (Phase 2+)

After phase 1 stabilizes, add write paths incrementally:

- table creation/update/drop/rename/commit
- namespace APIs
- optional view APIs

Each addition must update canonical spec first, then generated interfaces, then controller/handler implementations, then compatibility tests.
