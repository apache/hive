/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.rest;

import static org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivObjectActionType;
import static org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import static org.apache.iceberg.hive.HiveCatalog.HMS_DB_OWNER;
import static org.apache.iceberg.hive.HiveCatalog.HMS_DB_OWNER_TYPE;
import static org.apache.iceberg.hive.HiveCatalog.HMS_TABLE_OWNER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.credential.StorageOperation;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthorizer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.extension.MockHiveAuthorizer;
import org.apache.iceberg.rest.extension.MockHiveAuthorizerFactory;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.ImmutableRegisterTableRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class TestIcebergAuthorizer {
  private static final String CATALOG_NAME = "hive";
  private static final String DATABASE_NAME = "db";
  private static final Namespace NAMESPACE = Namespace.of(DATABASE_NAME);
  private static final String TABLE_NAME = "table";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, TABLE_NAME);
  private static final List<String> COLUMNS = List.of("id", "name");
  private static final String LOCATION = "file:/warehouse/db/table";
  private static final List<HivePrivilegeObject> WRITABLE_PRIVILEGES = List.of(
      new HivePrivilegeObject(
          HivePrivilegeObject.HivePrivilegeObjectType.TABLE_OR_VIEW,
          CATALOG_NAME,
          DATABASE_NAME,
          TABLE_NAME,
          null,
          null,
          HivePrivilegeObject.HivePrivObjectActionType.INSERT,
          null
      )
  );
  private static final List<HivePrivilegeObject> READABLE_PRIVILEGES = List.of(
      new HivePrivilegeObject(
          HivePrivilegeObject.HivePrivilegeObjectType.TABLE_OR_VIEW,
          CATALOG_NAME,
          DATABASE_NAME,
          TABLE_NAME,
          null,
          COLUMNS
      )
  );

  private static CreateTableRequest stageCreateRequest(String location, String tableOwnerName) {
    var builder = CreateTableRequest.builder()
        .withName(TABLE_NAME)
        .withLocation(location)
        .withSchema(new Schema())
        .stageCreate();
    if (tableOwnerName != null) {
      builder.setProperty(HMS_TABLE_OWNER, tableOwnerName);
    }
    return builder.build();
  }

  // HivePrivilegeObject does not have equals...
  private static void assertPrivilegeObjects(List<HivePrivilegeObject> expected, List<?> actual) {
    Assertions.assertEquals(expected.size(), actual.size());
    var actualObjects = new ArrayList<HivePrivilegeObject>(actual.size());
    actual.forEach(object -> actualObjects.add((HivePrivilegeObject) object));
    for (int index = 0; index < expected.size(); index++) {
      Assertions.assertEquals(0, expected.get(index).compareTo(actualObjects.get(index)));
    }
  }


  @Test
  void testConstructorWithPreEventListenerAndAuthorizer() {
    var conf = new Configuration(false);
    conf.set(MetastoreConf.ConfVars.PRE_EVENT_LISTENERS.getVarname(), HiveMetaStoreAuthorizer.class.getName());
    conf.set(MetastoreConf.ConfVars.HIVE_AUTHORIZATION_MANAGER.getVarname(), MockHiveAuthorizerFactory.class.getName());
    var icebergAuthorizer = new IcebergAuthorizer(conf);
    Assertions.assertEquals(MockHiveAuthorizer.class, icebergAuthorizer.authorizerSupplier.get().getClass());
  }

  @Test
  void testConstructorWithAdditionalPreEventListener() {
    var conf = new Configuration(false);
    conf.set(
        MetastoreConf.ConfVars.PRE_EVENT_LISTENERS.getVarname(),
        "org.apache.hadoop.hive.ql.security.authorization.AuthorizationPreEventListener,"
            + HiveMetaStoreAuthorizer.class.getName()
    );
    conf.set(MetastoreConf.ConfVars.HIVE_AUTHORIZATION_MANAGER.getVarname(), MockHiveAuthorizerFactory.class.getName());
    var icebergAuthorizer = new IcebergAuthorizer(conf);
    Assertions.assertEquals(MockHiveAuthorizer.class, icebergAuthorizer.authorizerSupplier.get().getClass());
  }

  @Test
  void testConstructorWithoutPreEventListener() {
    var conf = new Configuration(false);
    conf.set(MetastoreConf.ConfVars.HIVE_AUTHORIZATION_MANAGER.getVarname(), MockHiveAuthorizerFactory.class.getName());
    var icebergAuthorizer = new IcebergAuthorizer(conf);
    Assertions.assertNull(icebergAuthorizer.authorizerSupplier.get());
  }

  @Test
  void testConstructorWithIncompatiblePreEventListener() {
    var conf = new Configuration(false);
    conf.set(
        MetastoreConf.ConfVars.PRE_EVENT_LISTENERS.getVarname(),
        "org.apache.hadoop.hive.ql.security.authorization.AuthorizationPreEventListener"
    );
    var exception = Assertions.assertThrows(IllegalArgumentException.class, () -> new IcebergAuthorizer(conf));
    Assertions.assertEquals(
        "HiveMetaStoreAuthorizer is required when pre-event listeners are configured, " +
            "but [org.apache.hadoop.hive.ql.security.authorization.AuthorizationPreEventListener] is configured",
        exception.getMessage()
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  void testValidateStageCreateWithLocationAndNamespaceOwner() throws Exception {
    var hiveAuthorizer = mock(HiveAuthorizer.class);
    var icebergAuthorizer = new IcebergAuthorizer(() -> hiveAuthorizer);
    var databaseOwner = "database_owner";
    var namespaceMetadata = Map.of(
        HMS_DB_OWNER, databaseOwner,
        HMS_DB_OWNER_TYPE, PrincipalType.ROLE.name()
    );

    var tableOwner = "table_owner";
    icebergAuthorizer.validateStageCreateTable(
        CATALOG_NAME, NAMESPACE, namespaceMetadata, stageCreateRequest(LOCATION, tableOwner));

    var operation = ArgumentCaptor.forClass(HiveOperationType.class);
    var inputs = ArgumentCaptor.forClass(List.class);
    var outputs = ArgumentCaptor.forClass(List.class);
    var context = ArgumentCaptor.forClass(HiveAuthzContext.class);
    verify(hiveAuthorizer).checkPrivileges(operation.capture(), inputs.capture(), outputs.capture(), context.capture());

    Assertions.assertEquals(HiveOperationType.CREATETABLE, operation.getValue());

    Assertions.assertEquals(1, inputs.getValue().size());
    var location = (HivePrivilegeObject) inputs.getValue().getFirst();
    assertThat(location.getType()).isEqualTo(HivePrivilegeObjectType.DFS_URI);
    assertThat(location.getObjectName()).isEqualTo(LOCATION);
    assertThat(location.getActionType()).isEqualTo(HivePrivObjectActionType.OTHER);

    Assertions.assertEquals(2, outputs.getValue().size());
    var output1 = (HivePrivilegeObject) outputs.getValue().getFirst();
    Assertions.assertEquals(HivePrivilegeObjectType.DATABASE, output1.getType());
    Assertions.assertEquals(CATALOG_NAME, output1.getCatName());
    Assertions.assertEquals(NAMESPACE.level(0), output1.getDbname());
    Assertions.assertEquals(databaseOwner, output1.getOwnerName());
    Assertions.assertEquals(PrincipalType.ROLE, output1.getOwnerType());
    Assertions.assertEquals(HivePrivObjectActionType.OTHER, output1.getActionType());

    var output2 = (HivePrivilegeObject) outputs.getValue().getLast();
    Assertions.assertEquals(HivePrivilegeObjectType.TABLE_OR_VIEW, output2.getType());
    Assertions.assertEquals(CATALOG_NAME, output2.getCatName());
    Assertions.assertEquals(NAMESPACE.level(0), output2.getDbname());
    Assertions.assertEquals(TABLE_NAME, output2.getObjectName());
    Assertions.assertEquals(tableOwner, output2.getOwnerName());
    Assertions.assertEquals(PrincipalType.USER, output2.getOwnerType());
    Assertions.assertEquals(HivePrivObjectActionType.OTHER, output2.getActionType());

    Assertions.assertEquals("create table " + TABLE_NAME, context.getValue().getCommandString());
  }

  @Test
  @SuppressWarnings("unchecked")
  void testValidateStageCreateWithoutLocationOrNamespaceOwner() throws Exception {
    var hiveAuthorizer = mock(HiveAuthorizer.class);
    var icebergAuthorizer = new IcebergAuthorizer(() -> hiveAuthorizer);

    icebergAuthorizer.validateStageCreateTable(CATALOG_NAME, NAMESPACE, Map.of(), stageCreateRequest(null, null));

    var operation = ArgumentCaptor.forClass(HiveOperationType.class);
    var inputs = ArgumentCaptor.forClass(List.class);
    var outputs = ArgumentCaptor.forClass(List.class);
    var context = ArgumentCaptor.forClass(HiveAuthzContext.class);
    verify(hiveAuthorizer).checkPrivileges(operation.capture(), inputs.capture(), outputs.capture(), context.capture());

    Assertions.assertEquals(HiveOperationType.CREATETABLE, operation.getValue());

    Assertions.assertEquals(List.of(), inputs.getValue());

    Assertions.assertEquals(2, outputs.getValue().size());
    var output1 = (HivePrivilegeObject) outputs.getValue().getFirst();
    Assertions.assertEquals(HivePrivilegeObjectType.DATABASE, output1.getType());
    Assertions.assertEquals(CATALOG_NAME, output1.getCatName());
    Assertions.assertEquals(NAMESPACE.level(0), output1.getDbname());
    var expectedUserName = UserGroupInformation.getCurrentUser().getShortUserName();
    Assertions.assertEquals(expectedUserName, output1.getOwnerName());
    Assertions.assertEquals(PrincipalType.USER, output1.getOwnerType());
    Assertions.assertEquals(HivePrivObjectActionType.OTHER, output1.getActionType());

    var output2 = (HivePrivilegeObject) outputs.getValue().getLast();
    Assertions.assertEquals(HivePrivilegeObjectType.TABLE_OR_VIEW, output2.getType());
    Assertions.assertEquals(CATALOG_NAME, output2.getCatName());
    Assertions.assertEquals(NAMESPACE.level(0), output2.getDbname());
    Assertions.assertEquals(TABLE_NAME, output2.getObjectName());
    Assertions.assertEquals(expectedUserName, output2.getOwnerName());
    Assertions.assertEquals(PrincipalType.USER, output2.getOwnerType());
    Assertions.assertEquals(HivePrivObjectActionType.OTHER, output2.getActionType());

    Assertions.assertEquals("create table " + TABLE_NAME, context.getValue().getCommandString());
  }

  @Test
  void testValidateStageCreateTableWithoutAuthorizer() {
    var icebergAuthorizer = new IcebergAuthorizer(() -> null);
    icebergAuthorizer.validateStageCreateTable(CATALOG_NAME, NAMESPACE, Map.of(), stageCreateRequest(LOCATION, null));
  }

  @Test
  void testValidateStageCreateTableWithNonStageCreateRequest() {
    var hiveAuthorizer = mock(HiveAuthorizer.class);
    var icebergAuthorizer = new IcebergAuthorizer(() -> hiveAuthorizer);
    var request = CreateTableRequest.builder().withName(TABLE_NAME).withSchema(new Schema()).build();

    var exception = Assertions.assertThrows(IllegalArgumentException.class, () ->
        icebergAuthorizer.validateStageCreateTable(CATALOG_NAME, NAMESPACE, Map.of(), request));
    Assertions.assertEquals("Only stage create requests are supported", exception.getMessage());
    verifyNoInteractions(hiveAuthorizer);
  }

  @Test
  void testValidateStageCreateTableWithMultiLevelNamespace() {
    var hiveAuthorizer = mock(HiveAuthorizer.class);
    var icebergAuthorizer = new IcebergAuthorizer(() -> hiveAuthorizer);

    var nestedNamespace = Namespace.of("db", "nested");
    var request = stageCreateRequest(LOCATION, null);
    var exception = Assertions.assertThrows(IllegalArgumentException.class, () ->
        icebergAuthorizer.validateStageCreateTable(CATALOG_NAME, nestedNamespace, Map.of(), request));
    Assertions.assertEquals("Hive does not support multi-level namespaces", exception.getMessage());
    verifyNoInteractions(hiveAuthorizer);
  }

  @Test
  void testValidateStageCreateTableRejected() throws Exception {
    var hiveAuthorizer = mock(HiveAuthorizer.class);
    var failure = new HiveAccessControlException("access denied");
    doThrow(failure).when(hiveAuthorizer).checkPrivileges(any(), anyList(), anyList(), any());
    var icebergAuthorizer = new IcebergAuthorizer(() -> hiveAuthorizer);

    var request = stageCreateRequest(LOCATION, null);
    var exception = Assertions.assertThrows(ForbiddenException.class, () -> icebergAuthorizer
        .validateStageCreateTable(CATALOG_NAME, NAMESPACE, Map.of(), request));
    Assertions.assertEquals("access denied", exception.getMessage());
    Assertions.assertSame(failure, exception.getCause());
  }

  @Test
  @SuppressWarnings("unchecked")
  void testAuthorizeLoadTable() throws Exception {
    var hiveAuthorizer = mock(HiveAuthorizer.class);
    var icebergAuthorizer = new IcebergAuthorizer(() -> hiveAuthorizer);

    icebergAuthorizer.authorizeLoadTable(CATALOG_NAME, TableIdentifier.of(NAMESPACE, TABLE_NAME));

    var operation = ArgumentCaptor.forClass(HiveOperationType.class);
    var inputs = ArgumentCaptor.forClass(List.class);
    var outputs = ArgumentCaptor.forClass(List.class);
    verify(hiveAuthorizer).checkPrivileges(operation.capture(), inputs.capture(), outputs.capture(), any());

    Assertions.assertEquals(HiveOperationType.QUERY, operation.getValue());
    Assertions.assertEquals(List.of(), outputs.getValue());
    Assertions.assertEquals(1, inputs.getValue().size());
    var input = (HivePrivilegeObject) inputs.getValue().getFirst();
    assertThat(input.getType()).isEqualTo(HivePrivilegeObjectType.TABLE_OR_VIEW);
    assertThat(input.getCatName()).isEqualTo(CATALOG_NAME);
    assertThat(input.getDbname()).isEqualTo(NAMESPACE.level(0));
    assertThat(input.getObjectName()).isEqualTo(TABLE_NAME);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testAuthorizeLoadTableNormalizesMetadataTable() throws Exception {
    var hiveAuthorizer = mock(HiveAuthorizer.class);
    var icebergAuthorizer = new IcebergAuthorizer(() -> hiveAuthorizer);

    // A metadata-table identifier db.table.snapshots must be checked against its base table.
    var metadataTable = TableIdentifier.of(Namespace.of(NAMESPACE.level(0), TABLE_NAME), "snapshots");
    icebergAuthorizer.authorizeLoadTable(CATALOG_NAME, metadataTable);

    var inputs = ArgumentCaptor.forClass(List.class);
    verify(hiveAuthorizer).checkPrivileges(any(), inputs.capture(), anyList(), any());
    var input = (HivePrivilegeObject) inputs.getValue().getFirst();
    assertThat(input.getType()).isEqualTo(HivePrivilegeObjectType.TABLE_OR_VIEW);
    assertThat(input.getDbname()).isEqualTo(NAMESPACE.level(0));
    assertThat(input.getObjectName()).isEqualTo(TABLE_NAME);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testFilterTables() throws Exception {
    var hiveAuthorizer = mock(HiveAuthorizer.class);
    var visible = TableIdentifier.of(NAMESPACE, "visible");
    var hidden = TableIdentifier.of(NAMESPACE, "hidden");
    // Only "visible" survives the filter.
    Mockito.when(hiveAuthorizer.filterListCmdObjects(anyList(), any())).thenAnswer(invocation -> {
      List<HivePrivilegeObject> objects = invocation.getArgument(0);
      return objects.stream().filter(o -> "visible".equals(o.getObjectName())).collect(Collectors.toList());
    });
    var icebergAuthorizer = new IcebergAuthorizer(() -> hiveAuthorizer);

    Assertions.assertEquals(List.of(visible), icebergAuthorizer.filterTables(CATALOG_NAME, List.of(visible, hidden)));

    var objects = ArgumentCaptor.forClass(List.class);
    verify(hiveAuthorizer).filterListCmdObjects(objects.capture(), any());
    var passed = (HivePrivilegeObject) objects.getValue().getFirst();
    assertThat(passed.getType()).isEqualTo(HivePrivilegeObjectType.TABLE_OR_VIEW);
    assertThat(passed.getCatName()).isEqualTo(CATALOG_NAME);
    assertThat(passed.getDbname()).isEqualTo(NAMESPACE.level(0));
  }

  @Test
  void testFilterViews() throws Exception {
    var hiveAuthorizer = mock(HiveAuthorizer.class);
    var visible = TableIdentifier.of(NAMESPACE, "visible_view");
    var hidden = TableIdentifier.of(NAMESPACE, "hidden_view");
    Mockito.when(hiveAuthorizer.filterListCmdObjects(anyList(), any())).thenAnswer(invocation -> {
      List<HivePrivilegeObject> objects = invocation.getArgument(0);
      return objects.stream().filter(o -> "visible_view".equals(o.getObjectName())).collect(Collectors.toList());
    });
    var icebergAuthorizer = new IcebergAuthorizer(() -> hiveAuthorizer);

    Assertions.assertEquals(List.of(visible), icebergAuthorizer.filterViews(CATALOG_NAME, List.of(visible, hidden)));
  }

  @Test
  void testFilterNamespaces() throws Exception {
    var hiveAuthorizer = mock(HiveAuthorizer.class);
    var visible = Namespace.of("visible_db");
    var hidden = Namespace.of("hidden_db");
    Mockito.when(hiveAuthorizer.filterListCmdObjects(anyList(), any())).thenAnswer(invocation -> {
      List<HivePrivilegeObject> objects = invocation.getArgument(0);
      return objects.stream().filter(o -> "visible_db".equals(o.getDbname())).collect(Collectors.toList());
    });
    var icebergAuthorizer = new IcebergAuthorizer(() -> hiveAuthorizer);

    Assertions.assertEquals(
        List.of(visible), icebergAuthorizer.filterNamespaces(CATALOG_NAME, List.of(visible, hidden)));
  }

  @Test
  void testFilterNamespacesSkipsMultiLevel() throws Exception {
    var hiveAuthorizer = mock(HiveAuthorizer.class);
    Mockito.when(hiveAuthorizer.filterListCmdObjects(anyList(), any())).thenAnswer(invocation -> invocation.getArgument(0));
    var icebergAuthorizer = new IcebergAuthorizer(() -> hiveAuthorizer);

    var single = Namespace.of("db");
    var multi = Namespace.of("db", "nested");
    // Multi-level namespaces cannot map to a Hive database and are dropped rather than failing.
    Assertions.assertEquals(
        List.of(single), icebergAuthorizer.filterNamespaces(CATALOG_NAME, List.of(single, multi)));
  }

  @Test
  void testFilterWithoutAuthorizer() {
    var icebergAuthorizer = new IcebergAuthorizer(() -> null);
    var tables = List.of(TableIdentifier.of(NAMESPACE, TABLE_NAME));
    var namespaces = List.of(NAMESPACE);
    // Permissive when no authorizer is configured: the full listing is returned unchanged.
    Assertions.assertEquals(tables, icebergAuthorizer.filterTables(CATALOG_NAME, tables));
    Assertions.assertEquals(tables, icebergAuthorizer.filterViews(CATALOG_NAME, tables));
    Assertions.assertEquals(namespaces, icebergAuthorizer.filterNamespaces(CATALOG_NAME, namespaces));
  }

  @Test
  void testAuthorizeLoadTableRejected() throws Exception {
    var hiveAuthorizer = mock(HiveAuthorizer.class);
    var failure = new HiveAccessControlException("access denied");
    doThrow(failure).when(hiveAuthorizer).checkPrivileges(any(), anyList(), anyList(), any());
    var icebergAuthorizer = new IcebergAuthorizer(() -> hiveAuthorizer);

    var exception = Assertions.assertThrows(ForbiddenException.class, () ->
        icebergAuthorizer.authorizeLoadTable(CATALOG_NAME, TableIdentifier.of(NAMESPACE, TABLE_NAME)));
    Assertions.assertEquals("access denied", exception.getMessage());
    Assertions.assertSame(failure, exception.getCause());
  }

  @Test
  void testAuthorizeReadWithoutAuthorizer() {
    var icebergAuthorizer = new IcebergAuthorizer(() -> null);
    // Permissive when no authorizer is configured.
    icebergAuthorizer.authorizeLoadTable(CATALOG_NAME, TableIdentifier.of(NAMESPACE, TABLE_NAME));
  }

  @Test
  void testTranslateAuthorizationPluginException() throws Exception {
    HiveAuthorizer hiveAuthorizer = mock(HiveAuthorizer.class);
    HiveAuthzPluginException failure = new HiveAuthzPluginException("plugin failure");
    doThrow(failure).when(hiveAuthorizer).checkPrivileges(any(), anyList(), anyList(), any());
    IcebergAuthorizer icebergAuthorizer = new IcebergAuthorizer(() -> hiveAuthorizer);

    var request = stageCreateRequest(LOCATION, null);
    var exception = Assertions.assertThrows(IllegalStateException.class, () ->
        icebergAuthorizer.validateStageCreateTable(CATALOG_NAME, NAMESPACE, Map.of(), request));
    Assertions.assertEquals("Failed to check privileges for CREATETABLE", exception.getMessage());
    Assertions.assertSame(failure, exception.getCause());
  }

  @Test
  @SuppressWarnings("unchecked")
  void testValidateRegisterTableAuthorized() throws Exception {
    var hiveAuthorizer = mock(HiveAuthorizer.class);
    var icebergAuthorizer = new IcebergAuthorizer(() -> hiveAuthorizer);

    var metadataLocation = LOCATION + "/metadata/v1.metadata.json";
    var request = ImmutableRegisterTableRequest.builder().name(TABLE_NAME).metadataLocation(metadataLocation).build();

    icebergAuthorizer.validateRegisterTable(CATALOG_NAME, NAMESPACE, Map.of(), request);

    var operation = ArgumentCaptor.forClass(HiveOperationType.class);
    var inputs = ArgumentCaptor.forClass(List.class);
    var context = ArgumentCaptor.forClass(HiveAuthzContext.class);
    verify(hiveAuthorizer).checkPrivileges(operation.capture(), inputs.capture(), anyList(), context.capture());

    Assertions.assertEquals(HiveOperationType.CREATETABLE, operation.getValue());
    Assertions.assertEquals("register table " + TABLE_NAME, context.getValue().getCommandString());

    var checkedLocation = (HivePrivilegeObject) inputs.getValue().getFirst();
    assertThat(checkedLocation.getType()).isEqualTo(HivePrivilegeObjectType.DFS_URI);
    assertThat(checkedLocation.getObjectName()).isEqualTo(metadataLocation);
  }

  @Test
  void testValidateRegisterTableDeniedMetadataLocation() throws Exception {
    var hiveAuthorizer = mock(HiveAuthorizer.class);
    var failure = new HiveAccessControlException("access denied");
    doThrow(failure).when(hiveAuthorizer).checkPrivileges(any(), anyList(), anyList(), any());
    var icebergAuthorizer = new IcebergAuthorizer(() -> hiveAuthorizer);

    var request = ImmutableRegisterTableRequest.builder().name(TABLE_NAME).metadataLocation(LOCATION).build();
    var exception = Assertions.assertThrows(ForbiddenException.class, () ->
        icebergAuthorizer.validateRegisterTable(CATALOG_NAME, NAMESPACE, Map.of(), request));
    Assertions.assertEquals("access denied", exception.getMessage());
  }

  @Test
  void testValidateRegisterTableWithMultiLevelNamespace() {
    var hiveAuthorizer = mock(HiveAuthorizer.class);
    var icebergAuthorizer = new IcebergAuthorizer(() -> hiveAuthorizer);
    var nestedNamespace = Namespace.of("db", "nested");
    var request = ImmutableRegisterTableRequest.builder().name(TABLE_NAME).metadataLocation(LOCATION).build();

    var exception = Assertions.assertThrows(IllegalArgumentException.class, () ->
        icebergAuthorizer.validateRegisterTable(CATALOG_NAME, nestedNamespace, Map.of(), request));
    Assertions.assertEquals("Hive does not support multi-level namespaces", exception.getMessage());
    Mockito.verifyNoInteractions(hiveAuthorizer);
  }

  @Test
  void testValidateRegisterTableNoAuthorizerFallbackAllowed() {
    var icebergAuthorizer = new IcebergAuthorizer(() -> null);

    var externalRoot = "file:/warehouse/external";
    var namespaceMetadata = Map.of("location", externalRoot);
    var metadataLocation = externalRoot + "/table/metadata/v1.metadata.json";
    var request = ImmutableRegisterTableRequest.builder().name(TABLE_NAME).metadataLocation(metadataLocation).build();

    icebergAuthorizer.validateRegisterTable(CATALOG_NAME, NAMESPACE, namespaceMetadata, request);
  }

  @Test
  void testValidateRegisterTableNoAuthorizerFallbackDenied() {
    var icebergAuthorizer = new IcebergAuthorizer(() -> null);

    var request = ImmutableRegisterTableRequest.builder().name(TABLE_NAME).metadataLocation(LOCATION).build();
    Assertions.assertThrows(ForbiddenException.class,
        () -> icebergAuthorizer.validateRegisterTable(CATALOG_NAME, NAMESPACE, Map.of(), request));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testValidateDropTablePurgeAuthorized() throws Exception {
    var hiveAuthorizer = mock(HiveAuthorizer.class);
    var icebergAuthorizer = new IcebergAuthorizer(() -> hiveAuthorizer);
    var identifier = TableIdentifier.of(NAMESPACE, TABLE_NAME);

    icebergAuthorizer.validateDropTablePurge(CATALOG_NAME, identifier, LOCATION);

    var operation = ArgumentCaptor.forClass(HiveOperationType.class);
    var inputs = ArgumentCaptor.forClass(List.class);
    var outputs = ArgumentCaptor.forClass(List.class);
    var context = ArgumentCaptor.forClass(HiveAuthzContext.class);
    verify(hiveAuthorizer).checkPrivileges(operation.capture(), inputs.capture(), outputs.capture(), context.capture());

    Assertions.assertEquals(HiveOperationType.DROPTABLE, operation.getValue());
    Assertions.assertEquals(1, inputs.getValue().size());
    var inputLocation = (HivePrivilegeObject) inputs.getValue().getFirst();
    assertThat(inputLocation.getType()).isEqualTo(HivePrivilegeObjectType.DFS_URI);
    assertThat(inputLocation.getObjectName()).isEqualTo(LOCATION);
    Assertions.assertEquals(1, outputs.getValue().size());
    var outputLocation = (HivePrivilegeObject) outputs.getValue().getFirst();
    assertThat(outputLocation.getType()).isEqualTo(HivePrivilegeObjectType.DFS_URI);
    assertThat(outputLocation.getObjectName()).isEqualTo(LOCATION);
    Assertions.assertEquals("drop table " + TABLE_NAME, context.getValue().getCommandString());
  }

  @Test
  void testValidateDropTablePurgeDenied() throws Exception {
    var hiveAuthorizer = mock(HiveAuthorizer.class);
    var failure = new HiveAccessControlException("access denied");
    doThrow(failure).when(hiveAuthorizer).checkPrivileges(any(), anyList(), anyList(), any());
    var icebergAuthorizer = new IcebergAuthorizer(() -> hiveAuthorizer);
    var identifier = TableIdentifier.of(NAMESPACE, TABLE_NAME);

    var exception = Assertions.assertThrows(ForbiddenException.class, () ->
        icebergAuthorizer.validateDropTablePurge(CATALOG_NAME, identifier, LOCATION));
    Assertions.assertEquals("access denied", exception.getMessage());
    Assertions.assertSame(failure, exception.getCause());
  }

  @Test
  void testValidateDropTablePurgeTranslatesPluginException() throws Exception {
    var hiveAuthorizer = mock(HiveAuthorizer.class);
    var failure = new HiveAuthzPluginException("plugin failure");
    doThrow(failure).when(hiveAuthorizer).checkPrivileges(any(), anyList(), anyList(), any());
    var icebergAuthorizer = new IcebergAuthorizer(() -> hiveAuthorizer);
    var identifier = TableIdentifier.of(NAMESPACE, TABLE_NAME);

    var exception = Assertions.assertThrows(IllegalStateException.class, () ->
        icebergAuthorizer.validateDropTablePurge(CATALOG_NAME, identifier, LOCATION));
    Assertions.assertEquals("Failed to check privileges for DROPTABLE", exception.getMessage());
    Assertions.assertSame(failure, exception.getCause());
  }

  @Test
  void testValidateDropTablePurgeWithoutAuthorizer() {
    var icebergAuthorizer = new IcebergAuthorizer(() -> null);
    icebergAuthorizer.validateDropTablePurge(CATALOG_NAME, TableIdentifier.of(NAMESPACE, TABLE_NAME), LOCATION);
  }

  @Test
  void testResolveAllowedStorageOperationsWithoutAuthorizer() {
    var conf = new Configuration(false);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.PRE_EVENT_LISTENERS, "");
    var icebergAuthorizer = new IcebergAuthorizer(conf);

    var actual = icebergAuthorizer.resolveAllowedStorageOperations(CATALOG_NAME, TABLE_IDENTIFIER, COLUMNS);

    Assertions.assertEquals(EnumSet.allOf(StorageOperation.class), actual);
  }

  @Test
  void testResolveAllowedStorageOperationsWithReadAndWritable() throws Exception {
    var hiveAuthorizer = mock(HiveAuthorizer.class);
    when(hiveAuthorizer.needTransform()).thenReturn(false);

    var icebergAuthorizer = new IcebergAuthorizer(() -> hiveAuthorizer);
    var actual = icebergAuthorizer.resolveAllowedStorageOperations(CATALOG_NAME, TABLE_IDENTIFIER, COLUMNS);
    var allowedOperations = EnumSet.of(
        StorageOperation.LIST,
        StorageOperation.READ,
        StorageOperation.CREATE,
        StorageOperation.DELETE
    );
    Assertions.assertEquals(allowedOperations, actual);

    var operationCaptor = ArgumentCaptor.forClass(HiveOperationType.class);
    var inputCaptor = ArgumentCaptor.forClass(List.class);
    var outputCaptor = ArgumentCaptor.forClass(List.class);
    verify(hiveAuthorizer, times(2)).checkPrivileges(
        operationCaptor.capture(),
        inputCaptor.capture(),
        outputCaptor.capture(),
        any(HiveAuthzContext.class)
    );
    Assertions.assertEquals(List.of(HiveOperationType.QUERY, HiveOperationType.QUERY), operationCaptor.getAllValues());
    assertPrivilegeObjects(READABLE_PRIVILEGES, inputCaptor.getAllValues().getFirst());
    assertPrivilegeObjects(List.of(), inputCaptor.getAllValues().getLast());
    assertPrivilegeObjects(List.of(), outputCaptor.getAllValues().getFirst());
    assertPrivilegeObjects(WRITABLE_PRIVILEGES, outputCaptor.getAllValues().getLast());

    verify(hiveAuthorizer).needTransform();

    verifyNoMoreInteractions(hiveAuthorizer);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testResolveAllowedStorageOperationsWithReadable() throws Exception {
    var hiveAuthorizer = mock(HiveAuthorizer.class);
    doAnswer(invocation -> {
      // 2 is the index of the write privilege list
      if (!((List<?>) invocation.getArgument(2)).isEmpty()) {
        throw new HiveAccessControlException("write denied");
      }
      return null;
    }).when(hiveAuthorizer).checkPrivileges(any(), any(), any(), any());
    when(hiveAuthorizer.needTransform()).thenReturn(false);

    var icebergAuthorizer = new IcebergAuthorizer(() -> hiveAuthorizer);
    var actual = icebergAuthorizer.resolveAllowedStorageOperations(CATALOG_NAME, TABLE_IDENTIFIER, COLUMNS);
    Assertions.assertEquals(EnumSet.of(StorageOperation.LIST, StorageOperation.READ), actual);

    var operationCaptor = ArgumentCaptor.forClass(HiveOperationType.class);
    var inputCaptor = ArgumentCaptor.forClass(List.class);
    var outputCaptor = ArgumentCaptor.forClass(List.class);
    verify(hiveAuthorizer, times(2)).checkPrivileges(
        operationCaptor.capture(),
        inputCaptor.capture(),
        outputCaptor.capture(),
        any(HiveAuthzContext.class)
    );
    Assertions.assertEquals(List.of(HiveOperationType.QUERY, HiveOperationType.QUERY), operationCaptor.getAllValues());
    assertPrivilegeObjects(READABLE_PRIVILEGES, inputCaptor.getAllValues().getFirst());
    assertPrivilegeObjects(List.of(), inputCaptor.getAllValues().getLast());
    assertPrivilegeObjects(List.of(), outputCaptor.getAllValues().getFirst());
    assertPrivilegeObjects(WRITABLE_PRIVILEGES, outputCaptor.getAllValues().getLast());

    verify(hiveAuthorizer).needTransform();

    verifyNoMoreInteractions(hiveAuthorizer);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testResolveAllowedStorageOperationsWithReadableAndNullFilter() throws Exception {
    var hiveAuthorizer = mock(HiveAuthorizer.class);
    doAnswer(invocation -> {
      // 2 is the index of the write privilege list
      if (!((List<?>) invocation.getArgument(2)).isEmpty()) {
        throw new HiveAccessControlException("write denied");
      }
      return null;
    }).when(hiveAuthorizer).checkPrivileges(any(), any(), any(), any());
    when(hiveAuthorizer.needTransform()).thenReturn(true);
    when(hiveAuthorizer.applyRowFilterAndColumnMasking(any(), any())).thenReturn(null);

    var icebergAuthorizer = new IcebergAuthorizer(() -> hiveAuthorizer);
    var actual = icebergAuthorizer.resolveAllowedStorageOperations(CATALOG_NAME, TABLE_IDENTIFIER, COLUMNS);
    Assertions.assertEquals(EnumSet.of(StorageOperation.LIST, StorageOperation.READ), actual);

    var operationCaptor = ArgumentCaptor.forClass(HiveOperationType.class);
    var inputCaptor = ArgumentCaptor.forClass(List.class);
    var outputCaptor = ArgumentCaptor.forClass(List.class);
    verify(hiveAuthorizer, times(2)).checkPrivileges(
        operationCaptor.capture(),
        inputCaptor.capture(),
        outputCaptor.capture(),
        any(HiveAuthzContext.class)
    );
    Assertions.assertEquals(List.of(HiveOperationType.QUERY, HiveOperationType.QUERY), operationCaptor.getAllValues());
    assertPrivilegeObjects(READABLE_PRIVILEGES, inputCaptor.getAllValues().getFirst());
    assertPrivilegeObjects(List.of(), inputCaptor.getAllValues().getLast());
    assertPrivilegeObjects(List.of(), outputCaptor.getAllValues().getFirst());
    assertPrivilegeObjects(WRITABLE_PRIVILEGES, outputCaptor.getAllValues().getLast());

    var filterInputCaptor = ArgumentCaptor.forClass(List.class);
    verify(hiveAuthorizer).applyRowFilterAndColumnMasking(
        any(HiveAuthzContext.class),
        filterInputCaptor.capture()
    );
    Assertions.assertEquals(1, filterInputCaptor.getAllValues().size());
    assertPrivilegeObjects(READABLE_PRIVILEGES, filterInputCaptor.getAllValues().getFirst());

    verify(hiveAuthorizer).needTransform();

    verifyNoMoreInteractions(hiveAuthorizer);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testResolveAllowedStorageOperationsWithReadableAndEmptyFilter() throws Exception {
    var hiveAuthorizer = mock(HiveAuthorizer.class);
    doAnswer(invocation -> {
      // 2 is the index of the write privilege list
      if (!((List<?>) invocation.getArgument(2)).isEmpty()) {
        throw new HiveAccessControlException("write denied");
      }
      return null;
    }).when(hiveAuthorizer).checkPrivileges(any(), any(), any(), any());
    when(hiveAuthorizer.needTransform()).thenReturn(true);
    when(hiveAuthorizer.applyRowFilterAndColumnMasking(any(), any())).thenReturn(List.of());

    var icebergAuthorizer = new IcebergAuthorizer(() -> hiveAuthorizer);
    var actual = icebergAuthorizer.resolveAllowedStorageOperations(CATALOG_NAME, TABLE_IDENTIFIER, COLUMNS);
    Assertions.assertEquals(EnumSet.of(StorageOperation.LIST, StorageOperation.READ), actual);

    var operationCaptor = ArgumentCaptor.forClass(HiveOperationType.class);
    var inputCaptor = ArgumentCaptor.forClass(List.class);
    var outputCaptor = ArgumentCaptor.forClass(List.class);
    verify(hiveAuthorizer, times(2)).checkPrivileges(
        operationCaptor.capture(),
        inputCaptor.capture(),
        outputCaptor.capture(),
        any(HiveAuthzContext.class)
    );
    Assertions.assertEquals(List.of(HiveOperationType.QUERY, HiveOperationType.QUERY), operationCaptor.getAllValues());
    assertPrivilegeObjects(READABLE_PRIVILEGES, inputCaptor.getAllValues().getFirst());
    assertPrivilegeObjects(List.of(), inputCaptor.getAllValues().getLast());
    assertPrivilegeObjects(List.of(), outputCaptor.getAllValues().getFirst());
    assertPrivilegeObjects(WRITABLE_PRIVILEGES, outputCaptor.getAllValues().getLast());

    var filterInputCaptor = ArgumentCaptor.forClass(List.class);
    verify(hiveAuthorizer).applyRowFilterAndColumnMasking(
        any(HiveAuthzContext.class),
        filterInputCaptor.capture()
    );
    Assertions.assertEquals(1, filterInputCaptor.getAllValues().size());
    assertPrivilegeObjects(READABLE_PRIVILEGES, filterInputCaptor.getAllValues().getFirst());

    verify(hiveAuthorizer).needTransform();

    verifyNoMoreInteractions(hiveAuthorizer);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testResolveAllowedStorageOperationsWithReadableAndFilterUnavailable() throws Exception {
    var hiveAuthorizer = mock(HiveAuthorizer.class);
    doAnswer(invocation -> {
      // 2 is the index of the write privilege list
      if (!((List<?>) invocation.getArgument(2)).isEmpty()) {
        throw new HiveAccessControlException("write denied");
      }
      return null;
    }).when(hiveAuthorizer).checkPrivileges(any(), any(), any(), any());
    when(hiveAuthorizer.needTransform()).thenReturn(true);
    when(hiveAuthorizer.applyRowFilterAndColumnMasking(any(), any())).thenThrow(new SemanticException("error"));

    var icebergAuthorizer = new IcebergAuthorizer(() -> hiveAuthorizer);
    var actual = icebergAuthorizer.resolveAllowedStorageOperations(CATALOG_NAME, TABLE_IDENTIFIER, COLUMNS);
    Assertions.assertEquals(Set.of(), actual);

    var operationCaptor = ArgumentCaptor.forClass(HiveOperationType.class);
    var inputCaptor = ArgumentCaptor.forClass(List.class);
    var outputCaptor = ArgumentCaptor.forClass(List.class);
    verify(hiveAuthorizer).checkPrivileges(
        operationCaptor.capture(),
        inputCaptor.capture(),
        outputCaptor.capture(),
        any(HiveAuthzContext.class)
    );
    Assertions.assertEquals(List.of(HiveOperationType.QUERY), operationCaptor.getAllValues());
    assertPrivilegeObjects(READABLE_PRIVILEGES, inputCaptor.getAllValues().getFirst());
    assertPrivilegeObjects(List.of(), outputCaptor.getAllValues().getFirst());

    var filterInputCaptor = ArgumentCaptor.forClass(List.class);
    verify(hiveAuthorizer).applyRowFilterAndColumnMasking(
        any(HiveAuthzContext.class),
        filterInputCaptor.capture()
    );
    Assertions.assertEquals(1, filterInputCaptor.getAllValues().size());
    assertPrivilegeObjects(READABLE_PRIVILEGES, filterInputCaptor.getAllValues().getFirst());

    verify(hiveAuthorizer).needTransform();

    verifyNoMoreInteractions(hiveAuthorizer);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testResolveAllowedStorageOperationsWithReadableAndFilter() throws Exception {
    var hiveAuthorizer = mock(HiveAuthorizer.class);
    doAnswer(invocation -> {
      // 2 is the index of the write privilege list
      if (!((List<?>) invocation.getArgument(2)).isEmpty()) {
        throw new HiveAccessControlException("write denied");
      }
      return null;
    }).when(hiveAuthorizer).checkPrivileges(any(), any(), any(), any());
    when(hiveAuthorizer.needTransform()).thenReturn(true);
    when(hiveAuthorizer.applyRowFilterAndColumnMasking(any(), any()))
        .thenReturn(List.of(mock(HivePrivilegeObject.class)));

    var icebergAuthorizer = new IcebergAuthorizer(() -> hiveAuthorizer);
    var actual = icebergAuthorizer.resolveAllowedStorageOperations(CATALOG_NAME, TABLE_IDENTIFIER, COLUMNS);
    Assertions.assertEquals(Set.of(), actual);

    var operationCaptor = ArgumentCaptor.forClass(HiveOperationType.class);
    var inputCaptor = ArgumentCaptor.forClass(List.class);
    var outputCaptor = ArgumentCaptor.forClass(List.class);
    verify(hiveAuthorizer).checkPrivileges(
        operationCaptor.capture(),
        inputCaptor.capture(),
        outputCaptor.capture(),
        any(HiveAuthzContext.class)
    );
    Assertions.assertEquals(List.of(HiveOperationType.QUERY), operationCaptor.getAllValues());
    assertPrivilegeObjects(READABLE_PRIVILEGES, inputCaptor.getAllValues().getFirst());
    assertPrivilegeObjects(List.of(), outputCaptor.getAllValues().getFirst());

    var filterInputCaptor = ArgumentCaptor.forClass(List.class);
    verify(hiveAuthorizer).applyRowFilterAndColumnMasking(
        any(HiveAuthzContext.class),
        filterInputCaptor.capture()
    );
    Assertions.assertEquals(1, filterInputCaptor.getAllValues().size());
    assertPrivilegeObjects(READABLE_PRIVILEGES, filterInputCaptor.getAllValues().getFirst());

    verify(hiveAuthorizer).needTransform();

    verifyNoMoreInteractions(hiveAuthorizer);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testResolveAllowedStorageOperationsWithWritable() throws Exception {
    var hiveAuthorizer = mock(HiveAuthorizer.class);
    doAnswer(invocation -> {
      // 1 is the index of the write privilege list
      if (!((List<?>) invocation.getArgument(1)).isEmpty()) {
        throw new HiveAccessControlException("read denied");
      }
      return null;
    }).when(hiveAuthorizer).checkPrivileges(any(), any(), any(), any());

    var icebergAuthorizer = new IcebergAuthorizer(() -> hiveAuthorizer);
    var actual = icebergAuthorizer.resolveAllowedStorageOperations(CATALOG_NAME, TABLE_IDENTIFIER, COLUMNS);
    Assertions.assertEquals(Set.of(), actual);

    var operationCaptor = ArgumentCaptor.forClass(HiveOperationType.class);
    var inputCaptor = ArgumentCaptor.forClass(List.class);
    var outputCaptor = ArgumentCaptor.forClass(List.class);
    verify(hiveAuthorizer).checkPrivileges(
        operationCaptor.capture(),
        inputCaptor.capture(),
        outputCaptor.capture(),
        any(HiveAuthzContext.class)
    );
    Assertions.assertEquals(List.of(HiveOperationType.QUERY), operationCaptor.getAllValues());
    assertPrivilegeObjects(READABLE_PRIVILEGES, inputCaptor.getAllValues().getFirst());
    assertPrivilegeObjects(List.of(), outputCaptor.getAllValues().getFirst());

    verifyNoMoreInteractions(hiveAuthorizer);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testResolveAllowedStorageOperationsWithNoPrivilege() throws Exception {
    var hiveAuthorizer = mock(HiveAuthorizer.class);
    doAnswer(invocation -> {
      throw new HiveAccessControlException("denied");
    }).when(hiveAuthorizer).checkPrivileges(any(), any(), any(), any());

    var icebergAuthorizer = new IcebergAuthorizer(() -> hiveAuthorizer);
    var actual = icebergAuthorizer.resolveAllowedStorageOperations(CATALOG_NAME, TABLE_IDENTIFIER, COLUMNS);
    Assertions.assertEquals(Set.of(), actual);

    var operationCaptor = ArgumentCaptor.forClass(HiveOperationType.class);
    var inputCaptor = ArgumentCaptor.forClass(List.class);
    var outputCaptor = ArgumentCaptor.forClass(List.class);
    verify(hiveAuthorizer).checkPrivileges(
        operationCaptor.capture(),
        inputCaptor.capture(),
        outputCaptor.capture(),
        any(HiveAuthzContext.class)
    );
    Assertions.assertEquals(List.of(HiveOperationType.QUERY), operationCaptor.getAllValues());
    assertPrivilegeObjects(READABLE_PRIVILEGES, inputCaptor.getAllValues().getFirst());
    assertPrivilegeObjects(List.of(), outputCaptor.getAllValues().getFirst());

    verifyNoMoreInteractions(hiveAuthorizer);
  }

  @Test
  void testResolveAllowedStorageOperationsTranslatesPluginError() throws Exception {
    var hiveAuthorizer = mock(HiveAuthorizer.class);
    doAnswer(invocation -> {
      throw new HiveAuthzPluginException("boom");
    }).when(hiveAuthorizer).checkPrivileges(any(), any(), any(), any());

    var icebergAuthorizer = new IcebergAuthorizer(() -> hiveAuthorizer);
    Assertions.assertThrows(IllegalStateException.class, () ->
        icebergAuthorizer.resolveAllowedStorageOperations(CATALOG_NAME, TABLE_IDENTIFIER, COLUMNS));
  }
}
