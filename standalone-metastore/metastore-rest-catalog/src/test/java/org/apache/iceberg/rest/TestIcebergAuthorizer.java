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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
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
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.extension.MockHiveAuthorizer;
import org.apache.iceberg.rest.extension.MockHiveAuthorizerFactory;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class TestIcebergAuthorizer {
  private static final String CATALOG_NAME = "hive";
  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final String TABLE_NAME = "table";
  private static final String LOCATION = "file:/warehouse/db/table";

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
    Mockito.verifyNoInteractions(hiveAuthorizer);
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
    Mockito.verifyNoInteractions(hiveAuthorizer);
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
  void testTranslateAuthorizationPluginException() throws Exception {
    HiveAuthorizer hiveAuthorizer = mock(HiveAuthorizer.class);
    HiveAuthzPluginException failure = new HiveAuthzPluginException("plugin failure");
    doThrow(failure).when(hiveAuthorizer).checkPrivileges(any(), anyList(), anyList(), any());
    IcebergAuthorizer icebergAuthorizer = new IcebergAuthorizer(() -> hiveAuthorizer);

    var request = stageCreateRequest(LOCATION, null);
    var exception = Assertions.assertThrows(IllegalStateException.class, () ->
        icebergAuthorizer.validateStageCreateTable(CATALOG_NAME, NAMESPACE, Map.of(), request));
    Assertions.assertEquals("Failed to check privileges stage-create", exception.getMessage());
    Assertions.assertSame(failure, exception.getCause());
  }
}
