/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.security.authorization.plugin.metastore;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreFilterHook;
import org.apache.hadoop.hive.metastore.HMSHandler;
import org.apache.hadoop.hive.metastore.MetaStorePreEventListener;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.PreAlterTableEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateTableEvent;
import org.apache.hadoop.hive.metastore.events.PreDropTableEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.security.HiveMetastoreAuthenticationProvider;
import static org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObjectUtils.TablePrivilegeLookup;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.events.*;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactoryImpl;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.filtercontext.DataConnectorFilterContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.filtercontext.DatabaseFilterContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.filtercontext.TableFilterContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * HiveMetaStoreAuthorizer :  Do authorization checks on MetaStore Events in MetaStorePreEventListener
 */

public class HiveMetaStoreAuthorizer extends MetaStorePreEventListener implements MetaStoreFilterHook {
  private static final Logger LOG = LoggerFactory.getLogger(HiveMetaStoreAuthorizer.class);

  private static final ThreadLocal<Configuration> tConfig = new ThreadLocal<Configuration>() {

    @Override
    protected Configuration initialValue() {
      return null;
    }
  };

  private static final ThreadLocal<HiveMetastoreAuthenticationProvider> tAuthenticator = new ThreadLocal<HiveMetastoreAuthenticationProvider>() {
    @Override
    protected HiveMetastoreAuthenticationProvider initialValue() {
      try {
        return (HiveMetastoreAuthenticationProvider) HiveUtils.getAuthenticator(tConfig.get(), HiveConf.ConfVars.HIVE_METASTORE_AUTHENTICATOR_MANAGER);
      } catch (HiveException excp) {
        throw new IllegalStateException("Authentication provider instantiation failure", excp);
      }
    }
  };

  public HiveMetaStoreAuthorizer(Configuration config) {
    super(config);
  }

  @Override
  public final void onEvent(PreEventContext preEventContext)
      throws MetaException, NoSuchObjectException, InvalidOperationException {
    LOG.debug("==> HiveMetaStoreAuthorizer.onEvent(): EventType=" + preEventContext.getEventType());

    try {
      HiveMetaStoreAuthzInfo authzContext = buildAuthzContext(preEventContext);

      if (!skipAuthorization(authzContext)) {
        HiveAuthorizer hiveAuthorizer = createHiveMetaStoreAuthorizer();
        checkPrivileges(authzContext, hiveAuthorizer);
      }
    } catch (Exception e) {
      LOG.error("HiveMetaStoreAuthorizer.onEvent(): failed", e);
      throw MetaStoreUtils.newMetaException(e);
    }

    LOG.debug("<== HiveMetaStoreAuthorizer.onEvent(): EventType=" + preEventContext.getEventType());
  }

  @Override
  public final List<String> filterDatabases(List<String> list) throws MetaException {
    LOG.debug("HiveMetaStoreAuthorizer.filterDatabases()");

    if (list == null) {
      return Collections.emptyList();
    }

    DatabaseFilterContext databaseFilterContext = new DatabaseFilterContext(list);
    HiveMetaStoreAuthzInfo hiveMetaStoreAuthzInfo = databaseFilterContext.getAuthzContext();
    List<String> filteredDatabases = filterDatabaseObjects(hiveMetaStoreAuthzInfo);
    if (CollectionUtils.isEmpty(filteredDatabases)) {
      filteredDatabases = Collections.emptyList();
    }

    LOG.debug("HiveMetaStoreAuthorizer.filterDatabases() :" + filteredDatabases);

    return filteredDatabases;
  }

  @Override
  public final Database filterDatabase(Database database) throws MetaException, NoSuchObjectException {
    if (database != null) {
      String dbName = database.getName();
      List<String> databases = filterDatabases(Collections.singletonList(dbName));
      if (databases.isEmpty()) {
        throw new NoSuchObjectException(String.format("Database %s does not exist", dbName));
      }
    }
    return database;
  }

  @Override
  public final List<String> filterTableNames(String s, String s1, List<String> list) throws MetaException {
    LOG.debug("==> HiveMetaStoreAuthorizer.filterTableNames()");

    List<String> filteredTableNames = null;
    if (list != null) {
      String dbName = getDBName(s1);
      TableFilterContext tableFilterContext = new TableFilterContext(dbName, list);
      HiveMetaStoreAuthzInfo hiveMetaStoreAuthzInfo = tableFilterContext.getAuthzContext();
      filteredTableNames = filterTableNames(hiveMetaStoreAuthzInfo, dbName, list);
      if (CollectionUtils.isEmpty(filteredTableNames)) {
        filteredTableNames = Collections.emptyList();
      }
    }

    LOG.debug("<== HiveMetaStoreAuthorizer.filterTableNames() : " + filteredTableNames);

    return filteredTableNames;
  }

  @Override
  public final Table filterTable(Table table) throws MetaException, NoSuchObjectException {
    if (table != null) {
      List<Table> tables = filterTables(Collections.singletonList(table));
      if (tables.isEmpty()) {
        throw new NoSuchObjectException(String.format("Database %s does not exist", table.getTableName()));
      }
    }
    return table;
  }

  @Override
  public final List<Table> filterTables(List<Table> list) throws MetaException {
    LOG.debug("==> HiveMetaStoreAuthorizer.filterTables()");

    List<Table> filteredTables = null;

    if (list != null) {
      TableFilterContext tableFilterContext = new TableFilterContext(list);
      HiveMetaStoreAuthzInfo hiveMetaStoreAuthzInfo = tableFilterContext.getAuthzContext();
      filteredTables = filterTableObjects(hiveMetaStoreAuthzInfo, list);
      if (CollectionUtils.isEmpty(filteredTables)) {
        filteredTables = Collections.emptyList();
      }
    }

    LOG.debug("<== HiveMetaStoreAuthorizer.filterTables(): " + filteredTables);

    return filteredTables;
  }

  @Override
  public final Catalog filterCatalog(Catalog catalog) throws MetaException {
    return catalog;
  }

  @Override
  public final List<String> filterCatalogs(List<String> catalogs) throws MetaException {
    return catalogs;
  }

  @Override
  @Deprecated
  public List<TableMeta> filterTableMetas(String catName, String dbName,List<TableMeta> tableMetas)
      throws MetaException {
    return filterTableMetas(tableMetas);
  }

  @Override
  public final List<TableMeta> filterTableMetas(List<TableMeta> tableMetas)
      throws MetaException {
    return tableMetas;
  }

  @Override
  public final List<Partition> filterPartitions(List<Partition> list) throws MetaException {
    return list;
  }

  @Override
  public final List<PartitionSpec> filterPartitionSpecs(List<PartitionSpec> list) throws MetaException {
    return list;
  }

  @Override
  public final Partition filterPartition(Partition partition) throws MetaException, NoSuchObjectException {
    return partition;
  }

  @Override
  public final List<String> filterPartitionNames(String s, String s1, String s2, List<String> list)
      throws MetaException {
    return list;
  }

  @Override
  public List<String> filterDataConnectors(List<String> dcList) throws MetaException {
    LOG.debug("HiveMetaStoreAuthorizer.filterDataConnector()");

    if (dcList == null) {
      return Collections.emptyList();
    }

    DataConnectorFilterContext dataConnectorFilterContext = new DataConnectorFilterContext(dcList);
    HiveMetaStoreAuthzInfo hiveMetaStoreAuthzInfo = dataConnectorFilterContext.getAuthzContext();
    List<String> filteredDataConnector = filterDataConnectorObjects(hiveMetaStoreAuthzInfo);
    if (CollectionUtils.isEmpty(filteredDataConnector)) {
      filteredDataConnector = Collections.emptyList();
    }

    LOG.debug("HiveMetaStoreAuthorizer.filterDataConnectors() :" + filteredDataConnector);

    return filteredDataConnector;
  }

  private List<String> filterDatabaseObjects(HiveMetaStoreAuthzInfo hiveMetaStoreAuthzInfo) throws MetaException {
    List<String> ret = null;

    LOG.debug("==> HiveMetaStoreAuthorizer.filterDatabaseObjects()");

    try {
      HiveAuthorizer hiveAuthorizer = createHiveMetaStoreAuthorizer();
      List<HivePrivilegeObject> hivePrivilegeObjects = hiveMetaStoreAuthzInfo.getInputHObjs();
      HiveAuthzContext hiveAuthzContext = hiveMetaStoreAuthzInfo.getHiveAuthzContext();
      List<HivePrivilegeObject> filteredHivePrivilegeObjects =
          hiveAuthorizer.filterListCmdObjects(hivePrivilegeObjects, hiveAuthzContext);
      if (CollectionUtils.isNotEmpty(filteredHivePrivilegeObjects)) {
        ret = getFilteredDatabaseList(filteredHivePrivilegeObjects);
      }
      LOG.info(String.format("Filtered %d databases out of %d", filteredHivePrivilegeObjects.size(),
          hivePrivilegeObjects.size()));
    } catch (Exception e) {
      throw new MetaException("Error in HiveMetaStoreAuthorizer.filterDatabase()" + e.getMessage());
    }

    LOG.debug("<== HiveMetaStoreAuthorizer.filterDatabaseObjects() :" + ret );

    return ret;
  }

  private List<String> filterDataConnectorObjects(HiveMetaStoreAuthzInfo hiveMetaStoreAuthzInfo) throws MetaException {
    List<String> ret = null;

    LOG.debug("==> HiveMetaStoreAuthorizer.filterDataConnectorObjects()");

    try {
      HiveAuthorizer hiveAuthorizer = createHiveMetaStoreAuthorizer();
      List<HivePrivilegeObject> hivePrivilegeObjects = hiveMetaStoreAuthzInfo.getInputHObjs();
      HiveAuthzContext hiveAuthzContext = hiveMetaStoreAuthzInfo.getHiveAuthzContext();
      List<HivePrivilegeObject> filteredHivePrivilegeObjects =
              hiveAuthorizer.filterListCmdObjects(hivePrivilegeObjects, hiveAuthzContext);
      if (CollectionUtils.isNotEmpty(filteredHivePrivilegeObjects)) {
        ret = getFilteredDataConnectorList(filteredHivePrivilegeObjects);
      }
      LOG.info(String.format("Filtered %d connectors out of %d", filteredHivePrivilegeObjects.size(),
              hivePrivilegeObjects.size()));
    } catch (Exception e) {
      throw new MetaException("Error in HiveMetaStoreAuthorizer.filterDataConnector()" + e.getMessage());
    }

    LOG.debug("<== HiveMetaStoreAuthorizer.filterDataConnectorObjects() :" + ret );

    return ret;
  }

  private List<String> getFilteredDataConnectorList(List<HivePrivilegeObject> hivePrivilegeObjects) {
    List<String> ret = new ArrayList<>();
    for(HivePrivilegeObject hivePrivilegeObject: hivePrivilegeObjects) {
      String dcName = hivePrivilegeObject.getObjectName();
      ret.add(dcName);
    }
    return ret;
  }

  private List<Table> filterTableObjects(HiveMetaStoreAuthzInfo hiveMetaStoreAuthzInfo, List<Table> tableList)
      throws MetaException {
    List<Table> ret = null;

    try {
      HiveAuthorizer hiveAuthorizer = createHiveMetaStoreAuthorizer();
      List<HivePrivilegeObject> hivePrivilegeObjects = hiveMetaStoreAuthzInfo.getInputHObjs();
      HiveAuthzContext hiveAuthzContext = hiveMetaStoreAuthzInfo.getHiveAuthzContext();
      List<HivePrivilegeObject> filteredHivePrivilegeObjects =
          hiveAuthorizer.filterListCmdObjects(hivePrivilegeObjects, hiveAuthzContext);
      if (CollectionUtils.isNotEmpty(filteredHivePrivilegeObjects)) {
        ret = getFilteredTableList(filteredHivePrivilegeObjects, tableList);
      }
      LOG.info(String.format("Filtered %d tables out of %d", filteredHivePrivilegeObjects.size(),
          hivePrivilegeObjects.size()));
    } catch (Exception e) {
      throw new MetaException("Error in HiveMetaStoreAuthorizer.filterTables()" + e.getMessage());
    }
    return ret;
  }

  private List<String> getFilteredDatabaseList(List<HivePrivilegeObject> hivePrivilegeObjects) {
    List<String> ret = new ArrayList<>();
    for(HivePrivilegeObject hivePrivilegeObject: hivePrivilegeObjects) {
      String dbName = hivePrivilegeObject.getDbname();
      ret.add(dbName);
    }
    return ret;
  }

  private List<Table> getFilteredTableList(List<HivePrivilegeObject> hivePrivilegeObjects, List<Table> tableList) {
    final List<Table> ret = new ArrayList<>();
    final TablePrivilegeLookup index = new TablePrivilegeLookup(hivePrivilegeObjects);
    for(Table table : tableList) {
      if (index.lookup(table.getDbName(), table.getTableName()) != null) {
        ret.add(table);
      }
    }
    return ret;
  }


  private List<String> filterTableNames(HiveMetaStoreAuthzInfo hiveMetaStoreAuthzInfo, String dbName,
      List<String> tableNames) throws MetaException {
    List<String> ret = null;

    try {
      HiveAuthorizer hiveAuthorizer = createHiveMetaStoreAuthorizer();
      List<HivePrivilegeObject> hivePrivilegeObjects = hiveMetaStoreAuthzInfo.getInputHObjs();
      HiveAuthzContext hiveAuthzContext = hiveMetaStoreAuthzInfo.getHiveAuthzContext();
      List<HivePrivilegeObject> filteredHivePrivilegeObjects =
          hiveAuthorizer.filterListCmdObjects(hivePrivilegeObjects, hiveAuthzContext);
      if (CollectionUtils.isNotEmpty(filteredHivePrivilegeObjects)) {
        ret = getFilteredTableNames(filteredHivePrivilegeObjects, dbName, tableNames);
      }
      LOG.info(String.format("Filtered %d table names out of %d", filteredHivePrivilegeObjects.size(),
          hivePrivilegeObjects.size()));
    } catch (Exception e) {
      throw new MetaException("Error in HiveMetaStoreAuthorizer.filterTables()" + e.getMessage());
    }
    return ret;
  }
  private List<String> getFilteredTableNames(List<HivePrivilegeObject> hivePrivilegeObjects, String databaseName, List<String> tableNames) {
    List<String> ret = new ArrayList<>();
    final TablePrivilegeLookup index = new TablePrivilegeLookup(hivePrivilegeObjects);
    for(String tableName : tableNames) {
      if (index.lookup(databaseName, tableName) != null) {
        ret.add(tableName);
      }
    }
    return ret;
  }
  private String getDBName(String str) {
   return (str != null) ? str.substring(str.indexOf("#")+1) : null;
  }

  HiveMetaStoreAuthzInfo buildAuthzContext(PreEventContext preEventContext) throws MetaException {
    LOG.debug("==> HiveMetaStoreAuthorizer.buildAuthzContext(): EventType=" + preEventContext.getEventType());

    HiveMetaStoreAuthorizableEvent authzEvent = null;

    if (preEventContext != null) {

      switch (preEventContext.getEventType()) {
        case CREATE_DATABASE:
          authzEvent = new CreateDatabaseEvent(preEventContext);
          break;
        case ALTER_DATABASE:
          authzEvent = new AlterDatabaseEvent(preEventContext);
          break;
        case DROP_DATABASE:
          authzEvent = new DropDatabaseEvent(preEventContext);
          break;
        case CREATE_TABLE:
          authzEvent = new CreateTableEvent(preEventContext);
          if (isViewOperation(preEventContext) && (!isSuperUser(getCurrentUser(authzEvent)))) {
            //we allow view to be created, but mark it as having not been authorized
            PreCreateTableEvent pcte = (PreCreateTableEvent)preEventContext;
            Map<String, String> params = pcte.getTable().getParameters();
            params.put("Authorized", "false");
          }
          break;
        case ALTER_TABLE:
          authzEvent = new AlterTableEvent(preEventContext);
          if (isViewOperation(preEventContext) && (!isSuperUser(getCurrentUser(authzEvent)))) {
            //we allow view to be altered, but mark it as having not been authorized
            PreAlterTableEvent pcte = (PreAlterTableEvent)preEventContext;
            Map<String, String> params = pcte.getNewTable().getParameters();
            params.put("Authorized", "false");
          }
          break;
        case DROP_TABLE:
          authzEvent = new DropTableEvent(preEventContext);
          if (isViewOperation(preEventContext) && (!isSuperUser(getCurrentUser(authzEvent)))) {
            //TODO: do we need to check Authorized flag?
          }
          break;
        case ADD_PARTITION:
          authzEvent = new AddPartitionEvent(preEventContext);
          break;
        case ALTER_PARTITION:
          authzEvent = new AlterPartitionEvent(preEventContext);
          break;
        case LOAD_PARTITION_DONE:
          authzEvent = new LoadPartitionDoneEvent(preEventContext);
          break;
        case DROP_PARTITION:
          authzEvent = new DropPartitionEvent(preEventContext);
          break;
        case READ_TABLE:
          authzEvent = new ReadTableEvent(preEventContext);
          break;
        case READ_DATABASE:
          authzEvent = new ReadDatabaseEvent(preEventContext);
          break;
        case CREATE_FUNCTION:
          authzEvent = new CreateFunctionEvent(preEventContext);
          break;
        case DROP_FUNCTION:
          authzEvent = new DropFunctionEvent(preEventContext);
          break;
        case CREATE_DATACONNECTOR:
          authzEvent = new CreateDataConnectorEvent(preEventContext);
          break;
        case ALTER_DATACONNECTOR:
          authzEvent = new AlterDataConnectorEvent(preEventContext);
          break;
        case DROP_DATACONNECTOR:
          authzEvent = new DropDataConnectorEvent(preEventContext);
          break;
        case AUTHORIZATION_API_CALL:
        case READ_ISCHEMA:
        case CREATE_ISCHEMA:
        case DROP_ISCHEMA:
        case ALTER_ISCHEMA:
        case ADD_SCHEMA_VERSION:
        case ALTER_SCHEMA_VERSION:
        case DROP_SCHEMA_VERSION:
        case READ_SCHEMA_VERSION:
        case CREATE_CATALOG:
        case ALTER_CATALOG:
        case DROP_CATALOG:
          if (!isSuperUser(getCurrentUser())) {
            throw new MetaException(getErrorMessage(preEventContext, getCurrentUser()));
          }
          break;
        default:
          break;
       }
    }

    HiveMetaStoreAuthzInfo ret = authzEvent != null ? authzEvent.getAuthzContext() : null;

    LOG.debug("<== HiveMetaStoreAuthorizer.buildAuthzContext(): EventType=" + preEventContext.getEventType() + "; ret=" + ret);

    return ret;
  }

  HiveAuthorizer createHiveMetaStoreAuthorizer() throws Exception {
    HiveAuthorizer ret = null;
    HiveConf hiveConf = (HiveConf)tConfig.get();
    if(hiveConf == null){
      HiveConf hiveConf1 = new HiveConf(super.getConf(), HiveConf.class);
      tConfig.set(hiveConf1);
      hiveConf = hiveConf1;
    }
    HiveAuthorizerFactory authorizerFactory =
        HiveUtils.getAuthorizerFactory(hiveConf, HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER);

    if (authorizerFactory != null) {
      HiveMetastoreAuthenticationProvider authenticator = tAuthenticator.get();

      authenticator.setConf(hiveConf);

      HiveAuthzSessionContext.Builder authzContextBuilder = new HiveAuthzSessionContext.Builder();

      authzContextBuilder.setClientType(HiveAuthzSessionContext.CLIENT_TYPE.HIVEMETASTORE);
      authzContextBuilder.setSessionString("HiveMetaStore");

      HiveAuthzSessionContext authzSessionContext = authzContextBuilder.build();

      ret = authorizerFactory
          .createHiveAuthorizer(new HiveMetastoreClientFactoryImpl(), hiveConf, authenticator, authzSessionContext);
    }

    return ret;
  }

  boolean isSuperUser(String userName) {
    Configuration conf      = getConf();
    String        ipAddress = HMSHandler.getIPAddress();
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
    return (MetaStoreServerUtils.checkUserHasHostProxyPrivileges(userName, conf, ipAddress));
  }

  boolean isViewOperation(PreEventContext preEventContext) {
    boolean ret = false;

    PreEventContext.PreEventType  preEventType = preEventContext.getEventType();

    switch (preEventType) {
      case CREATE_TABLE:
        PreCreateTableEvent preCreateTableEvent = (PreCreateTableEvent) preEventContext;
        Table table = preCreateTableEvent.getTable();
        ret         = isViewType(table);
        break;
      case ALTER_TABLE:
        PreAlterTableEvent preAlterTableEvent  = (PreAlterTableEvent) preEventContext;
        Table inTable  = preAlterTableEvent.getOldTable();
        Table outTable = preAlterTableEvent.getNewTable();
        ret            = (isViewType(inTable) || isViewType(outTable));
        break;
      case  DROP_TABLE:
        PreDropTableEvent preDropTableEvent = (PreDropTableEvent) preEventContext;
        Table droppedTable = preDropTableEvent.getTable();
        ret                = isViewType(droppedTable);
        break;
    }

    return ret;
  }

  private void checkPrivileges(final HiveMetaStoreAuthzInfo authzContext, HiveAuthorizer authorizer)
      throws HiveAccessControlException, HiveAuthzPluginException {
    LOG.debug("==> HiveMetaStoreAuthorizer.checkPrivileges(): authzContext=" + authzContext + ", authorizer=" + authorizer);

    HiveOperationType         hiveOpType       = authzContext.getOperationType();
    List<HivePrivilegeObject> inputHObjs       = authzContext.getInputHObjs();
    List<HivePrivilegeObject> outputHObjs      = authzContext.getOutputHObjs();
    HiveAuthzContext          hiveAuthzContext = authzContext.getHiveAuthzContext();

    authorizer.checkPrivileges(hiveOpType, inputHObjs, outputHObjs, hiveAuthzContext);

    LOG.debug("<== HiveMetaStoreAuthorizer.checkPrivileges(): authzContext=" + authzContext + ", authorizer=" + authorizer);
  }

  private boolean skipAuthorization(HiveMetaStoreAuthzInfo authzContext) {
    LOG.debug("==> HiveMetaStoreAuthorizer.skipAuthorization()");

    //If HMS does not check the event type, it will leave it as null. We don't try to authorize null pointer. 
    if(authzContext == null){
      return true;
    }
    boolean ret = false;
    UserGroupInformation ugi = null;
    try {
      ugi = getUGI();
      ret = isSuperUser(ugi.getShortUserName());
    } catch (IOException e) {
      LOG.warn("Not able to obtain UserGroupInformation", e);
    }

    LOG.debug("<== HiveMetaStoreAuthorizer.skipAuthorization(): " + ret);

    return ret;
  }

  private  boolean isViewType(Table table) {
    boolean ret = false;

    String tableType = table.getTableType();

    if (TableType.MATERIALIZED_VIEW.name().equals(tableType) || TableType.VIRTUAL_VIEW.name().equals(tableType)) {
      ret = true;
    }

    return ret;
  }

  private String getErrorMessage(PreEventContext preEventContext, String user) {
    String err = "Operation type " + preEventContext.getEventType().name() + " not allowed for user:" + user;
    return err;
  }

  private String getErrorMessage(String eventType, String user) {
    String err = "Operation type " + eventType + " not allowed for user:" + user;
    return err;
  }

  private String getCurrentUser() {
    try {
      return UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException excp) {
    }
    return null;
  }

  private String getCurrentUser(HiveMetaStoreAuthorizableEvent authorizableEvent) {
    return authorizableEvent.getAuthzContext().getUGI().getShortUserName();
  }

  private UserGroupInformation getUGI() throws IOException {
    return UserGroupInformation.getCurrentUser();
  }
}

