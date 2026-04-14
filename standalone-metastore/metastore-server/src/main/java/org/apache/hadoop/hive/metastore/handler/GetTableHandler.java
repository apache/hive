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

package org.apache.hadoop.hive.metastore.handler;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.DatabaseName;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.HMSHandler;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.IMetaStoreMetadataTransformer;
import org.apache.hadoop.hive.metastore.MetaStoreFilterHook;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.ClientCapabilities;
import org.apache.hadoop.hive.metastore.api.ClientCapability;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.ExtendedTableInfo;
import org.apache.hadoop.hive.metastore.api.GetProjectionsSpec;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesExtRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesExtRequestFields;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.dataconnector.DataConnectorProviderFactory;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.metastore.events.PreReadDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreReadTableEvent;
import org.apache.hadoop.hive.metastore.utils.FilterUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.ExceptionHandler.handleException;
import static org.apache.hadoop.hive.metastore.ExceptionHandler.newMetaException;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.HIVE_IN_TEST;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.CAT_NAME;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.DB_NAME;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.isDatabaseRemote;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.parseDbName;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.prependCatalogToDbName;
import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

@SuppressWarnings({"unchecked", "rawtypes"})
@RequestHandler(requestBody = GetTableHandler.GetTableReq.class)
public class GetTableHandler<R, T> extends
    AbstractRequestHandler<GetTableHandler.GetTableReq<R>, GetTableHandler.GetTableResult<T>> {
  private static final Logger LOG = LoggerFactory.getLogger(GetTableHandler.class);
  private RawStore ms;
  private IMetaStoreMetadataTransformer transformer;
  private MetaStoreFilterHook filterHook;
  private Configuration conf;
  private boolean isInTest;
  GetTableHandler(IHMSHandler handler, GetTableReq request) {
    super(handler, false, request);
  }

  @Override
  protected void beforeExecute() throws TException, IOException {
    this.ms = handler.getMS();
    this.transformer = handler.getMetadataTransformer();
    this.filterHook = handler.getMetaFilterHook();
    this.conf = handler.getConf();

    this.isInTest = MetastoreConf.getBoolVar(handler.getConf(), HIVE_IN_TEST);
  }

  @Override
  protected GetTableResult execute() throws TException, IOException {
    R req = request.getRequest();
    if (req instanceof GetTablesExtRequest getTablesExtRequest) {
      List<ExtendedTableInfo> extendedTableInfos = getTablesExt(getTablesExtRequest);
      return new GetTableResult(extendedTableInfos, true);
    } else if (req instanceof GetTableRequest getTableRequest) {
      Table table = request.rawTable ? getTableCore(getTableRequest) : getTable(getTableRequest);
      return new GetTableResult(List.of(table), true);
    } else if (req instanceof GetTablesRequest getTablesRequest) {
      List<Table> tables = getTableObjects(getTablesRequest);
      return new GetTableResult(tables, true);
    } else if (req instanceof GetTableNamesRequest getTableNames) {
      return getTableNames.forTableMeta ? new GetTableResult(getTableMeta(getTableNames), true) :
          new GetTableResult(getTableNames(getTableNames), true);
    }
    throw new UnsupportedOperationException(req + " not yet implemented");
  }

  private List<ExtendedTableInfo> getTablesExt(GetTablesExtRequest req) throws MetaException, UnknownDBException {
    List<ExtendedTableInfo> ret = new ArrayList<ExtendedTableInfo>();
    String pattern  = req.getTableNamePattern();
    List<String> processorCapabilities = req.getProcessorCapabilities();
    int limit = req.getLimit();
    String catalog = req.isSetCatalog() ? req.getCatalog() : getDefaultCatalog(conf);
    String database = req.getDatabase();
    String processorId  = req.getProcessorIdentifier();
    List<String> tables = ms.getTables(catalog, database, pattern, null, limit);
    LOG.debug("get_tables_ext:getTables() returned {}", tables.size());
    tables = FilterUtils.filterTableNamesIfEnabled(filterHook != null, filterHook,
        catalog, database, tables);
    if (tables.isEmpty()) {
      return ret;
    }
    List<Table> tObjects = ms.getTableObjectsByName(catalog, database, tables);
    LOG.debug("get_tables_ext:getTableObjectsByName() returned {}", tObjects.size());
    if (processorCapabilities == null || processorCapabilities.isEmpty() ||
        processorCapabilities.contains("MANAGERAWMETADATA")) {
      LOG.info("Skipping translation for processor with {}", processorId);
    } else {
      if (transformer != null) {
        Map<Table, List<String>> retMap = transformer.transform(tObjects, processorCapabilities, processorId);
        for (Map.Entry<Table, List<String>> entry : retMap.entrySet())  {
          LOG.debug("Table " + entry.getKey().getTableName() + " requires " + Arrays.toString((entry.getValue()).toArray()));
          ret.add(convertTableToExtendedTable(entry.getKey(), entry.getValue(), req.getRequestedFields()));
        }
      } else {
        for (Table table : tObjects) {
          ret.add(convertTableToExtendedTable(table, processorCapabilities, req.getRequestedFields()));
        }
      }
    }
    return ret;
  }

  private ExtendedTableInfo convertTableToExtendedTable(Table table,
      List<String> processorCapabilities, int mask) {
    ExtendedTableInfo extTable = new ExtendedTableInfo(table.getTableName());
    if ((mask & GetTablesExtRequestFields.ACCESS_TYPE.getValue()) == GetTablesExtRequestFields.ACCESS_TYPE.getValue()) {
      extTable.setAccessType(table.getAccessType());
    }

    if ((mask & GetTablesExtRequestFields.PROCESSOR_CAPABILITIES.getValue())
        == GetTablesExtRequestFields.PROCESSOR_CAPABILITIES.getValue()) {
      extTable.setRequiredReadCapabilities(table.getRequiredReadCapabilities());
      extTable.setRequiredWriteCapabilities(table.getRequiredWriteCapabilities());
    }

    return extTable;
  }

  /**
   * This function retrieves table from metastore. If getColumnStats flag is true,
   * then engine should be specified so the table is retrieve with the column stats
   * for that engine.
   */
  private Table getTable(GetTableRequest getTableRequest) throws MetaException, NoSuchObjectException {

    Preconditions.checkArgument(!getTableRequest.isGetColumnStats() || getTableRequest.getEngine() != null,
        "To retrieve column statistics with a table, engine parameter cannot be null");

    if (isInTest) {
      assertClientHasCapability(getTableRequest.getCapabilities(), ClientCapability.TEST_CAPABILITY, "Hive tests",
          "get_table_req");
    }

    Table t = getTableCore(getTableRequest);
    if (MetaStoreUtils.isInsertOnlyTableParam(t.getParameters())) {
      assertClientHasCapability(getTableRequest.getCapabilities(), ClientCapability.INSERT_ONLY_TABLES,
          "insert-only tables", "get_table_req");
    }

    if (CollectionUtils.isEmpty(getTableRequest.getProcessorCapabilities()) || getTableRequest
        .getProcessorCapabilities().contains("MANAGERAWMETADATA")) {
      LOG.info("Skipping translation for processor with " + getTableRequest.getProcessorIdentifier());
    } else {
      if (transformer != null) {
        List<Table> tList = new ArrayList<>();
        tList.add(t);
        Map<Table, List<String>> ret = transformer
            .transform(tList, getTableRequest.getProcessorCapabilities(), getTableRequest.getProcessorIdentifier());
        if (ret.size() > 1) {
          LOG.warn("Unexpected resultset size:{}", ret.size());
          throw new MetaException("Unexpected result from metadata transformer:return list size is " + ret.size());
        }
        t = ret.keySet().iterator().next();
      }
    }

    ((HMSHandler) handler).firePreEvent(new PreReadTableEvent(t, handler));
    return t;
  }

  /**
   * This function retrieves table from metastore. If getColumnStats flag is true,
   * then engine should be specified so the table is retrieve with the column stats
   * for that engine.
   */
  private Table getTableCore(GetTableRequest getTableRequest) throws MetaException, NoSuchObjectException {
    Preconditions.checkArgument(!getTableRequest.isGetColumnStats() || getTableRequest.getEngine() != null,
        "To retrieve column statistics with a table, engine parameter cannot be null");
    String catName = getTableRequest.getCatName();
    String dbName = getTableRequest.getDbName();
    String tblName = getTableRequest.getTblName();
    Database db = null;
    Table t;
    try {
      db = handler.get_database_core(catName, dbName);
    } catch (Exception e) { /* appears exception is not thrown currently if db doesnt exist */ }

    if (MetaStoreUtils.isDatabaseRemote(db)) {
      t = DataConnectorProviderFactory.getDataConnectorProvider(db).getTable(tblName);
      if (t == null) {
        throw new NoSuchObjectException(TableName.getQualified(catName, dbName, tblName) + " table not found");
      }
      t.setDbName(dbName);
      return t;
    }

    t = ms.getTable(catName, dbName, tblName, getTableRequest.getValidWriteIdList(), getTableRequest.getId());
    if (t == null) {
      throw new NoSuchObjectException(TableName.getQualified(catName, dbName, tblName) + " table not found");
    }

    // If column statistics was requested and is valid fetch it.
    if (getTableRequest.isGetColumnStats()) {
      ColumnStatistics colStats = ms.getTableColumnStatistics(catName, dbName, tblName,
          StatsSetupConst.getColumnsHavingStats(t.getParameters()), getTableRequest.getEngine(),
          getTableRequest.getValidWriteIdList());
      if (colStats != null) {
        t.setColStats(colStats);
      }
    }
    return t;
  }

  private List<Table> getTableObjects(GetTablesRequest req) throws TException {
    String catName = req.isSetCatName() ? req.getCatName() : getDefaultCatalog(conf);
    String dbName = req.getDbName();
    if (dbName == null || dbName.isEmpty()) {
      throw new UnknownDBException("DB name is null or empty");
    }
    try {
      Database database = handler.get_database_core(catName, dbName);
      if (isDatabaseRemote(database)) {
        return getRemoteTableObjectsInternal(database, req.getTblNames(), req.getTablesPattern());
      }
    } catch (NoSuchObjectException nse) {
      // The caller consumes UnknownDBException other than NoSuchObjectException
      // in case database doesn't exist
      throw new UnknownDBException("Could not find database " + DatabaseName.getQualified(catName, dbName));
    }
    return getTableObjectsInternal(req);
  }

  private List<Table> filterTablesByName(List<Table> tables, List<String> tableNames) {
    List<Table> filteredTables = new ArrayList<>();
    for (Table table : tables) {
      if (tableNames.contains(table.getTableName())) {
        filteredTables.add(table);
      }
    }
    return filteredTables;
  }

  private List<Table> getRemoteTableObjectsInternal(Database db, List<String> tableNames, String pattern) throws MetaException {
    try {
      List<Table> tables = DataConnectorProviderFactory.getDataConnectorProvider(db).getTables(null);
      // filtered out undesired tables
      if (tableNames != null) {
        tables = filterTablesByName(tables, tableNames);
      }
      // set remote tables' local hive database reference
      for (Table table : tables) {
        table.setDbName(db.getName());
      }
      return FilterUtils.filterTablesIfEnabled(filterHook != null, filterHook, tables);
    } catch (Exception e) {
      LOG.warn("Unexpected exception while getting table(s) in remote database " + db.getName() , e);
      if (isInTest) {
        // ignore the exception
        return new ArrayList<Table>();
      } else {
        throw newMetaException(e);
      }
    }
  }

  private List<Table> getTableObjectsInternal(GetTablesRequest req)
      throws MetaException, InvalidOperationException, UnknownDBException {
    if (isInTest) {
      assertClientHasCapability(req.getCapabilities(), ClientCapability.TEST_CAPABILITY,
          "Hive tests", "get_table_objects_by_name_req");
    }

    GetProjectionsSpec projectionsSpec = req.getProjectionSpec();
    if (projectionsSpec != null) {
      if (!projectionsSpec.isSetFieldList() && (projectionsSpec.isSetIncludeParamKeyPattern() ||
          projectionsSpec.isSetExcludeParamKeyPattern())) {
        throw new InvalidOperationException("Include and Exclude Param key are not supported.");
      }
    }

    String dbName = req.getDbName();
    String catName = req.isSetCatName() ? req.getCatName() : getDefaultCatalog(conf);
    List<Table> tables = new ArrayList<>();
    int tableBatchSize = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.BATCH_RETRIEVE_MAX);
    List<String> tableNames = req.getTblNames();
    if(req.getTablesPattern() != null) {
      tables = ms.getTableObjectsByName(catName, dbName, tableNames, projectionsSpec, req.getTablesPattern());
    } else {
      if (tableNames == null) {
        throw new InvalidOperationException(dbName + " cannot find null tables");
      }

      // The list of table names could contain duplicates. RawStore.getTableObjectsByName()
      // only guarantees returning no duplicate table objects in one batch. If we need
      // to break into multiple batches, remove duplicates first.
      List<String> distinctTableNames = tableNames;
      if (distinctTableNames.size() > tableBatchSize) {
        List<String> lowercaseTableNames = new ArrayList<>();
        for (String tableName : tableNames) {
          lowercaseTableNames.add(normalizeIdentifier(tableName));
        }
        distinctTableNames = new ArrayList<>(new HashSet<>(lowercaseTableNames));
      }

      int startIndex = 0;
      // Retrieve the tables from the metastore in batches. Some databases like
      // Oracle cannot have over 1000 expressions in a in-list
      while (startIndex < distinctTableNames.size()) {
        int endIndex = Math.min(startIndex + tableBatchSize, distinctTableNames.size());
        tables.addAll(ms.getTableObjectsByName(catName, dbName, distinctTableNames.subList(
            startIndex, endIndex), projectionsSpec, null));
        startIndex = endIndex;
      }
    }
    for (Table t : tables) {
      if (t.getParameters() != null && MetaStoreUtils.isInsertOnlyTableParam(t.getParameters())) {
        assertClientHasCapability(req.getCapabilities(), ClientCapability.INSERT_ONLY_TABLES,
            "insert-only tables", "get_table_req");
      }
    }

    tables = FilterUtils.filterTablesIfEnabled(filterHook != null, filterHook, tables);
    return tables;
  }

  private void assertClientHasCapability(ClientCapabilities client,
      ClientCapability value, String what, String call) throws MetaException {
    if (!doesClientHaveCapability(client, value)) {
      throw new MetaException("Your client does not appear to support " + what + ". To skip"
          + " capability checks, please set " + MetastoreConf.ConfVars.CAPABILITY_CHECK.toString()
          + " to false. This setting can be set globally, or on the client for the current"
          + " metastore session. Note that this may lead to incorrect results, data loss,"
          + " undefined behavior, etc. if your client is actually incompatible. You can also"
          + " specify custom client capabilities via " + call + " API.");
    }
  }

  private boolean doesClientHaveCapability(ClientCapabilities client, ClientCapability value) {
    if (!MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.CAPABILITY_CHECK)) {
      return true;
    }
    return (client != null && client.isSetValues() && client.getValues().contains(value));
  }

  private List<TableMeta> getTableMeta(GetTableNamesRequest getNamesReq) throws TException {
    String catName = getNamesReq.catName;
    String dbname = getNamesReq.dbName;
    List<String> tblTypes = null;
    if (getNamesReq.tableType != null) {
      tblTypes = Arrays.asList(getNamesReq.tableType.split(","));
    }
    List<TableMeta> t = ms.getTableMeta(catName, dbname, getNamesReq.pattern, tblTypes);
    t = FilterUtils.filterTableMetasIfEnabled(filterHook != null, filterHook, t);
    return filterReadableTables(catName, t);
  }

  /**
   * filters out the table meta for which read database access is not granted
   * @param catName catalog name
   * @param tableMetas list of table metas
   * @return filtered list of table metas
   * @throws RuntimeException
   * @throws NoSuchObjectException
   */
  private List<TableMeta> filterReadableTables(String catName, List<TableMeta> tableMetas)
      throws RuntimeException, NoSuchObjectException {
    List<TableMeta> finalT = new ArrayList<>();
    Map<String, Boolean> databaseNames = new HashMap();
    for (TableMeta tableMeta : tableMetas) {
      String fullDbName = prependCatalogToDbName(catName, tableMeta.getDbName(), conf);
      if (databaseNames.get(fullDbName) == null) {
        boolean isExecptionThrown = false;
        try {
          fireReadDatabasePreEvent(fullDbName);
        } catch (MetaException e) {
          isExecptionThrown = true;
        }
        databaseNames.put(fullDbName, isExecptionThrown);
      }
      if (!databaseNames.get(fullDbName)) {
        finalT.add(tableMeta);
      }
    }
    return finalT;
  }

  /**
   * Fire a pre-event for read database operation, if there are any
   * pre-event listeners registered
   */
  private void fireReadDatabasePreEvent(final String name)
      throws MetaException, RuntimeException, NoSuchObjectException {
    Supplier<PreEventContext> supplier = () -> {
      String[] parsedDbName = parseDbName(name, conf);
      Database db = null;
      try {
        db = handler.get_database_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME]);
        if (db == null) {
          throw new NoSuchObjectException("Database: " + name + " not found");
        }
      } catch(MetaException | NoSuchObjectException e) {
        throw new RuntimeException(e);
      }
      return new PreReadDatabaseEvent(db, handler);
    };
    ((HMSHandler) handler).firePreEvent(supplier);
  }


  private List<String> getTableNames(GetTableNamesRequest getNamesReq) throws TException {
    String catName = getNamesReq.catName;
    String dbname = getNamesReq.dbName;
    try {
      Database database = handler.get_database_core(catName, dbname);
      if (isDatabaseRemote(database)) {
        return DataConnectorProviderFactory.getDataConnectorProvider(database).getTableNames();
      }
    } catch (NoSuchObjectException nse) {
      throw new UnknownDBException("Could not find database " + DatabaseName.getQualified(catName, dbname));
    }

    List<String> names;
    if (getNamesReq.filter != null) {
      names = ms.listTableNamesByFilter(catName, dbname, getNamesReq.filter, getNamesReq.limit);
    } else if (getNamesReq.tableType != null) {
      names = ms.getTables(catName, dbname, getNamesReq.pattern,
          TableType.valueOf(getNamesReq.tableType), -1);
    } else if (getNamesReq.pattern != null) {
      names = ms.getTables(catName, dbname, getNamesReq.pattern);
    } else {
      names = ms.getAllTables(catName, dbname);
    }
    if (filterHook != null && !names.isEmpty()) {
      String tables = String.join("|", names);
      List<TableMeta> tableMetas = ms.getTableMeta(catName, dbname, tables, null);
      if (tableMetas == null || tableMetas.isEmpty()) {
        return new ArrayList<>();
      }
      List<TableMeta> filteredTableMetas = FilterUtils.filterTableMetasIfEnabled(filterHook != null, filterHook, tableMetas);
      return filteredTableMetas.stream().map(TableMeta::getTableName).collect(Collectors.toList());
    }
    return names;
  }

  public static class GetTableReq<Req> extends TAbstractBase {
    private final Req request;
    private boolean rawTable;
    public GetTableReq(Req req) {
      this.request = req;
    }
    public Req getRequest() {
      return request;
    }
  }

  public static class GetTableNamesRequest extends TAbstractBase {
    private final String catName;
    private final String dbName;
    private String filter;
    private String pattern;
    private String tableType;
    private short limit;
    private boolean forTableMeta;
    private GetTableNamesRequest(String catalog, String database) {
      this.catName = catalog;
      this.dbName = database;
    }
    public static GetTableNamesRequest fromDatabase(String database, Configuration configuration)
        throws MetaException, UnknownDBException {
      String[] parsedDbName = parseDbName(database, configuration);
      if (parsedDbName[CAT_NAME] == null || parsedDbName[CAT_NAME].isEmpty() ||
          parsedDbName[DB_NAME] == null || parsedDbName[DB_NAME].isEmpty()) {
        throw new UnknownDBException("DB name is null or empty");
      }
      return new GetTableNamesRequest(parsedDbName[CAT_NAME], parsedDbName[DB_NAME]);
    }
    public GetTableNamesRequest byFilter(String filter, short limit) throws InvalidOperationException{
      if (filter == null) {
        throw new InvalidOperationException(filter + " cannot apply null filter");
      }
      this.filter = filter;
      this.limit = limit;
      return this;
    }

    public GetTableNamesRequest byType(String tableType, String pattern) {
      this.tableType = tableType;
      this.pattern = pattern;
      return this;
    }

    public GetTableNamesRequest byPattern(String pattern) {
      return byType(null, pattern);
    }

    public GetTableNamesRequest forTableMeta() {
      this.forTableMeta = true;
      return this;
    }
  }

  public record GetTableResult<T>(List<T> result, boolean success) implements Result {

  }

  public static <T, R> List<T> getTables(Runnable preHook, IHMSHandler handler, R req,
      Consumer<Pair<List<T>, Exception>> postHook) throws TException {
    if (preHook != null) {
      preHook.run();
    }
    List<T> tables = null;
    Exception ex = null;
    try {
      GetTableReq<R> internalRequest = new GetTableReq<>(req);
      GetTableHandler<GetTableRequest, T> getTablesHandler =
          AbstractRequestHandler.offer(handler, internalRequest);
      tables = getTablesHandler.getResult().result();
      return tables;
    } catch (Exception e) {
      ex = e;
      throw handleException(e).defaultTException();
    } finally {
      if (postHook != null) {
        Pair<List<T>, Exception> result = Pair.of(tables, ex);
        postHook.accept(result);
      }
    }
  }

  public static Table getTable(Runnable preHook,
      IHMSHandler handler, GetTableRequest request, boolean rawTable,
      Consumer<Pair<Table, Exception>> postHook)
      throws NoSuchObjectException, MetaException {
    if (preHook != null) {
      preHook.run();
    }
    Table t = null;
    Exception ex = null;
    try {
      GetTableReq<GetTableRequest> internalRequest = new GetTableReq<>(request);
      internalRequest.rawTable = rawTable;
      GetTableHandler<GetTableRequest, Table> getTableHandler =
          AbstractRequestHandler.offer(handler, internalRequest);
      List<Table> tables = getTableHandler.getResult().result();
      t = tables.getFirst();
      return t;
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(NoSuchObjectException.class, MetaException.class)
          .defaultMetaException();
    } finally {
      if (postHook != null) {
        Pair<Table, Exception> result = Pair.of(t, ex);
        postHook.accept(result);
      }
    }
  }
}
