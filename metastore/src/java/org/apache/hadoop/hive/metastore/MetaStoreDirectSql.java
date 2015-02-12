/**
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

package org.apache.hadoop.hive.metastore;

import static org.apache.commons.lang.StringUtils.join;
import static org.apache.commons.lang.StringUtils.repeat;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.jdo.JDODataStoreException;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;
import javax.jdo.Transaction;
import javax.jdo.datastore.JDOConnection;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.model.MDatabase;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnStatistics;
import org.apache.hadoop.hive.metastore.model.MTableColumnStatistics;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.FilterBuilder;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.LeafNode;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.LogicalOperator;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.Operator;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.TreeNode;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.TreeVisitor;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.datanucleus.store.rdbms.query.ForwardQueryResult;

import com.google.common.collect.Lists;

/**
 * This class contains the optimizations for MetaStore that rely on direct SQL access to
 * the underlying database. It should use ANSI SQL and be compatible with common databases
 * such as MySQL (note that MySQL doesn't use full ANSI mode by default), Postgres, etc.
 *
 * As of now, only the partition retrieval is done this way to improve job startup time;
 * JDOQL partition retrieval is still present so as not to limit the ORM solution we have
 * to SQL stores only. There's always a way to do without direct SQL.
 */
class MetaStoreDirectSql {
  private static enum DB {
    MYSQL,
    ORACLE,
    MSSQL,
    DERBY,
    OTHER
  }

  private static final int NO_BATCHING = -1, DETECT_BATCHING = 0;

  private static final Log LOG = LogFactory.getLog(MetaStoreDirectSql.class);
  private final PersistenceManager pm;
  /**
   * We want to avoid db-specific code in this class and stick with ANSI SQL. However:
   * 1) mysql and postgres are differently ansi-incompatible (mysql by default doesn't support
   * quoted identifiers, and postgres contravenes ANSI by coercing unquoted ones to lower case).
   * MySQL's way of working around this is simpler (just set ansi quotes mode on), so we will
   * use that. MySQL detection is done by actually issuing the set-ansi-quotes command;
   *
   * Use sparingly, we don't want to devolve into another DataNucleus...
   */
  private final DB dbType;
  private final int batchSize;
  private final boolean convertMapNullsToEmptyStrings;

  /**
   * Whether direct SQL can be used with the current datastore backing {@link #pm}.
   */
  private final boolean isCompatibleDatastore;

  public MetaStoreDirectSql(PersistenceManager pm, Configuration conf) {
    this.pm = pm;
    this.dbType = determineDbType();
    int batchSize = HiveConf.getIntVar(conf, ConfVars.METASTORE_DIRECT_SQL_PARTITION_BATCH_SIZE);
    if (batchSize == DETECT_BATCHING) {
      batchSize = (dbType == DB.ORACLE || dbType == DB.MSSQL) ? 1000 : NO_BATCHING;
    }
    this.batchSize = batchSize;

    convertMapNullsToEmptyStrings =
        HiveConf.getBoolVar(conf, ConfVars.METASTORE_ORM_RETRIEVE_MAPNULLS_AS_EMPTY_STRINGS);

    this.isCompatibleDatastore = ensureDbInit() && runTestQuery();
    if (isCompatibleDatastore) {
      LOG.info("Using direct SQL, underlying DB is " + dbType);
    }
  }

  private DB determineDbType() {
    DB dbType = DB.OTHER;
    if (runDbCheck("SET @@session.sql_mode=ANSI_QUOTES", "MySql")) {
      dbType = DB.MYSQL;
    } else if (runDbCheck("SELECT version FROM v$instance", "Oracle")) {
      dbType = DB.ORACLE;
    } else if (runDbCheck("SELECT @@version", "MSSQL")) {
      dbType = DB.MSSQL;
    } else {
      // TODO: maybe we should use getProductName to identify all the DBs
      String productName = getProductName();
      if (productName != null && productName.toLowerCase().contains("derby")) {
        dbType = DB.DERBY;
      }
    }
    return dbType;
  }

  private String getProductName() {
    JDOConnection jdoConn = pm.getDataStoreConnection();
    try {
      return ((Connection)jdoConn.getNativeConnection()).getMetaData().getDatabaseProductName();
    } catch (Throwable t) {
      LOG.warn("Error retrieving product name", t);
      return null;
    } finally {
      jdoConn.close(); // We must release the connection before we call other pm methods.
    }
  }

  private boolean ensureDbInit() {
    Transaction tx = pm.currentTransaction();
    try {
      // Force the underlying db to initialize.
      pm.newQuery(MDatabase.class, "name == ''").execute();
      pm.newQuery(MTableColumnStatistics.class, "dbName == ''").execute();
      pm.newQuery(MPartitionColumnStatistics.class, "dbName == ''").execute();
      return true;
    } catch (Exception ex) {
      LOG.warn("Database initialization failed; direct SQL is disabled", ex);
      tx.rollback();
      return false;
    }
  }

  private boolean runTestQuery() {
    Transaction tx = pm.currentTransaction();
    // Run a self-test query. If it doesn't work, we will self-disable. What a PITA...
    String selfTestQuery = "select \"DB_ID\" from \"DBS\"";
    try {
      pm.newQuery("javax.jdo.query.SQL", selfTestQuery).execute();
      tx.commit();
      return true;
    } catch (Exception ex) {
      LOG.warn("Self-test query [" + selfTestQuery + "] failed; direct SQL is disabled", ex);
      tx.rollback();
      return false;
    }
  }

  public boolean isCompatibleDatastore() {
    return isCompatibleDatastore;
  }

  /**
   * This function is intended to be called by functions before they put together a query
   * Thus, any query-specific instantiation to be done from within the transaction is done
   * here - for eg., for MySQL, we signal that we want to use ANSI SQL quoting behaviour
   */
  private void doDbSpecificInitializationsBeforeQuery() throws MetaException {
    if (dbType != DB.MYSQL) return;
    try {
      assert pm.currentTransaction().isActive(); // must be inside tx together with queries
      executeNoResult("SET @@session.sql_mode=ANSI_QUOTES");
    } catch (SQLException sqlEx) {
      throw new MetaException("Error setting ansi quotes: " + sqlEx.getMessage());
    }
  }

  private void executeNoResult(final String queryText) throws SQLException {
    JDOConnection jdoConn = pm.getDataStoreConnection();
    boolean doTrace = LOG.isDebugEnabled();
    try {
      long start = doTrace ? System.nanoTime() : 0;
      ((Connection)jdoConn.getNativeConnection()).createStatement().execute(queryText);
      timingTrace(doTrace, queryText, start, doTrace ? System.nanoTime() : 0);
    } finally {
      jdoConn.close(); // We must release the connection before we call other pm methods.
    }
  }

  private boolean runDbCheck(String queryText, String name) {
    Transaction tx = pm.currentTransaction();
    if (!tx.isActive()) {
      tx.begin();
    }
    try {
      executeNoResult(queryText);
      return true;
    } catch (Throwable t) {
      LOG.debug(name + " check failed, assuming we are not on " + name + ": " + t.getMessage());
      tx.rollback();
      tx = pm.currentTransaction();
      tx.begin();
      return false;
    }
  }

  public Database getDatabase(String dbName) throws MetaException{
    Query queryDbSelector = null;
    Query queryDbParams = null;
    try {
      dbName = dbName.toLowerCase();

      doDbSpecificInitializationsBeforeQuery();

      String queryTextDbSelector= "select "
          + "\"DB_ID\", \"NAME\", \"DB_LOCATION_URI\", \"DESC\", "
          + "\"OWNER_NAME\", \"OWNER_TYPE\" "
          + "FROM \"DBS\" where \"NAME\" = ? ";
      Object[] params = new Object[] { dbName };
      queryDbSelector = pm.newQuery("javax.jdo.query.SQL", queryTextDbSelector);

      if (LOG.isTraceEnabled()) {
        LOG.trace("getDatabase:query instantiated : " + queryTextDbSelector
            + " with param [" + params[0] + "]");
      }

      List<Object[]> sqlResult = executeWithArray(
          queryDbSelector, params, queryTextDbSelector);
      if ((sqlResult == null) || sqlResult.isEmpty()) {
        return null;
      }

      assert(sqlResult.size() == 1);
      if (sqlResult.get(0) == null) {
        return null;
      }

      Object[] dbline = sqlResult.get(0);
      Long dbid = extractSqlLong(dbline[0]);

      String queryTextDbParams = "select \"PARAM_KEY\", \"PARAM_VALUE\" "
          + " FROM \"DATABASE_PARAMS\" "
          + " WHERE \"DB_ID\" = ? "
          + " AND \"PARAM_KEY\" IS NOT NULL";
      params[0] = dbid;
      queryDbParams = pm.newQuery("javax.jdo.query.SQL", queryTextDbParams);
      if (LOG.isTraceEnabled()) {
        LOG.trace("getDatabase:query2 instantiated : " + queryTextDbParams
            + " with param [" + params[0] + "]");
      }

      Map<String,String> dbParams = new HashMap<String,String>();
      List<Object[]> sqlResult2 = ensureList(executeWithArray(
          queryDbParams, params, queryTextDbParams));
      if (!sqlResult2.isEmpty()) {
        for (Object[] line : sqlResult2) {
          dbParams.put(extractSqlString(line[0]), extractSqlString(line[1]));
        }
      }
      Database db = new Database();
      db.setName(extractSqlString(dbline[1]));
      db.setLocationUri(extractSqlString(dbline[2]));
      db.setDescription(extractSqlString(dbline[3]));
      db.setOwnerName(extractSqlString(dbline[4]));
      String type = extractSqlString(dbline[5]);
      db.setOwnerType(
          (null == type || type.trim().isEmpty()) ? null : PrincipalType.valueOf(type));
      db.setParameters(MetaStoreUtils.trimMapNulls(dbParams,convertMapNullsToEmptyStrings));
      if (LOG.isDebugEnabled()){
        LOG.debug("getDatabase: directsql returning db " + db.getName()
            + " locn["+db.getLocationUri()  +"] desc [" +db.getDescription()
            + "] owner [" + db.getOwnerName() + "] ownertype ["+ db.getOwnerType() +"]");
      }
      return db;
    } finally {
      if (queryDbSelector != null){
        queryDbSelector.closeAll();
      }
      if (queryDbParams != null){
        queryDbParams.closeAll();
      }
    }
  }

  /**
   * Gets partitions by using direct SQL queries.
   * Note that batching is not needed for this method - list of names implies the batch size;
   * @param dbName Metastore db name.
   * @param tblName Metastore table name.
   * @param partNames Partition names to get.
   * @return List of partitions.
   */
  public List<Partition> getPartitionsViaSqlFilter(
      String dbName, String tblName, List<String> partNames) throws MetaException {
    if (partNames.isEmpty()) {
      return new ArrayList<Partition>();
    }
    return getPartitionsViaSqlFilterInternal(dbName, tblName, null,
        "\"PARTITIONS\".\"PART_NAME\" in (" + makeParams(partNames.size()) + ")",
        partNames, new ArrayList<String>(), null);
  }

  /**
   * Gets partitions by using direct SQL queries.
   * @param table The table.
   * @param tree The expression tree from which the SQL filter will be derived.
   * @param max The maximum number of partitions to return.
   * @return List of partitions. Null if SQL filter cannot be derived.
   */
  public List<Partition> getPartitionsViaSqlFilter(
      Table table, ExpressionTree tree, Integer max) throws MetaException {
    assert tree != null;
    List<Object> params = new ArrayList<Object>();
    List<String> joins = new ArrayList<String>();
    // Derby and Oracle do not interpret filters ANSI-properly in some cases and need a workaround.
    boolean dbHasJoinCastBug = (dbType == DB.DERBY || dbType == DB.ORACLE);
    String sqlFilter = PartitionFilterGenerator.generateSqlFilter(
        table, tree, params, joins, dbHasJoinCastBug);
    if (sqlFilter == null) {
      return null; // Cannot make SQL filter to push down.
    }
    Boolean isViewTable = isViewTable(table);
    return getPartitionsViaSqlFilterInternal(table.getDbName(), table.getTableName(),
        isViewTable, sqlFilter, params, joins, max);
  }

  /**
   * Gets all partitions of a table by using direct SQL queries.
   * @param dbName Metastore db name.
   * @param tblName Metastore table name.
   * @param max The maximum number of partitions to return.
   * @return List of partitions.
   */
  public List<Partition> getPartitions(
      String dbName, String tblName, Integer max) throws MetaException {
    return getPartitionsViaSqlFilterInternal(dbName, tblName, null,
        null, new ArrayList<String>(), new ArrayList<String>(), max);
  }

  private static Boolean isViewTable(Table t) {
    return t.isSetTableType() ?
        t.getTableType().equals(TableType.VIRTUAL_VIEW.toString()) : null;
  }

  private boolean isViewTable(String dbName, String tblName) throws MetaException {
    String queryText = "select \"TBL_TYPE\" from \"TBLS\"" +
        " inner join \"DBS\" on \"TBLS\".\"DB_ID\" = \"DBS\".\"DB_ID\" " +
        " where \"TBLS\".\"TBL_NAME\" = ? and \"DBS\".\"NAME\" = ?";
    Object[] params = new Object[] { tblName, dbName };
    Query query = pm.newQuery("javax.jdo.query.SQL", queryText);
    query.setUnique(true);
    Object result = executeWithArray(query, params, queryText);
    return (result != null) && result.toString().equals(TableType.VIRTUAL_VIEW.toString());
  }

  /**
   * Get partition objects for the query using direct SQL queries, to avoid bazillion
   * queries created by DN retrieving stuff for each object individually.
   * @param dbName Metastore db name.
   * @param tblName Metastore table name.
   * @param isView Whether table is a view. Can be passed as null if not immediately
   *               known, then this method will get it only if necessary.
   * @param sqlFilter SQL filter to use. Better be SQL92-compliant.
   * @param paramsForFilter params for ?-s in SQL filter text. Params must be in order.
   * @param joinsForFilter if the filter needs additional join statement, they must be in
   *                       this list. Better be SQL92-compliant.
   * @param max The maximum number of partitions to return.
   * @return List of partition objects.
   */
  private List<Partition> getPartitionsViaSqlFilterInternal(String dbName, String tblName,
      Boolean isView, String sqlFilter, List<? extends Object> paramsForFilter,
      List<String> joinsForFilter, Integer max) throws MetaException {
    boolean doTrace = LOG.isDebugEnabled();
    dbName = dbName.toLowerCase();
    tblName = tblName.toLowerCase();
    // We have to be mindful of order during filtering if we are not returning all partitions.
    String orderForFilter = (max != null) ? " order by \"PART_NAME\" asc" : "";

    doDbSpecificInitializationsBeforeQuery();

    // Get all simple fields for partitions and related objects, which we can map one-on-one.
    // We will do this in 2 queries to use different existing indices for each one.
    // We do not get table and DB name, assuming they are the same as we are using to filter.
    // TODO: We might want to tune the indexes instead. With current ones MySQL performs
    // poorly, esp. with 'order by' w/o index on large tables, even if the number of actual
    // results is small (query that returns 8 out of 32k partitions can go 4sec. to 0sec. by
    // just adding a \"PART_ID\" IN (...) filter that doesn't alter the results to it, probably
    // causing it to not sort the entire table due to not knowing how selective the filter is.
    String queryText =
        "select \"PARTITIONS\".\"PART_ID\" from \"PARTITIONS\""
      + "  inner join \"TBLS\" on \"PARTITIONS\".\"TBL_ID\" = \"TBLS\".\"TBL_ID\" "
      + "    and \"TBLS\".\"TBL_NAME\" = ? "
      + "  inner join \"DBS\" on \"TBLS\".\"DB_ID\" = \"DBS\".\"DB_ID\" "
      + "     and \"DBS\".\"NAME\" = ? "
      + join(joinsForFilter, ' ')
      + (StringUtils.isBlank(sqlFilter) ? "" : (" where " + sqlFilter)) + orderForFilter;
    Object[] params = new Object[paramsForFilter.size() + 2];
    params[0] = tblName;
    params[1] = dbName;
    for (int i = 0; i < paramsForFilter.size(); ++i) {
      params[i + 2] = paramsForFilter.get(i);
    }

    long start = doTrace ? System.nanoTime() : 0;
    Query query = pm.newQuery("javax.jdo.query.SQL", queryText);
    if (max != null) {
      query.setRange(0, max.shortValue());
    }
    List<Object> sqlResult = executeWithArray(query, params, queryText);
    long queryTime = doTrace ? System.nanoTime() : 0;
    if (sqlResult.isEmpty()) {
      timingTrace(doTrace, queryText, start, queryTime);
      return new ArrayList<Partition>(); // no partitions, bail early.
    }

    // Get full objects. For Oracle, do it in batches.
    List<Partition> result = null;
    if (batchSize != NO_BATCHING && batchSize < sqlResult.size()) {
      result = new ArrayList<Partition>(sqlResult.size());
      while (result.size() < sqlResult.size()) {
        int toIndex = Math.min(result.size() + batchSize, sqlResult.size());
        List<Object> batchedSqlResult = sqlResult.subList(result.size(), toIndex);
        result.addAll(getPartitionsFromPartitionIds(dbName, tblName, isView, batchedSqlResult));
      }
    } else {
      result = getPartitionsFromPartitionIds(dbName, tblName, isView, sqlResult);
    }
 
    timingTrace(doTrace, queryText, start, queryTime);
    query.closeAll();
    return result;
  }

  private List<Partition> getPartitionsFromPartitionIds(String dbName, String tblName,
      Boolean isView, List<Object> partIdList) throws MetaException {
    boolean doTrace = LOG.isDebugEnabled();
    int idStringWidth = (int)Math.ceil(Math.log10(partIdList.size())) + 1; // 1 for comma
    int sbCapacity = partIdList.size() * idStringWidth;
    // Prepare StringBuilder for "PART_ID in (...)" to use in future queries.
    StringBuilder partSb = new StringBuilder(sbCapacity);
    for (Object partitionId : partIdList) {
      partSb.append(extractSqlLong(partitionId)).append(",");
    }
    String partIds = trimCommaList(partSb);

    // Get most of the fields for the IDs provided.
    // Assume db and table names are the same for all partition, as provided in arguments.
    String queryText =
      "select \"PARTITIONS\".\"PART_ID\", \"SDS\".\"SD_ID\", \"SDS\".\"CD_ID\","
    + " \"SERDES\".\"SERDE_ID\", \"PARTITIONS\".\"CREATE_TIME\","
    + " \"PARTITIONS\".\"LAST_ACCESS_TIME\", \"SDS\".\"INPUT_FORMAT\", \"SDS\".\"IS_COMPRESSED\","
    + " \"SDS\".\"IS_STOREDASSUBDIRECTORIES\", \"SDS\".\"LOCATION\", \"SDS\".\"NUM_BUCKETS\","
    + " \"SDS\".\"OUTPUT_FORMAT\", \"SERDES\".\"NAME\", \"SERDES\".\"SLIB\" "
    + "from \"PARTITIONS\""
    + "  left outer join \"SDS\" on \"PARTITIONS\".\"SD_ID\" = \"SDS\".\"SD_ID\" "
    + "  left outer join \"SERDES\" on \"SDS\".\"SERDE_ID\" = \"SERDES\".\"SERDE_ID\" "
    + "where \"PART_ID\" in (" + partIds + ") order by \"PART_NAME\" asc";
    long start = doTrace ? System.nanoTime() : 0;
    Query query = pm.newQuery("javax.jdo.query.SQL", queryText);
    @SuppressWarnings("unchecked")
    List<Object[]> sqlResult = executeWithArray(query, null, queryText);
    long queryTime = doTrace ? System.nanoTime() : 0;

    // Read all the fields and create partitions, SDs and serdes.
    TreeMap<Long, Partition> partitions = new TreeMap<Long, Partition>();
    TreeMap<Long, StorageDescriptor> sds = new TreeMap<Long, StorageDescriptor>();
    TreeMap<Long, SerDeInfo> serdes = new TreeMap<Long, SerDeInfo>();
    TreeMap<Long, List<FieldSchema>> colss = new TreeMap<Long, List<FieldSchema>>();
    // Keep order by name, consistent with JDO.
    ArrayList<Partition> orderedResult = new ArrayList<Partition>(partIdList.size());

    // Prepare StringBuilder-s for "in (...)" lists to use in one-to-many queries.
    StringBuilder sdSb = new StringBuilder(sbCapacity), serdeSb = new StringBuilder(sbCapacity);
    StringBuilder colsSb = new StringBuilder(7); // We expect that there's only one field schema.
    tblName = tblName.toLowerCase();
    dbName = dbName.toLowerCase();
    for (Object[] fields : sqlResult) {
      // Here comes the ugly part...
      long partitionId = extractSqlLong(fields[0]);
      Long sdId = extractSqlLong(fields[1]);
      Long colId = extractSqlLong(fields[2]);
      Long serdeId = extractSqlLong(fields[3]);
      // A partition must have either everything set, or nothing set if it's a view.
      if (sdId == null || colId == null || serdeId == null) {
        if (isView == null) {
          isView = isViewTable(dbName, tblName);
        }
        if ((sdId != null || colId != null || serdeId != null) || !isView) {
          throw new MetaException("Unexpected null for one of the IDs, SD " + sdId + ", column "
              + colId + ", serde " + serdeId + " for a " + (isView ? "" : "non-") + " view");
        }
      }

      Partition part = new Partition();
      orderedResult.add(part);
      // Set the collection fields; some code might not check presence before accessing them.
      part.setParameters(new HashMap<String, String>());
      part.setValues(new ArrayList<String>());
      part.setDbName(dbName);
      part.setTableName(tblName);
      if (fields[4] != null) part.setCreateTime(extractSqlInt(fields[4]));
      if (fields[5] != null) part.setLastAccessTime(extractSqlInt(fields[5]));
      partitions.put(partitionId, part);

      if (sdId == null) continue; // Probably a view.
      assert colId != null && serdeId != null;

      // We assume each partition has an unique SD.
      StorageDescriptor sd = new StorageDescriptor();
      StorageDescriptor oldSd = sds.put(sdId, sd);
      if (oldSd != null) {
        throw new MetaException("Partitions reuse SDs; we don't expect that");
      }
      // Set the collection fields; some code might not check presence before accessing them.
      sd.setSortCols(new ArrayList<Order>());
      sd.setBucketCols(new ArrayList<String>());
      sd.setParameters(new HashMap<String, String>());
      sd.setSkewedInfo(new SkewedInfo(new ArrayList<String>(),
          new ArrayList<List<String>>(), new HashMap<List<String>, String>()));
      sd.setInputFormat((String)fields[6]);
      Boolean tmpBoolean = extractSqlBoolean(fields[7]);
      if (tmpBoolean != null) sd.setCompressed(tmpBoolean);
      tmpBoolean = extractSqlBoolean(fields[8]);
      if (tmpBoolean != null) sd.setStoredAsSubDirectories(tmpBoolean);
      sd.setLocation((String)fields[9]);
      if (fields[10] != null) sd.setNumBuckets(extractSqlInt(fields[10]));
      sd.setOutputFormat((String)fields[11]);
      sdSb.append(sdId).append(",");
      part.setSd(sd);

      List<FieldSchema> cols = colss.get(colId);
      // We expect that colId will be the same for all (or many) SDs.
      if (cols == null) {
        cols = new ArrayList<FieldSchema>();
        colss.put(colId, cols);
        colsSb.append(colId).append(",");
      }
      sd.setCols(cols);

      // We assume each SD has an unique serde.
      SerDeInfo serde = new SerDeInfo();
      SerDeInfo oldSerde = serdes.put(serdeId, serde);
      if (oldSerde != null) {
        throw new MetaException("SDs reuse serdes; we don't expect that");
      }
      serde.setParameters(new HashMap<String, String>());
      serde.setName((String)fields[12]);
      serde.setSerializationLib((String)fields[13]);
      serdeSb.append(serdeId).append(",");
      sd.setSerdeInfo(serde);
    }
    query.closeAll();
    timingTrace(doTrace, queryText, start, queryTime);

    // Now get all the one-to-many things. Start with partitions.
    queryText = "select \"PART_ID\", \"PARAM_KEY\", \"PARAM_VALUE\" from \"PARTITION_PARAMS\""
        + " where \"PART_ID\" in (" + partIds + ") and \"PARAM_KEY\" is not null"
        + " order by \"PART_ID\" asc";
    loopJoinOrderedResult(partitions, queryText, 0, new ApplyFunc<Partition>() {
      @Override
      public void apply(Partition t, Object[] fields) {
        t.putToParameters((String)fields[1], (String)fields[2]);
      }});

    queryText = "select \"PART_ID\", \"PART_KEY_VAL\" from \"PARTITION_KEY_VALS\""
        + " where \"PART_ID\" in (" + partIds + ") and \"INTEGER_IDX\" >= 0"
        + " order by \"PART_ID\" asc, \"INTEGER_IDX\" asc";
    loopJoinOrderedResult(partitions, queryText, 0, new ApplyFunc<Partition>() {
      @Override
      public void apply(Partition t, Object[] fields) {
        t.addToValues((String)fields[1]);
      }});

    // Prepare IN (blah) lists for the following queries. Cut off the final ','s.
    if (sdSb.length() == 0) {
      assert serdeSb.length() == 0 && colsSb.length() == 0;
      return orderedResult; // No SDs, probably a view.
    }
    String sdIds = trimCommaList(sdSb), serdeIds = trimCommaList(serdeSb),
        colIds = trimCommaList(colsSb);

    // Get all the stuff for SD. Don't do empty-list check - we expect partitions do have SDs.
    queryText = "select \"SD_ID\", \"PARAM_KEY\", \"PARAM_VALUE\" from \"SD_PARAMS\""
        + " where \"SD_ID\" in (" + sdIds + ") and \"PARAM_KEY\" is not null"
        + " order by \"SD_ID\" asc";
    loopJoinOrderedResult(sds, queryText, 0, new ApplyFunc<StorageDescriptor>() {
      @Override
      public void apply(StorageDescriptor t, Object[] fields) {
        t.putToParameters((String)fields[1], (String)fields[2]);
      }});

    queryText = "select \"SD_ID\", \"COLUMN_NAME\", \"SORT_COLS\".\"ORDER\" from \"SORT_COLS\""
        + " where \"SD_ID\" in (" + sdIds + ") and \"INTEGER_IDX\" >= 0"
        + " order by \"SD_ID\" asc, \"INTEGER_IDX\" asc";
    loopJoinOrderedResult(sds, queryText, 0, new ApplyFunc<StorageDescriptor>() {
      @Override
      public void apply(StorageDescriptor t, Object[] fields) {
        if (fields[2] == null) return;
        t.addToSortCols(new Order((String)fields[1], extractSqlInt(fields[2])));
      }});

    queryText = "select \"SD_ID\", \"BUCKET_COL_NAME\" from \"BUCKETING_COLS\""
        + " where \"SD_ID\" in (" + sdIds + ") and \"INTEGER_IDX\" >= 0"
        + " order by \"SD_ID\" asc, \"INTEGER_IDX\" asc";
    loopJoinOrderedResult(sds, queryText, 0, new ApplyFunc<StorageDescriptor>() {
      @Override
      public void apply(StorageDescriptor t, Object[] fields) {
        t.addToBucketCols((String)fields[1]);
      }});

    // Skewed columns stuff.
    queryText = "select \"SD_ID\", \"SKEWED_COL_NAME\" from \"SKEWED_COL_NAMES\""
        + " where \"SD_ID\" in (" + sdIds + ") and \"INTEGER_IDX\" >= 0"
        + " order by \"SD_ID\" asc, \"INTEGER_IDX\" asc";
    boolean hasSkewedColumns =
      loopJoinOrderedResult(sds, queryText, 0, new ApplyFunc<StorageDescriptor>() {
        @Override
        public void apply(StorageDescriptor t, Object[] fields) {
          if (!t.isSetSkewedInfo()) t.setSkewedInfo(new SkewedInfo());
          t.getSkewedInfo().addToSkewedColNames((String)fields[1]);
        }}) > 0;

    // Assume we don't need to fetch the rest of the skewed column data if we have no columns.
    if (hasSkewedColumns) {
      // We are skipping the SKEWED_STRING_LIST table here, as it seems to be totally useless.
      queryText =
            "select \"SKEWED_VALUES\".\"SD_ID_OID\","
          + "  \"SKEWED_STRING_LIST_VALUES\".\"STRING_LIST_ID\","
          + "  \"SKEWED_STRING_LIST_VALUES\".\"STRING_LIST_VALUE\" "
          + "from \"SKEWED_VALUES\" "
          + "  left outer join \"SKEWED_STRING_LIST_VALUES\" on \"SKEWED_VALUES\"."
          + "\"STRING_LIST_ID_EID\" = \"SKEWED_STRING_LIST_VALUES\".\"STRING_LIST_ID\" "
          + "where \"SKEWED_VALUES\".\"SD_ID_OID\" in (" + sdIds + ") "
          + "  and \"SKEWED_VALUES\".\"STRING_LIST_ID_EID\" is not null "
          + "  and \"SKEWED_VALUES\".\"INTEGER_IDX\" >= 0 "
          + "order by \"SKEWED_VALUES\".\"SD_ID_OID\" asc, \"SKEWED_VALUES\".\"INTEGER_IDX\" asc,"
          + "  \"SKEWED_STRING_LIST_VALUES\".\"INTEGER_IDX\" asc";
      loopJoinOrderedResult(sds, queryText, 0, new ApplyFunc<StorageDescriptor>() {
        private Long currentListId;
        private List<String> currentList;
        @Override
        public void apply(StorageDescriptor t, Object[] fields) throws MetaException {
          if (!t.isSetSkewedInfo()) t.setSkewedInfo(new SkewedInfo());
          // Note that this is not a typical list accumulator - there's no call to finalize
          // the last list. Instead we add list to SD first, as well as locally to add elements.
          if (fields[1] == null) {
            currentList = null; // left outer join produced a list with no values
            currentListId = null;
            t.getSkewedInfo().addToSkewedColValues(new ArrayList<String>());
          } else {
            long fieldsListId = extractSqlLong(fields[1]);
            if (currentListId == null || fieldsListId != currentListId) {
              currentList = new ArrayList<String>();
              currentListId = fieldsListId;
              t.getSkewedInfo().addToSkewedColValues(currentList);
            }
            currentList.add((String)fields[2]);
          }
        }});

      // We are skipping the SKEWED_STRING_LIST table here, as it seems to be totally useless.
      queryText =
            "select \"SKEWED_COL_VALUE_LOC_MAP\".\"SD_ID\","
          + " \"SKEWED_STRING_LIST_VALUES\".STRING_LIST_ID,"
          + " \"SKEWED_COL_VALUE_LOC_MAP\".\"LOCATION\","
          + " \"SKEWED_STRING_LIST_VALUES\".\"STRING_LIST_VALUE\" "
          + "from \"SKEWED_COL_VALUE_LOC_MAP\""
          + "  left outer join \"SKEWED_STRING_LIST_VALUES\" on \"SKEWED_COL_VALUE_LOC_MAP\"."
          + "\"STRING_LIST_ID_KID\" = \"SKEWED_STRING_LIST_VALUES\".\"STRING_LIST_ID\" "
          + "where \"SKEWED_COL_VALUE_LOC_MAP\".\"SD_ID\" in (" + sdIds + ")"
          + "  and \"SKEWED_COL_VALUE_LOC_MAP\".\"STRING_LIST_ID_KID\" is not null "
          + "order by \"SKEWED_COL_VALUE_LOC_MAP\".\"SD_ID\" asc,"
          + "  \"SKEWED_STRING_LIST_VALUES\".\"STRING_LIST_ID\" asc,"
          + "  \"SKEWED_STRING_LIST_VALUES\".\"INTEGER_IDX\" asc";

      loopJoinOrderedResult(sds, queryText, 0, new ApplyFunc<StorageDescriptor>() {
        private Long currentListId;
        private List<String> currentList;
        @Override
        public void apply(StorageDescriptor t, Object[] fields) throws MetaException {
          if (!t.isSetSkewedInfo()) {
            SkewedInfo skewedInfo = new SkewedInfo();
            skewedInfo.setSkewedColValueLocationMaps(new HashMap<List<String>, String>());
            t.setSkewedInfo(skewedInfo);
          }
          Map<List<String>, String> skewMap = t.getSkewedInfo().getSkewedColValueLocationMaps();
          // Note that this is not a typical list accumulator - there's no call to finalize
          // the last list. Instead we add list to SD first, as well as locally to add elements.
          if (fields[1] == null) {
            currentList = new ArrayList<String>(); // left outer join produced a list with no values
            currentListId = null;
          } else {
            long fieldsListId = extractSqlLong(fields[1]);
            if (currentListId == null || fieldsListId != currentListId) {
              currentList = new ArrayList<String>();
              currentListId = fieldsListId;
            } else {
              skewMap.remove(currentList); // value based compare.. remove first
            }
            currentList.add((String)fields[3]);
          }
          skewMap.put(currentList, (String)fields[2]);
        }});
    } // if (hasSkewedColumns)

    // Get FieldSchema stuff if any.
    if (!colss.isEmpty()) {
      // We are skipping the CDS table here, as it seems to be totally useless.
      queryText = "select \"CD_ID\", \"COMMENT\", \"COLUMN_NAME\", \"TYPE_NAME\""
          + " from \"COLUMNS_V2\" where \"CD_ID\" in (" + colIds + ") and \"INTEGER_IDX\" >= 0"
          + " order by \"CD_ID\" asc, \"INTEGER_IDX\" asc";
      loopJoinOrderedResult(colss, queryText, 0, new ApplyFunc<List<FieldSchema>>() {
        @Override
        public void apply(List<FieldSchema> t, Object[] fields) {
          t.add(new FieldSchema((String)fields[2], (String)fields[3], (String)fields[1]));
        }});
    }

    // Finally, get all the stuff for serdes - just the params.
    queryText = "select \"SERDE_ID\", \"PARAM_KEY\", \"PARAM_VALUE\" from \"SERDE_PARAMS\""
        + " where \"SERDE_ID\" in (" + serdeIds + ") and \"PARAM_KEY\" is not null"
        + " order by \"SERDE_ID\" asc";
    loopJoinOrderedResult(serdes, queryText, 0, new ApplyFunc<SerDeInfo>() {
      @Override
      public void apply(SerDeInfo t, Object[] fields) {
        t.putToParameters((String)fields[1], (String)fields[2]);
      }});

    return orderedResult;
  }

  private void timingTrace(boolean doTrace, String queryText, long start, long queryTime) {
    if (!doTrace) return;
    LOG.debug("Direct SQL query in " + (queryTime - start) / 1000000.0 + "ms + " +
        (System.nanoTime() - queryTime) / 1000000.0 + "ms, the query is [" + queryText + "]");
  }

  static Long extractSqlLong(Object obj) throws MetaException {
    if (obj == null) return null;
    if (!(obj instanceof Number)) {
      throw new MetaException("Expected numeric type but got " + obj.getClass().getName());
    }
    return ((Number)obj).longValue();
  }

  private static Boolean extractSqlBoolean(Object value) throws MetaException {
    // MySQL has booleans, but e.g. Derby uses 'Y'/'N' mapping. People using derby probably
    // don't care about performance anyway, but let's cover the common case.
    if (value == null) return null;
    if (value instanceof Boolean) return (Boolean)value;
    Character c = null;
    if (value instanceof String && ((String)value).length() == 1) {
      c = ((String)value).charAt(0);
    }
    if (c == null) return null;
    if (c == 'Y') return true;
    if (c == 'N') return false;
    throw new MetaException("Cannot extract boolean from column value " + value);
  }

  private int extractSqlInt(Object field) {
    return ((Number)field).intValue();
  }

  private String extractSqlString(Object value) {
    if (value == null) return null;
    return value.toString();
  }

  private static String trimCommaList(StringBuilder sb) {
    if (sb.length() > 0) {
      sb.setLength(sb.length() - 1);
    }
    return sb.toString();
  }

  private abstract class ApplyFunc<Target> {
    public abstract void apply(Target t, Object[] fields) throws MetaException;
  }

  /**
   * Merges applies the result of a PM SQL query into a tree of object.
   * Essentially it's an object join. DN could do this for us, but it issues queries
   * separately for every object, which is suboptimal.
   * @param tree The object tree, by ID.
   * @param queryText The query text.
   * @param keyIndex Index of the Long column corresponding to the map ID in query result rows.
   * @param func The function that is called on each (object,row) pair with the same id.
   * @return the count of results returned from the query.
   */
  private <T> int loopJoinOrderedResult(TreeMap<Long, T> tree,
      String queryText, int keyIndex, ApplyFunc<T> func) throws MetaException {
    boolean doTrace = LOG.isDebugEnabled();
    long start = doTrace ? System.nanoTime() : 0;
    Query query = pm.newQuery("javax.jdo.query.SQL", queryText);
    Object result = query.execute();
    long queryTime = doTrace ? System.nanoTime() : 0;
    if (result == null) {
      query.closeAll();
      return 0;
    }
    List<Object[]> list = ensureList(result);
    Iterator<Object[]> iter = list.iterator();
    Object[] fields = null;
    for (Map.Entry<Long, T> entry : tree.entrySet()) {
      if (fields == null && !iter.hasNext()) break;
      long id = entry.getKey();
      while (fields != null || iter.hasNext()) {
        if (fields == null) {
          fields = iter.next();
        }
        long nestedId = extractSqlLong(fields[keyIndex]);
        if (nestedId < id) throw new MetaException("Found entries for unknown ID " + nestedId);
        if (nestedId > id) break; // fields belong to one of the next entries
        func.apply(entry.getValue(), fields);
        fields = null;
      }
    }
    int rv = list.size();
    query.closeAll();
    timingTrace(doTrace, queryText, start, queryTime);
    return rv;
  }

  private static class PartitionFilterGenerator extends TreeVisitor {
    private final Table table;
    private final FilterBuilder filterBuffer;
    private final List<Object> params;
    private final List<String> joins;
    private final boolean dbHasJoinCastBug;

    private PartitionFilterGenerator(
        Table table, List<Object> params, List<String> joins, boolean dbHasJoinCastBug) {
      this.table = table;
      this.params = params;
      this.joins = joins;
      this.dbHasJoinCastBug = dbHasJoinCastBug;
      this.filterBuffer = new FilterBuilder(false);
    }

    /**
     * Generate the ANSI SQL92 filter for the given expression tree
     * @param table the table being queried
     * @param params the ordered parameters for the resulting expression
     * @param joins the joins necessary for the resulting expression
     * @return the string representation of the expression tree
     */
    private static String generateSqlFilter(Table table, ExpressionTree tree,
        List<Object> params, List<String> joins, boolean dbHasJoinCastBug) throws MetaException {
      assert table != null;
      if (tree.getRoot() == null) {
        return "";
      }
      PartitionFilterGenerator visitor = new PartitionFilterGenerator(
          table, params, joins, dbHasJoinCastBug);
      tree.accept(visitor);
      if (visitor.filterBuffer.hasError()) {
        LOG.info("Unable to push down SQL filter: " + visitor.filterBuffer.getErrorMessage());
        return null;
      }

      // Some joins might be null (see processNode for LeafNode), clean them up.
      for (int i = 0; i < joins.size(); ++i) {
        if (joins.get(i) != null) continue;
        joins.remove(i--);
      }
      return "(" + visitor.filterBuffer.getFilter() + ")";
    }

    @Override
    protected void beginTreeNode(TreeNode node) throws MetaException {
      filterBuffer.append(" (");
    }

    @Override
    protected void midTreeNode(TreeNode node) throws MetaException {
      filterBuffer.append((node.getAndOr() == LogicalOperator.AND) ? " and " : " or ");
    }

    @Override
    protected void endTreeNode(TreeNode node) throws MetaException {
      filterBuffer.append(") ");
    }

    @Override
    protected boolean shouldStop() {
      return filterBuffer.hasError();
    }

    private static enum FilterType {
      Integral,
      String,
      Date,

      Invalid;

      static FilterType fromType(String colTypeStr) {
        if (colTypeStr.equals(serdeConstants.STRING_TYPE_NAME)) {
          return FilterType.String;
        } else if (colTypeStr.equals(serdeConstants.DATE_TYPE_NAME)) {
          return FilterType.Date;
        } else if (serdeConstants.IntegralTypes.contains(colTypeStr)) {
          return FilterType.Integral;
        }
        return FilterType.Invalid;
      }

      public static FilterType fromClass(Object value) {
        if (value instanceof String) {
          return FilterType.String;
        } else if (value instanceof Long) {
          return FilterType.Integral;
        } else if (value instanceof java.sql.Date) {
          return FilterType.Date;
        }
        return FilterType.Invalid;
      }
    }

    @Override
    public void visit(LeafNode node) throws MetaException {
      if (node.operator == Operator.LIKE) {
        filterBuffer.setError("LIKE is not supported for SQL filter pushdown");
        return;
      }
      int partColCount = table.getPartitionKeys().size();
      int partColIndex = node.getPartColIndexForFilter(table, filterBuffer);
      if (filterBuffer.hasError()) return;

      // We skipped 'like', other ops should all work as long as the types are right.
      String colTypeStr = table.getPartitionKeys().get(partColIndex).getType();
      FilterType colType = FilterType.fromType(colTypeStr);
      if (colType == FilterType.Invalid) {
        filterBuffer.setError("Filter pushdown not supported for type " + colTypeStr);
        return;
      }
      FilterType valType = FilterType.fromClass(node.value);
      Object nodeValue = node.value;
      if (valType == FilterType.Invalid) {
        filterBuffer.setError("Filter pushdown not supported for value " + node.value.getClass());
        return;
      }

      // TODO: if Filter.g does date parsing for quoted strings, we'd need to verify there's no
      //       type mismatch when string col is filtered by a string that looks like date.
      if (colType == FilterType.Date && valType == FilterType.String) {
        // TODO: Filter.g cannot parse a quoted date; try to parse date here too.
        try {
          nodeValue = new java.sql.Date(
              HiveMetaStore.PARTITION_DATE_FORMAT.get().parse((String)nodeValue).getTime());
          valType = FilterType.Date;
        } catch (ParseException pe) { // do nothing, handled below - types will mismatch
        }
      }

      if (colType != valType) {
        // It's not clear how filtering for e.g. "stringCol > 5" should work (which side is
        // to be coerced?). Let the expression evaluation sort this one out, not metastore.
        filterBuffer.setError("Cannot push down filter for "
            + colTypeStr + " column and value " + nodeValue.getClass());
        return;
      }

      if (joins.isEmpty()) {
        // There's a fixed number of partition cols that we might have filters on. To avoid
        // joining multiple times for one column (if there are several filters on it), we will
        // keep numCols elements in the list, one for each column; we will fill it with nulls,
        // put each join at a corresponding index when necessary, and remove nulls in the end.
        for (int i = 0; i < partColCount; ++i) {
          joins.add(null);
        }
      }
      if (joins.get(partColIndex) == null) {
        joins.set(partColIndex, "inner join \"PARTITION_KEY_VALS\" \"FILTER" + partColIndex
            + "\" on \"FILTER"  + partColIndex + "\".\"PART_ID\" = \"PARTITIONS\".\"PART_ID\""
            + " and \"FILTER" + partColIndex + "\".\"INTEGER_IDX\" = " + partColIndex);
      }

      // Build the filter and add parameters linearly; we are traversing leaf nodes LTR.
      String tableValue = "\"FILTER" + partColIndex + "\".\"PART_KEY_VAL\"";
      if (node.isReverseOrder) {
        params.add(nodeValue);
      }
      if (colType != FilterType.String) {
        // The underlying database field is varchar, we need to compare numbers.
        // Note that this won't work with __HIVE_DEFAULT_PARTITION__. It will fail and fall
        // back to JDO. That is by design; we could add an ugly workaround here but didn't.
        if (colType == FilterType.Integral) {
          tableValue = "cast(" + tableValue + " as decimal(21,0))";
        } else if (colType == FilterType.Date) {
          tableValue = "cast(" + tableValue + " as date)";
        }

        if (dbHasJoinCastBug) {
          // This is a workaround for DERBY-6358 and Oracle bug; it is pretty horrible.
          tableValue = "(case when \"TBLS\".\"TBL_NAME\" = ? and \"DBS\".\"NAME\" = ? and "
              + "\"FILTER" + partColIndex + "\".\"PART_ID\" = \"PARTITIONS\".\"PART_ID\" and "
                + "\"FILTER" + partColIndex + "\".\"INTEGER_IDX\" = " + partColIndex + " then "
              + tableValue + " else null end)";
          params.add(table.getTableName().toLowerCase());
          params.add(table.getDbName().toLowerCase());
        }
      }
      if (!node.isReverseOrder) {
        params.add(nodeValue);
      }

      filterBuffer.append(node.isReverseOrder
          ? "(? " + node.operator.getSqlOp() + " " + tableValue + ")"
          : "(" + tableValue + " " + node.operator.getSqlOp() + " ?)");
    }
  }

  public ColumnStatistics getTableStats(
      String dbName, String tableName, List<String> colNames) throws MetaException {
    if (colNames.isEmpty()) {
      return null;
    }
    boolean doTrace = LOG.isDebugEnabled();
    long start = doTrace ? System.nanoTime() : 0;
    String queryText = "select " + STATS_COLLIST + " from \"TAB_COL_STATS\" "
      + " where \"DB_NAME\" = ? and \"TABLE_NAME\" = ? and \"COLUMN_NAME\" in ("
      + makeParams(colNames.size()) + ")";
    Query query = pm.newQuery("javax.jdo.query.SQL", queryText);
    Object[] params = new Object[colNames.size() + 2];
    params[0] = dbName;
    params[1] = tableName;
    for (int i = 0; i < colNames.size(); ++i) {
      params[i + 2] = colNames.get(i);
    }
    Object qResult = executeWithArray(query, params, queryText);
    long queryTime = doTrace ? System.nanoTime() : 0;
    if (qResult == null) {
      query.closeAll();
      return null;
    }
    List<Object[]> list = ensureList(qResult);
    if (list.isEmpty()) return null;
    ColumnStatisticsDesc csd = new ColumnStatisticsDesc(true, dbName, tableName);
    ColumnStatistics result = makeColumnStats(list, csd, 0);
    timingTrace(doTrace, queryText, start, queryTime);
    query.closeAll();
    return result;
  }

  public AggrStats aggrColStatsForPartitions(String dbName, String tableName,
      List<String> partNames, List<String> colNames) throws MetaException {
    long partsFound = partsFoundForPartitions(dbName, tableName, partNames, colNames);
    List<ColumnStatisticsObj> stats = columnStatisticsObjForPartitions(dbName,
        tableName, partNames, colNames, partsFound);
    return new AggrStats(stats, partsFound);
  }

  private long partsFoundForPartitions(String dbName, String tableName,
      List<String> partNames, List<String> colNames) throws MetaException {
    long partsFound = 0;
    boolean doTrace = LOG.isDebugEnabled();
    String queryText = "select count(\"COLUMN_NAME\") from \"PART_COL_STATS\""
        + " where \"DB_NAME\" = ? and \"TABLE_NAME\" = ? "
        + " and \"COLUMN_NAME\" in (" + makeParams(colNames.size()) + ")"
        + " and \"PARTITION_NAME\" in (" + makeParams(partNames.size()) + ")"
        + " group by \"PARTITION_NAME\"";
    long start = doTrace ? System.nanoTime() : 0;
    Query query = pm.newQuery("javax.jdo.query.SQL", queryText);
    Object qResult = executeWithArray(query, prepareParams(
        dbName, tableName, partNames, colNames), queryText);
    long end = doTrace ? System.nanoTime() : 0;
    timingTrace(doTrace, queryText, start, end);
    ForwardQueryResult fqr = (ForwardQueryResult) qResult;
    Iterator<?> iter = fqr.iterator();
    while (iter.hasNext()) {
      if (extractSqlLong(iter.next()) == colNames.size()) {
        partsFound++;
      }
    }
    return partsFound;
  }

  private List<ColumnStatisticsObj> columnStatisticsObjForPartitions(
      String dbName, String tableName, List<String> partNames,
      List<String> colNames, long partsFound) throws MetaException {
    // TODO: all the extrapolation logic should be moved out of this class,
    //       only mechanical data retrieval should remain here.
    String commonPrefix = "select \"COLUMN_NAME\", \"COLUMN_TYPE\", "
        + "min(\"LONG_LOW_VALUE\"), max(\"LONG_HIGH_VALUE\"), min(\"DOUBLE_LOW_VALUE\"), max(\"DOUBLE_HIGH_VALUE\"), "
        + "min(\"BIG_DECIMAL_LOW_VALUE\"), max(\"BIG_DECIMAL_HIGH_VALUE\"), sum(\"NUM_NULLS\"), max(\"NUM_DISTINCTS\"), "
        + "max(\"AVG_COL_LEN\"), max(\"MAX_COL_LEN\"), sum(\"NUM_TRUES\"), sum(\"NUM_FALSES\") from \"PART_COL_STATS\""
        + " where \"DB_NAME\" = ? and \"TABLE_NAME\" = ? ";
    String queryText = null;
    long start = 0;
    long end = 0;
    Query query = null;
    boolean doTrace = LOG.isDebugEnabled();
    Object qResult = null;
    ForwardQueryResult fqr = null;
    // Check if the status of all the columns of all the partitions exists
    // Extrapolation is not needed.
    if (partsFound == partNames.size()) {
      queryText = commonPrefix
          + " and \"COLUMN_NAME\" in (" + makeParams(colNames.size()) + ")"
          + " and \"PARTITION_NAME\" in (" + makeParams(partNames.size()) + ")"
          + " group by \"COLUMN_NAME\", \"COLUMN_TYPE\"";
      start = doTrace ? System.nanoTime() : 0;
      query = pm.newQuery("javax.jdo.query.SQL", queryText);
      qResult = executeWithArray(query, prepareParams(
          dbName, tableName, partNames, colNames), queryText);
      if (qResult == null) {
        query.closeAll();
        return Lists.newArrayList();
      }
      end = doTrace ? System.nanoTime() : 0;
      timingTrace(doTrace, queryText, start, end);
      List<Object[]> list = ensureList(qResult);
      List<ColumnStatisticsObj> colStats = new ArrayList<ColumnStatisticsObj>(
          list.size());
      for (Object[] row : list) {
        colStats.add(prepareCSObj(row, 0));
      }
      query.closeAll();
      return colStats;
    } else {
      // Extrapolation is needed for some columns.
      // In this case, at least a column status for a partition is missing.
      // We need to extrapolate this partition based on the other partitions
      List<ColumnStatisticsObj> colStats = new ArrayList<ColumnStatisticsObj>(
          colNames.size());
      queryText = "select \"COLUMN_NAME\", \"COLUMN_TYPE\", count(\"PARTITION_NAME\") "
          + " from \"PART_COL_STATS\""
          + " where \"DB_NAME\" = ? and \"TABLE_NAME\" = ? "
          + " and \"COLUMN_NAME\" in (" + makeParams(colNames.size()) + ")"
          + " and \"PARTITION_NAME\" in (" + makeParams(partNames.size()) + ")"
          + " group by \"COLUMN_NAME\", \"COLUMN_TYPE\"";
      start = doTrace ? System.nanoTime() : 0;
      query = pm.newQuery("javax.jdo.query.SQL", queryText);
      qResult = executeWithArray(query, prepareParams(
          dbName, tableName, partNames, colNames), queryText);
      end = doTrace ? System.nanoTime() : 0;
      timingTrace(doTrace, queryText, start, end);
      if (qResult == null) {
        query.closeAll();
        return Lists.newArrayList();
      }
      List<String> noExtraColumnNames = new ArrayList<String>();
      Map<String, String[]> extraColumnNameTypeParts = new HashMap<String, String[]>();
      List<Object[]> list = ensureList(qResult);
      for (Object[] row : list) {
        String colName = (String) row[0];
        String colType = (String) row[1];
        // Extrapolation is not needed for this column if
        // count(\"PARTITION_NAME\")==partNames.size()
        // Or, extrapolation is not possible for this column if
        // count(\"PARTITION_NAME\")<2
        Long count = extractSqlLong(row[2]);
        if (count == partNames.size() || count < 2) {
          noExtraColumnNames.add(colName);
        } else {
          extraColumnNameTypeParts.put(colName, new String[] { colType, String.valueOf(count) });
        }
      }
      query.closeAll();
      // Extrapolation is not needed for columns noExtraColumnNames
      if (noExtraColumnNames.size() != 0) {
        queryText = commonPrefix
            + " and \"COLUMN_NAME\" in ("+ makeParams(noExtraColumnNames.size()) + ")"
            + " and \"PARTITION_NAME\" in ("+ makeParams(partNames.size()) +")"
            + " group by \"COLUMN_NAME\", \"COLUMN_TYPE\"";
        start = doTrace ? System.nanoTime() : 0;
        query = pm.newQuery("javax.jdo.query.SQL", queryText);
        qResult = executeWithArray(query, prepareParams(
            dbName, tableName, partNames, noExtraColumnNames), queryText);
        if (qResult == null) {
          query.closeAll();
          return Lists.newArrayList();
        }
        list = ensureList(qResult);
        for (Object[] row : list) {
          colStats.add(prepareCSObj(row, 0));
        }
        end = doTrace ? System.nanoTime() : 0;
        timingTrace(doTrace, queryText, start, end);
        query.closeAll();
      }
      // Extrapolation is needed for extraColumnNames.
      // give a sequence number for all the partitions
      if (extraColumnNameTypeParts.size() != 0) {
        Map<String, Integer> indexMap = new HashMap<String, Integer>();
        for (int index = 0; index < partNames.size(); index++) {
          indexMap.put(partNames.get(index), index);
        }
        // get sum for all columns to reduce the number of queries
        Map<String, Map<Integer, Object>> sumMap = new HashMap<String, Map<Integer, Object>>();
        queryText = "select \"COLUMN_NAME\", sum(\"NUM_NULLS\"), sum(\"NUM_TRUES\"), sum(\"NUM_FALSES\")"
            + " from \"PART_COL_STATS\""
            + " where \"DB_NAME\" = ? and \"TABLE_NAME\" = ? "
            + " and \"COLUMN_NAME\" in (" +makeParams(extraColumnNameTypeParts.size())+ ")"
            + " and \"PARTITION_NAME\" in (" + makeParams(partNames.size()) + ")"
            + " group by \"COLUMN_NAME\"";
        start = doTrace ? System.nanoTime() : 0;
        query = pm.newQuery("javax.jdo.query.SQL", queryText);
        List<String> extraColumnNames = new ArrayList<String>();
        extraColumnNames.addAll(extraColumnNameTypeParts.keySet());
        qResult = executeWithArray(query, prepareParams(
            dbName, tableName, partNames, extraColumnNames), queryText);
        if (qResult == null) {
          query.closeAll();
          return Lists.newArrayList();
        }
        list = ensureList(qResult);
        // see the indexes for colstats in IExtrapolatePartStatus
        Integer[] sumIndex = new Integer[] { 6, 10, 11 };
        for (Object[] row : list) {
          Map<Integer, Object> indexToObject = new HashMap<Integer, Object>();
          for (int ind = 1; ind < row.length; ind++) {
            indexToObject.put(sumIndex[ind - 1], row[ind]);
          }
          sumMap.put((String) row[0], indexToObject);
        }
        end = doTrace ? System.nanoTime() : 0;
        timingTrace(doTrace, queryText, start, end);
        query.closeAll();
        for (Map.Entry<String, String[]> entry : extraColumnNameTypeParts
            .entrySet()) {
          Object[] row = new Object[IExtrapolatePartStatus.colStatNames.length + 2];
          String colName = entry.getKey();
          String colType = entry.getValue()[0];
          Long sumVal = Long.parseLong(entry.getValue()[1]);
          // fill in colname
          row[0] = colName;
          // fill in coltype
          row[1] = colType;
          // use linear extrapolation. more complicated one can be added in the future.
          IExtrapolatePartStatus extrapolateMethod = new LinearExtrapolatePartStatus();
          // fill in colstatus
          Integer[] index = IExtrapolatePartStatus.indexMaps.get(colType
              .toLowerCase());
          //if the colType is not the known type, long, double, etc, then get all index.
          if (index == null) {
            index = IExtrapolatePartStatus.indexMaps.get("default");
          }
          for (int colStatIndex : index) {
            String colStatName = IExtrapolatePartStatus.colStatNames[colStatIndex];
            // if the aggregation type is sum, we do a scale-up
            if (IExtrapolatePartStatus.aggrTypes[colStatIndex] == IExtrapolatePartStatus.AggrType.Sum) {
              Object o = sumMap.get(colName).get(colStatIndex);
              if (o == null) {
                row[2 + colStatIndex] = null;
              } else {
                Long val = extractSqlLong(o);
                row[2 + colStatIndex] = (Long) (val / sumVal * (partNames.size()));
              }
            } else {
              // if the aggregation type is min/max, we extrapolate from the
              // left/right borders
              queryText = "select \""
                  + colStatName
                  + "\",\"PARTITION_NAME\" from \"PART_COL_STATS\""
                  + " where \"DB_NAME\" = ? and \"TABLE_NAME\" = ?"
                  + " and \"COLUMN_NAME\" = ?"
                  + " and \"PARTITION_NAME\" in (" + makeParams(partNames.size()) + ")"
                  + " order by \'" + colStatName + "\'";
              start = doTrace ? System.nanoTime() : 0;
              query = pm.newQuery("javax.jdo.query.SQL", queryText);
              qResult = executeWithArray(query, prepareParams(
                  dbName, tableName, partNames, Arrays.asList(colName)), queryText);
              if (qResult == null) {
                query.closeAll();
                return Lists.newArrayList();
              }
              fqr = (ForwardQueryResult) qResult;
              Object[] min = (Object[]) (fqr.get(0));
              Object[] max = (Object[]) (fqr.get(fqr.size() - 1));
              end = doTrace ? System.nanoTime() : 0;
              timingTrace(doTrace, queryText, start, end);
              query.closeAll();
              if (min[0] == null || max[0] == null) {
                row[2 + colStatIndex] = null;
              } else {
                row[2 + colStatIndex] = extrapolateMethod.extrapolate(min, max,
                    colStatIndex, indexMap);
              }
            }
          }
          colStats.add(prepareCSObj(row, 0));
        }
      }
      return colStats;
    }
  }

  private ColumnStatisticsObj prepareCSObj (Object[] row, int i) throws MetaException {
    ColumnStatisticsData data = new ColumnStatisticsData();
    ColumnStatisticsObj cso = new ColumnStatisticsObj((String)row[i++], (String)row[i++], data);
    Object llow = row[i++], lhigh = row[i++], dlow = row[i++], dhigh = row[i++],
        declow = row[i++], dechigh = row[i++], nulls = row[i++], dist = row[i++],
        avglen = row[i++], maxlen = row[i++], trues = row[i++], falses = row[i++];
    StatObjectConverter.fillColumnStatisticsData(cso.getColType(), data,
        llow, lhigh, dlow, dhigh, declow, dechigh, nulls, dist, avglen, maxlen, trues, falses);
    return cso;
  }

  private Object[] prepareParams(String dbName, String tableName, List<String> partNames,
    List<String> colNames) throws MetaException {

    Object[] params = new Object[colNames.size() + partNames.size() + 2];
    int paramI = 0;
    params[paramI++] = dbName;
    params[paramI++] = tableName;
    for (String colName : colNames) {
      params[paramI++] = colName;
    }
    for (String partName : partNames) {
      params[paramI++] = partName;
    }

    return params;
  }
  
  public List<ColumnStatistics> getPartitionStats(String dbName, String tableName,
      List<String> partNames, List<String> colNames) throws MetaException {
    if (colNames.isEmpty() || partNames.isEmpty()) {
      return Lists.newArrayList();
    }
    boolean doTrace = LOG.isDebugEnabled();
    long start = doTrace ? System.nanoTime() : 0;
    String queryText = "select \"PARTITION_NAME\", " + STATS_COLLIST + " from \"PART_COL_STATS\""
      + " where \"DB_NAME\" = ? and \"TABLE_NAME\" = ? and \"COLUMN_NAME\" in ("
      + makeParams(colNames.size()) + ") AND \"PARTITION_NAME\" in ("
      + makeParams(partNames.size()) + ") order by \"PARTITION_NAME\"";

    Query query = pm.newQuery("javax.jdo.query.SQL", queryText);
    Object qResult = executeWithArray(query, prepareParams(
        dbName, tableName, partNames, colNames), queryText);
    long queryTime = doTrace ? System.nanoTime() : 0;
    if (qResult == null) {
      query.closeAll();
      return Lists.newArrayList();
    }
    List<Object[]> list = ensureList(qResult);
    List<ColumnStatistics> result = new ArrayList<ColumnStatistics>(
        Math.min(list.size(), partNames.size()));
    String lastPartName = null;
    int from = 0;
    for (int i = 0; i <= list.size(); ++i) {
      boolean isLast = i == list.size();
      String partName = isLast ? null : (String)list.get(i)[0];
      if (!isLast && partName.equals(lastPartName)) {
        continue;
      } else if (from != i) {
        ColumnStatisticsDesc csd = new ColumnStatisticsDesc(false, dbName, tableName);
        csd.setPartName(lastPartName);
        result.add(makeColumnStats(list.subList(from, i), csd, 1));
      }
      lastPartName = partName;
      from = i;
    }

    timingTrace(doTrace, queryText, start, queryTime);
    query.closeAll();
    return result;
  }

  /** The common query part for table and partition stats */
  private static final String STATS_COLLIST =
      "\"COLUMN_NAME\", \"COLUMN_TYPE\", \"LONG_LOW_VALUE\", \"LONG_HIGH_VALUE\", "
    + "\"DOUBLE_LOW_VALUE\", \"DOUBLE_HIGH_VALUE\", \"BIG_DECIMAL_LOW_VALUE\", "
    + "\"BIG_DECIMAL_HIGH_VALUE\", \"NUM_NULLS\", \"NUM_DISTINCTS\", \"AVG_COL_LEN\", "
    + "\"MAX_COL_LEN\", \"NUM_TRUES\", \"NUM_FALSES\", \"LAST_ANALYZED\" ";

  private ColumnStatistics makeColumnStats(
      List<Object[]> list, ColumnStatisticsDesc csd, int offset) throws MetaException {
    ColumnStatistics result = new ColumnStatistics();
    result.setStatsDesc(csd);
    List<ColumnStatisticsObj> csos = new ArrayList<ColumnStatisticsObj>(list.size());
    for (Object[] row : list) {
      // LastAnalyzed is stored per column but thrift has it per several;
      // get the lowest for now as nobody actually uses this field.
      Object laObj = row[offset + 14];
      if (laObj != null && (!csd.isSetLastAnalyzed() || csd.getLastAnalyzed() > extractSqlLong(laObj))) {
        csd.setLastAnalyzed(extractSqlLong(laObj));
      }
      csos.add(prepareCSObj(row, offset));
    }
    result.setStatsObj(csos);
    return result;
  }

  @SuppressWarnings("unchecked")
  private List<Object[]> ensureList(Object result) throws MetaException {
    if (!(result instanceof List<?>)) {
      throw new MetaException("Wrong result type " + result.getClass());
    }
    return (List<Object[]>)result;
  }

  private String makeParams(int size) {
    // W/ size 0, query will fail, but at least we'd get to see the query in debug output.
    return (size == 0) ? "" : repeat(",?", size).substring(1);
  }

  @SuppressWarnings("unchecked")
  private <T> T executeWithArray(Query query, Object[] params, String sql) throws MetaException {
    try {
      return (T)((params == null) ? query.execute() : query.executeWithArray(params));
    } catch (Exception ex) {
      String error = "Failed to execute [" + sql + "] with parameters [";
      if (params != null) {
        boolean isFirst = true;
        for (Object param : params) {
          error += (isFirst ? "" : ", ") + param;
          isFirst = false;
        }
      }
      LOG.warn(error + "]", ex);
      // We just logged an exception with (in case of JDO) a humongous callstack. Make a new one.
      throw new MetaException("See previous errors; " + ex.getMessage());
    }
  }
}
