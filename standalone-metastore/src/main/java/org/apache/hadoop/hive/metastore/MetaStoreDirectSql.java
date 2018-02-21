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

package org.apache.hadoop.hive.metastore;

import static org.apache.commons.lang.StringUtils.join;
import static org.apache.commons.lang.StringUtils.repeat;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;
import javax.jdo.Transaction;
import javax.jdo.datastore.JDOConnection;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.AggregateStatsCache.AggrColStats;
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
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.model.MConstraint;
import org.apache.hadoop.hive.metastore.model.MCreationMetadata;
import org.apache.hadoop.hive.metastore.model.MDatabase;
import org.apache.hadoop.hive.metastore.model.MNotificationLog;
import org.apache.hadoop.hive.metastore.model.MNotificationNextId;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnStatistics;
import org.apache.hadoop.hive.metastore.model.MTableColumnStatistics;
import org.apache.hadoop.hive.metastore.model.MWMResourcePlan;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.FilterBuilder;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.LeafNode;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.LogicalOperator;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.Operator;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.TreeNode;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.TreeVisitor;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.ColStatsObjWithSourceInfo;
import org.apache.hive.common.util.BloomFilter;
import org.datanucleus.store.rdbms.query.ForwardQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final int NO_BATCHING = -1, DETECT_BATCHING = 0;

  private static final Logger LOG = LoggerFactory.getLogger(MetaStoreDirectSql.class);
  private final PersistenceManager pm;
  private final String schema;

  /**
   * We want to avoid db-specific code in this class and stick with ANSI SQL. However:
   * 1) mysql and postgres are differently ansi-incompatible (mysql by default doesn't support
   * quoted identifiers, and postgres contravenes ANSI by coercing unquoted ones to lower case).
   * MySQL's way of working around this is simpler (just set ansi quotes mode on), so we will
   * use that. MySQL detection is done by actually issuing the set-ansi-quotes command;
   *
   * Use sparingly, we don't want to devolve into another DataNucleus...
   */
  private final DatabaseProduct dbType;
  private final int batchSize;
  private final boolean convertMapNullsToEmptyStrings;
  private final String defaultPartName;

  /**
   * Whether direct SQL can be used with the current datastore backing {@link #pm}.
   */
  private final boolean isCompatibleDatastore;
  private final boolean isAggregateStatsCacheEnabled;
  private AggregateStatsCache aggrStatsCache;

  @java.lang.annotation.Target(java.lang.annotation.ElementType.FIELD)
  @java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
  private @interface TableName {}

  // Table names with schema name, if necessary
  @TableName
  private String DBS, TBLS, PARTITIONS, DATABASE_PARAMS, PARTITION_PARAMS, SORT_COLS, SD_PARAMS,
      SDS, SERDES, SKEWED_STRING_LIST_VALUES, SKEWED_VALUES, BUCKETING_COLS, SKEWED_COL_NAMES,
      SKEWED_COL_VALUE_LOC_MAP, COLUMNS_V2, PARTITION_KEYS, SERDE_PARAMS, PART_COL_STATS, KEY_CONSTRAINTS,
      TAB_COL_STATS, PARTITION_KEY_VALS;

  public MetaStoreDirectSql(PersistenceManager pm, Configuration conf, String schema) {
    this.pm = pm;
    this.schema = schema;
    DatabaseProduct dbType = null;
    try {
      dbType = DatabaseProduct.determineDatabaseProduct(getProductName(pm));
    } catch (SQLException e) {
      LOG.warn("Cannot determine database product; assuming OTHER", e);
      dbType = DatabaseProduct.OTHER;
    }
    this.dbType = dbType;
    int batchSize = MetastoreConf.getIntVar(conf, ConfVars.DIRECT_SQL_PARTITION_BATCH_SIZE);
    if (batchSize == DETECT_BATCHING) {
      batchSize = DatabaseProduct.needsInBatching(dbType) ? 1000 : NO_BATCHING;
    }
    this.batchSize = batchSize;

    for (java.lang.reflect.Field f : this.getClass().getDeclaredFields()) {
      if (f.getAnnotation(TableName.class) == null) continue;
      try {
        f.set(this, getFullyQualifiedName(schema, f.getName()));
      } catch (IllegalArgumentException | IllegalAccessException e) {
        throw new RuntimeException("Internal error, cannot set " + f.getName());
      }
    }

    convertMapNullsToEmptyStrings =
        MetastoreConf.getBoolVar(conf, ConfVars.ORM_RETRIEVE_MAPNULLS_AS_EMPTY_STRINGS);
    defaultPartName = MetastoreConf.getVar(conf, ConfVars.DEFAULTPARTITIONNAME);

    String jdoIdFactory = MetastoreConf.getVar(conf, ConfVars.IDENTIFIER_FACTORY);
    if (! ("datanucleus1".equalsIgnoreCase(jdoIdFactory))){
      LOG.warn("Underlying metastore does not use 'datanucleus1' for its ORM naming scheme."
          + " Disabling directSQL as it uses hand-hardcoded SQL with that assumption.");
      isCompatibleDatastore = false;
    } else {
      isCompatibleDatastore = ensureDbInit() && runTestQuery();
      if (isCompatibleDatastore) {
        LOG.info("Using direct SQL, underlying DB is " + dbType);
      }
    }

    isAggregateStatsCacheEnabled = MetastoreConf.getBoolVar(
        conf, ConfVars.AGGREGATE_STATS_CACHE_ENABLED);
    if (isAggregateStatsCacheEnabled) {
      aggrStatsCache = AggregateStatsCache.getInstance(conf);
    }
  }

  private static String getFullyQualifiedName(String schema, String tblName) {
    return ((schema == null || schema.isEmpty()) ? "" : "\"" + schema + "\".\"")
        + "\"" + tblName + "\"";
  }


  public MetaStoreDirectSql(PersistenceManager pm, Configuration conf) {
    this(pm, conf, "");
  }

  static String getProductName(PersistenceManager pm) {
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
    boolean doCommit = false;
    if (!tx.isActive()) {
      tx.begin();
      doCommit = true;
    }
    LinkedList<Query> initQueries = new LinkedList<>();

    try {
      // Force the underlying db to initialize.
      initQueries.add(pm.newQuery(MDatabase.class, "name == ''"));
      initQueries.add(pm.newQuery(MTableColumnStatistics.class, "dbName == ''"));
      initQueries.add(pm.newQuery(MPartitionColumnStatistics.class, "dbName == ''"));
      initQueries.add(pm.newQuery(MConstraint.class, "childIntegerIndex < 0"));
      initQueries.add(pm.newQuery(MNotificationLog.class, "dbName == ''"));
      initQueries.add(pm.newQuery(MNotificationNextId.class, "nextEventId < -1"));
      initQueries.add(pm.newQuery(MWMResourcePlan.class, "name == ''"));
      initQueries.add(pm.newQuery(MCreationMetadata.class, "dbName == ''"));
      Query q;
      while ((q = initQueries.peekFirst()) != null) {
        q.execute();
        initQueries.pollFirst();
      }

      return true;
    } catch (Exception ex) {
      doCommit = false;
      LOG.warn("Database initialization failed; direct SQL is disabled", ex);
      tx.rollback();
      return false;
    } finally {
      if (doCommit) {
        tx.commit();
      }
      for (Query q : initQueries) {
        try {
          q.closeAll();
        } catch (Throwable t) {
        }
      }
    }
  }

  private boolean runTestQuery() {
    Transaction tx = pm.currentTransaction();
    boolean doCommit = false;
    if (!tx.isActive()) {
      tx.begin();
      doCommit = true;
    }
    Query query = null;
    // Run a self-test query. If it doesn't work, we will self-disable. What a PITA...
    String selfTestQuery = "select \"DB_ID\" from " + DBS + "";
    try {
      prepareTxn();
      query = pm.newQuery("javax.jdo.query.SQL", selfTestQuery);
      query.execute();
      return true;
    } catch (Throwable t) {
      doCommit = false;
      LOG.warn("Self-test query [" + selfTestQuery + "] failed; direct SQL is disabled", t);
      tx.rollback();
      return false;
    } finally {
      if (doCommit) {
        tx.commit();
      }
      if (query != null) {
        query.closeAll();
      }
    }
  }

  public String getSchema() {
    return schema;
  }

  public boolean isCompatibleDatastore() {
    return isCompatibleDatastore;
  }

  private void executeNoResult(final String queryText) throws SQLException {
    JDOConnection jdoConn = pm.getDataStoreConnection();
    Statement statement = null;
    boolean doTrace = LOG.isDebugEnabled();
    try {
      long start = doTrace ? System.nanoTime() : 0;
      statement = ((Connection)jdoConn.getNativeConnection()).createStatement();
      statement.execute(queryText);
      timingTrace(doTrace, queryText, start, doTrace ? System.nanoTime() : 0);
    } finally {
      if(statement != null){
          statement.close();
      }
      jdoConn.close(); // We must release the connection before we call other pm methods.
    }
  }

  public Database getDatabase(String dbName) throws MetaException{
    Query queryDbSelector = null;
    Query queryDbParams = null;
    try {
      dbName = dbName.toLowerCase();

      String queryTextDbSelector= "select "
          + "\"DB_ID\", \"NAME\", \"DB_LOCATION_URI\", \"DESC\", "
          + "\"OWNER_NAME\", \"OWNER_TYPE\" "
          + "FROM "+ DBS +" where \"NAME\" = ? ";
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
          + " from " + DATABASE_PARAMS + " "
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
   * Get table names by using direct SQL queries.
   *
   * @param dbName Metastore database namme
   * @param tableType Table type, or null if we want to get all tables
   * @return list of table names
   */
  public List<String> getTables(String dbName, TableType tableType) throws MetaException {
    String queryText = "SELECT " + TBLS + ".\"TBL_NAME\""
      + " FROM " + TBLS + " "
      + " INNER JOIN " + DBS + " ON " + TBLS + ".\"DB_ID\" = " + DBS + ".\"DB_ID\" "
      + " WHERE " + DBS + ".\"NAME\" = ? "
      + (tableType == null ? "" : "AND " + TBLS + ".\"TBL_TYPE\" = ? ") ;

    List<String> pms = new ArrayList<String>();
    pms.add(dbName);
    if (tableType != null) {
      pms.add(tableType.toString());
    }

    Query<?> queryParams = pm.newQuery("javax.jdo.query.SQL", queryText);
    return executeWithArray(
        queryParams, pms.toArray(), queryText);
  }

  /**
   * Get table names by using direct SQL queries.
   *
   * @param dbName Metastore database namme
   * @return list of table names
   */
  public List<String> getMaterializedViewsForRewriting(String dbName) throws MetaException {
    String queryText = "SELECT " + TBLS + ".\"TBL_NAME\""
      + " FROM " + TBLS + " "
      + " INNER JOIN " + DBS + " ON " + TBLS + ".\"DB_ID\" = " + DBS + ".\"DB_ID\" "
      + " WHERE " + DBS + ".\"NAME\" = ? AND " + TBLS + ".\"TBL_TYPE\" = ? " ;

    List<String> pms = new ArrayList<String>();
    pms.add(dbName);
    pms.add(TableType.MATERIALIZED_VIEW.toString());

    Query<?> queryParams = pm.newQuery("javax.jdo.query.SQL", queryText);
    return executeWithArray(
        queryParams, pms.toArray(), queryText);
  }

  /**
   * Gets partitions by using direct SQL queries.
   * Note that batching is not needed for this method - list of names implies the batch size;
   * @param dbName Metastore db name.
   * @param tblName Metastore table name.
   * @param partNames Partition names to get.
   * @return List of partitions.
   */
  public List<Partition> getPartitionsViaSqlFilter(final String dbName, final String tblName,
      List<String> partNames) throws MetaException {
    if (partNames.isEmpty()) {
      return Collections.emptyList();
    }
    return runBatched(partNames, new Batchable<String, Partition>() {
      @Override
      public List<Partition> run(List<String> input) throws MetaException {
        String filter = "" + PARTITIONS + ".\"PART_NAME\" in (" + makeParams(input.size()) + ")";
        return getPartitionsViaSqlFilterInternal(dbName, tblName, null, filter, input,
            Collections.<String>emptyList(), null);
      }
    });
  }

  /**
   * Gets partitions by using direct SQL queries.
   * @param filter The filter.
   * @param max The maximum number of partitions to return.
   * @return List of partitions.
   */
  public List<Partition> getPartitionsViaSqlFilter(
      SqlFilterForPushdown filter, Integer max) throws MetaException {
    Boolean isViewTable = isViewTable(filter.table);
    return getPartitionsViaSqlFilterInternal(filter.table.getDbName(), filter.table.getTableName(),
        isViewTable, filter.filter, filter.params, filter.joins, max);
  }

  public static class SqlFilterForPushdown {
    private final List<Object> params = new ArrayList<Object>();
    private final List<String> joins = new ArrayList<String>();
    private String filter;
    private Table table;
  }

  public boolean generateSqlFilterForPushdown(
      Table table, ExpressionTree tree, SqlFilterForPushdown result) throws MetaException {
    // Derby and Oracle do not interpret filters ANSI-properly in some cases and need a workaround.
    boolean dbHasJoinCastBug = DatabaseProduct.hasJoinOperationOrderBug(dbType);
    result.table = table;
    result.filter = PartitionFilterGenerator.generateSqlFilter(table, tree, result.params,
        result.joins, dbHasJoinCastBug, defaultPartName, dbType, schema);
    return result.filter != null;
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
        null, Collections.<String>emptyList(), Collections.<String>emptyList(), max);
  }

  private static Boolean isViewTable(Table t) {
    return t.isSetTableType() ?
        t.getTableType().equals(TableType.VIRTUAL_VIEW.toString()) : null;
  }

  private boolean isViewTable(String dbName, String tblName) throws MetaException {
    Query query = null;
    try {
      String queryText = "select \"TBL_TYPE\" from " + TBLS + "" +
          " inner join " + DBS + " on " + TBLS + ".\"DB_ID\" = " + DBS + ".\"DB_ID\" " +
          " where " + TBLS + ".\"TBL_NAME\" = ? and " + DBS + ".\"NAME\" = ?";
      Object[] params = new Object[] { tblName, dbName };
      query = pm.newQuery("javax.jdo.query.SQL", queryText);
      query.setUnique(true);
      Object result = executeWithArray(query, params, queryText);
      return (result != null) && result.toString().equals(TableType.VIRTUAL_VIEW.toString());
    } finally {
      if (query != null) {
        query.closeAll();
      }
    }
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
      final Boolean isView, String sqlFilter, List<? extends Object> paramsForFilter,
      List<String> joinsForFilter, Integer max) throws MetaException {
    boolean doTrace = LOG.isDebugEnabled();
    final String dbNameLcase = dbName.toLowerCase(), tblNameLcase = tblName.toLowerCase();
    // We have to be mindful of order during filtering if we are not returning all partitions.
    String orderForFilter = (max != null) ? " order by \"PART_NAME\" asc" : "";

    // Get all simple fields for partitions and related objects, which we can map one-on-one.
    // We will do this in 2 queries to use different existing indices for each one.
    // We do not get table and DB name, assuming they are the same as we are using to filter.
    // TODO: We might want to tune the indexes instead. With current ones MySQL performs
    // poorly, esp. with 'order by' w/o index on large tables, even if the number of actual
    // results is small (query that returns 8 out of 32k partitions can go 4sec. to 0sec. by
    // just adding a \"PART_ID\" IN (...) filter that doesn't alter the results to it, probably
    // causing it to not sort the entire table due to not knowing how selective the filter is.
    String queryText =
        "select " + PARTITIONS + ".\"PART_ID\" from " + PARTITIONS + ""
      + "  inner join " + TBLS + " on " + PARTITIONS + ".\"TBL_ID\" = " + TBLS + ".\"TBL_ID\" "
      + "    and " + TBLS + ".\"TBL_NAME\" = ? "
      + "  inner join " + DBS + " on " + TBLS + ".\"DB_ID\" = " + DBS + ".\"DB_ID\" "
      + "     and " + DBS + ".\"NAME\" = ? "
      + join(joinsForFilter, ' ')
      + (StringUtils.isBlank(sqlFilter) ? "" : (" where " + sqlFilter)) + orderForFilter;
    Object[] params = new Object[paramsForFilter.size() + 2];
    params[0] = tblNameLcase;
    params[1] = dbNameLcase;
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
    timingTrace(doTrace, queryText, start, queryTime);
    if (sqlResult.isEmpty()) {
      return Collections.emptyList(); // no partitions, bail early.
    }

    // Get full objects. For Oracle/etc. do it in batches.
    List<Partition> result = runBatched(sqlResult, new Batchable<Object, Partition>() {
      @Override
      public List<Partition> run(List<Object> input) throws MetaException {
        return getPartitionsFromPartitionIds(dbNameLcase, tblNameLcase, isView, input);
      }
    });

    query.closeAll();
    return result;
  }

  /** Should be called with the list short enough to not trip up Oracle/etc. */
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
      "select " + PARTITIONS + ".\"PART_ID\", " + SDS + ".\"SD_ID\", " + SDS + ".\"CD_ID\","
    + " " + SERDES + ".\"SERDE_ID\", " + PARTITIONS + ".\"CREATE_TIME\","
    + " " + PARTITIONS + ".\"LAST_ACCESS_TIME\", " + SDS + ".\"INPUT_FORMAT\", " + SDS + ".\"IS_COMPRESSED\","
    + " " + SDS + ".\"IS_STOREDASSUBDIRECTORIES\", " + SDS + ".\"LOCATION\", " + SDS + ".\"NUM_BUCKETS\","
    + " " + SDS + ".\"OUTPUT_FORMAT\", " + SERDES + ".\"NAME\", " + SERDES + ".\"SLIB\" "
    + "from " + PARTITIONS + ""
    + "  left outer join " + SDS + " on " + PARTITIONS + ".\"SD_ID\" = " + SDS + ".\"SD_ID\" "
    + "  left outer join " + SERDES + " on " + SDS + ".\"SERDE_ID\" = " + SERDES + ".\"SERDE_ID\" "
    + "where \"PART_ID\" in (" + partIds + ") order by \"PART_NAME\" asc";
    long start = doTrace ? System.nanoTime() : 0;
    Query query = pm.newQuery("javax.jdo.query.SQL", queryText);
    List<Object[]> sqlResult = executeWithArray(query, null, queryText);
    long queryTime = doTrace ? System.nanoTime() : 0;
    Deadline.checkTimeout();

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
      // A partition must have at least sdId and serdeId set, or nothing set if it's a view.
      if (sdId == null || serdeId == null) {
        if (isView == null) {
          isView = isViewTable(dbName, tblName);
        }
        if ((sdId != null || colId != null || serdeId != null) || !isView) {
          throw new MetaException("Unexpected null for one of the IDs, SD " + sdId +
                  ", serde " + serdeId + " for a " + (isView ? "" : "non-") + " view");
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
      assert serdeId != null;

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

      if (colId != null) {
        List<FieldSchema> cols = colss.get(colId);
        // We expect that colId will be the same for all (or many) SDs.
        if (cols == null) {
          cols = new ArrayList<FieldSchema>();
          colss.put(colId, cols);
          colsSb.append(colId).append(",");
        }
        sd.setCols(cols);
      }

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
      Deadline.checkTimeout();
    }
    query.closeAll();
    timingTrace(doTrace, queryText, start, queryTime);

    // Now get all the one-to-many things. Start with partitions.
    queryText = "select \"PART_ID\", \"PARAM_KEY\", \"PARAM_VALUE\" from " + PARTITION_PARAMS + ""
        + " where \"PART_ID\" in (" + partIds + ") and \"PARAM_KEY\" is not null"
        + " order by \"PART_ID\" asc";
    loopJoinOrderedResult(partitions, queryText, 0, new ApplyFunc<Partition>() {
      @Override
      public void apply(Partition t, Object[] fields) {
        t.putToParameters((String)fields[1], (String)fields[2]);
      }});
    // Perform conversion of null map values
    for (Partition t : partitions.values()) {
      t.setParameters(MetaStoreUtils.trimMapNulls(t.getParameters(), convertMapNullsToEmptyStrings));
    }

    queryText = "select \"PART_ID\", \"PART_KEY_VAL\" from " + PARTITION_KEY_VALS + ""
        + " where \"PART_ID\" in (" + partIds + ")"
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

    String sdIds = trimCommaList(sdSb);
    String serdeIds = trimCommaList(serdeSb);
    String colIds = trimCommaList(colsSb);

    // Get all the stuff for SD. Don't do empty-list check - we expect partitions do have SDs.
    queryText = "select \"SD_ID\", \"PARAM_KEY\", \"PARAM_VALUE\" from " + SD_PARAMS + ""
        + " where \"SD_ID\" in (" + sdIds + ") and \"PARAM_KEY\" is not null"
        + " order by \"SD_ID\" asc";
    loopJoinOrderedResult(sds, queryText, 0, new ApplyFunc<StorageDescriptor>() {
      @Override
      public void apply(StorageDescriptor t, Object[] fields) {
        t.putToParameters((String)fields[1], extractSqlClob(fields[2]));
      }});
    // Perform conversion of null map values
    for (StorageDescriptor t : sds.values()) {
      t.setParameters(MetaStoreUtils.trimMapNulls(t.getParameters(), convertMapNullsToEmptyStrings));
    }

    queryText = "select \"SD_ID\", \"COLUMN_NAME\", " + SORT_COLS + ".\"ORDER\""
        + " from " + SORT_COLS + ""
        + " where \"SD_ID\" in (" + sdIds + ")"
        + " order by \"SD_ID\" asc, \"INTEGER_IDX\" asc";
    loopJoinOrderedResult(sds, queryText, 0, new ApplyFunc<StorageDescriptor>() {
      @Override
      public void apply(StorageDescriptor t, Object[] fields) {
        if (fields[2] == null) return;
        t.addToSortCols(new Order((String)fields[1], extractSqlInt(fields[2])));
      }});

    queryText = "select \"SD_ID\", \"BUCKET_COL_NAME\" from " + BUCKETING_COLS + ""
        + " where \"SD_ID\" in (" + sdIds + ")"
        + " order by \"SD_ID\" asc, \"INTEGER_IDX\" asc";
    loopJoinOrderedResult(sds, queryText, 0, new ApplyFunc<StorageDescriptor>() {
      @Override
      public void apply(StorageDescriptor t, Object[] fields) {
        t.addToBucketCols((String)fields[1]);
      }});

    // Skewed columns stuff.
    queryText = "select \"SD_ID\", \"SKEWED_COL_NAME\" from " + SKEWED_COL_NAMES + ""
        + " where \"SD_ID\" in (" + sdIds + ")"
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
            "select " + SKEWED_VALUES + ".\"SD_ID_OID\","
          + "  " + SKEWED_STRING_LIST_VALUES + ".\"STRING_LIST_ID\","
          + "  " + SKEWED_STRING_LIST_VALUES + ".\"STRING_LIST_VALUE\" "
          + "from " + SKEWED_VALUES + " "
          + "  left outer join " + SKEWED_STRING_LIST_VALUES + " on " + SKEWED_VALUES + "."
          + "\"STRING_LIST_ID_EID\" = " + SKEWED_STRING_LIST_VALUES + ".\"STRING_LIST_ID\" "
          + "where " + SKEWED_VALUES + ".\"SD_ID_OID\" in (" + sdIds + ") "
          + "  and " + SKEWED_VALUES + ".\"STRING_LIST_ID_EID\" is not null "
          + "  and " + SKEWED_VALUES + ".\"INTEGER_IDX\" >= 0 "
          + "order by " + SKEWED_VALUES + ".\"SD_ID_OID\" asc, " + SKEWED_VALUES + ".\"INTEGER_IDX\" asc,"
          + "  " + SKEWED_STRING_LIST_VALUES + ".\"INTEGER_IDX\" asc";
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
            t.getSkewedInfo().addToSkewedColValues(Collections.<String>emptyList());
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
            "select " + SKEWED_COL_VALUE_LOC_MAP + ".\"SD_ID\","
          + " " + SKEWED_STRING_LIST_VALUES + ".STRING_LIST_ID,"
          + " " + SKEWED_COL_VALUE_LOC_MAP + ".\"LOCATION\","
          + " " + SKEWED_STRING_LIST_VALUES + ".\"STRING_LIST_VALUE\" "
          + "from " + SKEWED_COL_VALUE_LOC_MAP + ""
          + "  left outer join " + SKEWED_STRING_LIST_VALUES + " on " + SKEWED_COL_VALUE_LOC_MAP + "."
          + "\"STRING_LIST_ID_KID\" = " + SKEWED_STRING_LIST_VALUES + ".\"STRING_LIST_ID\" "
          + "where " + SKEWED_COL_VALUE_LOC_MAP + ".\"SD_ID\" in (" + sdIds + ")"
          + "  and " + SKEWED_COL_VALUE_LOC_MAP + ".\"STRING_LIST_ID_KID\" is not null "
          + "order by " + SKEWED_COL_VALUE_LOC_MAP + ".\"SD_ID\" asc,"
          + "  " + SKEWED_STRING_LIST_VALUES + ".\"STRING_LIST_ID\" asc,"
          + "  " + SKEWED_STRING_LIST_VALUES + ".\"INTEGER_IDX\" asc";

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
          + " from " + COLUMNS_V2 + " where \"CD_ID\" in (" + colIds + ")"
          + " order by \"CD_ID\" asc, \"INTEGER_IDX\" asc";
      loopJoinOrderedResult(colss, queryText, 0, new ApplyFunc<List<FieldSchema>>() {
        @Override
        public void apply(List<FieldSchema> t, Object[] fields) {
          t.add(new FieldSchema((String)fields[2], extractSqlClob(fields[3]), (String)fields[1]));
        }});
    }

    // Finally, get all the stuff for serdes - just the params.
    queryText = "select \"SERDE_ID\", \"PARAM_KEY\", \"PARAM_VALUE\" from " + SERDE_PARAMS + ""
        + " where \"SERDE_ID\" in (" + serdeIds + ") and \"PARAM_KEY\" is not null"
        + " order by \"SERDE_ID\" asc";
    loopJoinOrderedResult(serdes, queryText, 0, new ApplyFunc<SerDeInfo>() {
      @Override
      public void apply(SerDeInfo t, Object[] fields) {
        t.putToParameters((String)fields[1], extractSqlClob(fields[2]));
      }});
    // Perform conversion of null map values
    for (SerDeInfo t : serdes.values()) {
      t.setParameters(MetaStoreUtils.trimMapNulls(t.getParameters(), convertMapNullsToEmptyStrings));
    }

    return orderedResult;
  }

  public int getNumPartitionsViaSqlFilter(SqlFilterForPushdown filter) throws MetaException {
    boolean doTrace = LOG.isDebugEnabled();
    String dbName = filter.table.getDbName().toLowerCase();
    String tblName = filter.table.getTableName().toLowerCase();

    // Get number of partitions by doing count on PART_ID.
    String queryText = "select count(" + PARTITIONS + ".\"PART_ID\") from " + PARTITIONS + ""
      + "  inner join " + TBLS + " on " + PARTITIONS + ".\"TBL_ID\" = " + TBLS + ".\"TBL_ID\" "
      + "    and " + TBLS + ".\"TBL_NAME\" = ? "
      + "  inner join " + DBS + " on " + TBLS + ".\"DB_ID\" = " + DBS + ".\"DB_ID\" "
      + "     and " + DBS + ".\"NAME\" = ? "
      + join(filter.joins, ' ')
      + (filter.filter == null || filter.filter.trim().isEmpty() ? "" : (" where " + filter.filter));

    Object[] params = new Object[filter.params.size() + 2];
    params[0] = tblName;
    params[1] = dbName;
    for (int i = 0; i < filter.params.size(); ++i) {
      params[i + 2] = filter.params.get(i);
    }

    long start = doTrace ? System.nanoTime() : 0;
    Query query = pm.newQuery("javax.jdo.query.SQL", queryText);
    query.setUnique(true);
    int sqlResult = extractSqlInt(query.executeWithArray(params));
    long queryTime = doTrace ? System.nanoTime() : 0;
    timingTrace(doTrace, queryText, start, queryTime);
    return sqlResult;
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

  /**
   * Convert a boolean value returned from the RDBMS to a Java Boolean object.
   * MySQL has booleans, but e.g. Derby uses 'Y'/'N' mapping.
   *
   * @param value
   *          column value from the database
   * @return The Boolean value of the database column value, null if the column
   *         value is null
   * @throws MetaException
   *           if the column value cannot be converted into a Boolean object
   */
  private static Boolean extractSqlBoolean(Object value) throws MetaException {
    if (value == null) {
      return null;
    }
    if (value instanceof Boolean) {
      return (Boolean)value;
    }
    if (value instanceof String) {
      try {
        return BooleanUtils.toBooleanObject((String) value, "Y", "N", null);
      } catch (IllegalArgumentException iae) {
        // NOOP
      }
    }
    throw new MetaException("Cannot extract boolean from column value " + value);
  }

  private int extractSqlInt(Object field) {
    return ((Number)field).intValue();
  }

  private String extractSqlString(Object value) {
    if (value == null) return null;
    return value.toString();
  }

  static Double extractSqlDouble(Object obj) throws MetaException {
    if (obj == null)
      return null;
    if (!(obj instanceof Number)) {
      throw new MetaException("Expected numeric type but got " + obj.getClass().getName());
    }
    return ((Number) obj).doubleValue();
  }

  private String extractSqlClob(Object value) {
    if (value == null) return null;
    try {
      if (value instanceof Clob) {
        // we trim the Clob value to a max length an int can hold
        int maxLength = (((Clob)value).length() < Integer.MAX_VALUE - 2) ? (int)((Clob)value).length() : Integer.MAX_VALUE - 2;
        return ((Clob)value).getSubString(1L, maxLength);
      } else {
        return value.toString();
      }
    } catch (SQLException sqle) {
      return null;
    }
  }

  static byte[] extractSqlBlob(Object value) throws MetaException {
    if (value == null)
      return null;
    if (value instanceof Blob) {
      //derby, oracle
      try {
        // getBytes function says: pos the ordinal position of the first byte in
        // the BLOB value to be extracted; the first byte is at position 1
        return ((Blob) value).getBytes(1, (int) ((Blob) value).length());
      } catch (SQLException e) {
        throw new MetaException("Encounter error while processing blob.");
      }
    }
    else if (value instanceof byte[]) {
      // mysql, postgres, sql server
      return (byte[]) value;
    }
	else {
      // this may happen when enablebitvector is false
      LOG.debug("Expected blob type but got " + value.getClass().getName());
      return null;
    }
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
      Deadline.checkTimeout();
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
    private final String defaultPartName;
    private final DatabaseProduct dbType;
    private final String PARTITION_KEY_VALS, PARTITIONS, DBS, TBLS;

    private PartitionFilterGenerator(Table table, List<Object> params, List<String> joins,
        boolean dbHasJoinCastBug, String defaultPartName, DatabaseProduct dbType, String schema) {
      this.table = table;
      this.params = params;
      this.joins = joins;
      this.dbHasJoinCastBug = dbHasJoinCastBug;
      this.filterBuffer = new FilterBuilder(false);
      this.defaultPartName = defaultPartName;
      this.dbType = dbType;
      this.PARTITION_KEY_VALS = getFullyQualifiedName(schema, "PARTITION_KEY_VALS");
      this.PARTITIONS = getFullyQualifiedName(schema, "PARTITIONS");
      this.DBS = getFullyQualifiedName(schema, "DBS");
      this.TBLS = getFullyQualifiedName(schema, "TBLS");
    }

    /**
     * Generate the ANSI SQL92 filter for the given expression tree
     * @param table the table being queried
     * @param params the ordered parameters for the resulting expression
     * @param joins the joins necessary for the resulting expression
     * @return the string representation of the expression tree
     */
    private static String generateSqlFilter(Table table, ExpressionTree tree, List<Object> params,
        List<String> joins, boolean dbHasJoinCastBug, String defaultPartName,
        DatabaseProduct dbType, String schema) throws MetaException {
      assert table != null;
      if (tree == null) {
        // consistent with other APIs like makeExpressionTree, null is returned to indicate that
        // the filter could not pushed down due to parsing issue etc
        return null;
      }
      if (tree.getRoot() == null) {
        return "";
      }
      PartitionFilterGenerator visitor = new PartitionFilterGenerator(
          table, params, joins, dbHasJoinCastBug, defaultPartName, dbType, schema);
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
        if (colTypeStr.equals(ColumnType.STRING_TYPE_NAME)) {
          return FilterType.String;
        } else if (colTypeStr.equals(ColumnType.DATE_TYPE_NAME)) {
          return FilterType.Date;
        } else if (ColumnType.IntegralTypes.contains(colTypeStr)) {
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

      // if Filter.g does date parsing for quoted strings, we'd need to verify there's no
      // type mismatch when string col is filtered by a string that looks like date.
      if (colType == FilterType.Date && valType == FilterType.String) {
        // Filter.g cannot parse a quoted date; try to parse date here too.
        try {
          nodeValue = new java.sql.Date(
              org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.PARTITION_DATE_FORMAT.get().parse((String)nodeValue).getTime());
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
        joins.set(partColIndex, "inner join " + PARTITION_KEY_VALS + " \"FILTER" + partColIndex
            + "\" on \"FILTER"  + partColIndex + "\".\"PART_ID\" = " + PARTITIONS + ".\"PART_ID\""
            + " and \"FILTER" + partColIndex + "\".\"INTEGER_IDX\" = " + partColIndex);
      }

      // Build the filter and add parameters linearly; we are traversing leaf nodes LTR.
      String tableValue = "\"FILTER" + partColIndex + "\".\"PART_KEY_VAL\"";

      if (node.isReverseOrder) {
        params.add(nodeValue);
      }
      String tableColumn = tableValue;
      if (colType != FilterType.String) {
        // The underlying database field is varchar, we need to compare numbers.
        if (colType == FilterType.Integral) {
          tableValue = "cast(" + tableValue + " as decimal(21,0))";
        } else if (colType == FilterType.Date) {
          if (dbType == DatabaseProduct.ORACLE) {
            // Oracle requires special treatment... as usual.
            tableValue = "TO_DATE(" + tableValue + ", 'YYYY-MM-DD')";
          } else {
            tableValue = "cast(" + tableValue + " as date)";
          }
        }

        // Workaround for HIVE_DEFAULT_PARTITION - ignore it like JDO does, for now.
        String tableValue0 = tableValue;
        tableValue = "(case when " + tableColumn + " <> ?";
        params.add(defaultPartName);

        if (dbHasJoinCastBug) {
          // This is a workaround for DERBY-6358 and Oracle bug; it is pretty horrible.
          tableValue += (" and " + TBLS + ".\"TBL_NAME\" = ? and " + DBS + ".\"NAME\" = ? and "
              + "\"FILTER" + partColIndex + "\".\"PART_ID\" = " + PARTITIONS + ".\"PART_ID\" and "
                + "\"FILTER" + partColIndex + "\".\"INTEGER_IDX\" = " + partColIndex);
          params.add(table.getTableName().toLowerCase());
          params.add(table.getDbName().toLowerCase());
        }
        tableValue += " then " + tableValue0 + " else null end)";
      }
      if (!node.isReverseOrder) {
        params.add(nodeValue);
      }

      filterBuffer.append(node.isReverseOrder
          ? "(? " + node.operator.getSqlOp() + " " + tableValue + ")"
          : "(" + tableValue + " " + node.operator.getSqlOp() + " ?)");
    }
  }

  /**
   * Retrieve the column statistics for the specified columns of the table. NULL
   * is returned if the columns are not provided.
   * @param dbName      the database name of the table
   * @param tableName   the table name
   * @param colNames    the list of the column names
   * @return            the column statistics for the specified columns
   * @throws MetaException
   */
  public ColumnStatistics getTableStats(final String dbName, final String tableName,
      List<String> colNames, boolean enableBitVector) throws MetaException {
    if (colNames == null || colNames.isEmpty()) {
      return null;
    }
    final boolean doTrace = LOG.isDebugEnabled();
    final String queryText0 = "select " + getStatsList(enableBitVector) + " from " + TAB_COL_STATS + " "
          + " where \"DB_NAME\" = ? and \"TABLE_NAME\" = ? and \"COLUMN_NAME\" in (";
    Batchable<String, Object[]> b = new Batchable<String, Object[]>() {
      @Override
      public List<Object[]> run(List<String> input) throws MetaException {
        String queryText = queryText0 + makeParams(input.size()) + ")";
        Object[] params = new Object[input.size() + 2];
        params[0] = dbName;
        params[1] = tableName;
        for (int i = 0; i < input.size(); ++i) {
          params[i + 2] = input.get(i);
        }
        long start = doTrace ? System.nanoTime() : 0;
        Query query = pm.newQuery("javax.jdo.query.SQL", queryText);
        Object qResult = executeWithArray(query, params, queryText);
        timingTrace(doTrace, queryText0 + "...)", start, (doTrace ? System.nanoTime() : 0));
        if (qResult == null) {
          query.closeAll();
          return null;
        }
        addQueryAfterUse(query);
        return ensureList(qResult);
      }
    };
    List<Object[]> list = runBatched(colNames, b);
    if (list.isEmpty()) {
      return null;
    }
    ColumnStatisticsDesc csd = new ColumnStatisticsDesc(true, dbName, tableName);
    ColumnStatistics result = makeColumnStats(list, csd, 0);
    b.closeAllQueries();
    return result;
  }

  public AggrStats aggrColStatsForPartitions(String dbName, String tableName,
      List<String> partNames, List<String> colNames, boolean useDensityFunctionForNDVEstimation,
      double ndvTuner, boolean enableBitVector) throws MetaException {
    if (colNames.isEmpty() || partNames.isEmpty()) {
      LOG.debug("Columns is empty or partNames is empty : Short-circuiting stats eval");
      return new AggrStats(Collections.<ColumnStatisticsObj>emptyList(), 0); // Nothing to aggregate
    }
    long partsFound = 0;
    List<ColumnStatisticsObj> colStatsList;
    // Try to read from the cache first
    if (isAggregateStatsCacheEnabled
        && (partNames.size() < aggrStatsCache.getMaxPartsPerCacheNode())) {
      AggrColStats colStatsAggrCached;
      List<ColumnStatisticsObj> colStatsAggrFromDB;
      int maxPartsPerCacheNode = aggrStatsCache.getMaxPartsPerCacheNode();
      double fpp = aggrStatsCache.getFalsePositiveProbability();
      colStatsList = new ArrayList<ColumnStatisticsObj>();
      // Bloom filter for the new node that we will eventually add to the cache
      BloomFilter bloomFilter = createPartsBloomFilter(maxPartsPerCacheNode, fpp, partNames);
      boolean computePartsFound = true;
      for (String colName : colNames) {
        // Check the cache first
        colStatsAggrCached = aggrStatsCache.get(dbName, tableName, colName, partNames);
        if (colStatsAggrCached != null) {
          colStatsList.add(colStatsAggrCached.getColStats());
          partsFound = colStatsAggrCached.getNumPartsCached();
        } else {
          if (computePartsFound) {
            partsFound = partsFoundForPartitions(dbName, tableName, partNames, colNames);
            computePartsFound = false;
          }
          List<String> colNamesForDB = new ArrayList<String>();
          colNamesForDB.add(colName);
          // Read aggregated stats for one column
          colStatsAggrFromDB =
              columnStatisticsObjForPartitions(dbName, tableName, partNames, colNamesForDB,
                  partsFound, useDensityFunctionForNDVEstimation, ndvTuner, enableBitVector);
          if (!colStatsAggrFromDB.isEmpty()) {
            ColumnStatisticsObj colStatsAggr = colStatsAggrFromDB.get(0);
            colStatsList.add(colStatsAggr);
            // Update the cache to add this new aggregate node
            aggrStatsCache.add(dbName, tableName, colName, partsFound, colStatsAggr, bloomFilter);
          }
        }
      }
    } else {
      partsFound = partsFoundForPartitions(dbName, tableName, partNames, colNames);
      colStatsList =
          columnStatisticsObjForPartitions(dbName, tableName, partNames, colNames, partsFound,
              useDensityFunctionForNDVEstimation, ndvTuner, enableBitVector);
    }
    LOG.info("useDensityFunctionForNDVEstimation = " + useDensityFunctionForNDVEstimation
        + "\npartsFound = " + partsFound + "\nColumnStatisticsObj = "
        + Arrays.toString(colStatsList.toArray()));
    return new AggrStats(colStatsList, partsFound);
  }

  private BloomFilter createPartsBloomFilter(int maxPartsPerCacheNode, double fpp,
      List<String> partNames) {
    BloomFilter bloomFilter = new BloomFilter(maxPartsPerCacheNode, fpp);
    for (String partName : partNames) {
      bloomFilter.add(partName.getBytes());
    }
    return bloomFilter;
  }

  private long partsFoundForPartitions(final String dbName, final String tableName,
      final List<String> partNames, List<String> colNames) throws MetaException {
    assert !colNames.isEmpty() && !partNames.isEmpty();
    final boolean doTrace = LOG.isDebugEnabled();
    final String queryText0  = "select count(\"COLUMN_NAME\") from " + PART_COL_STATS + ""
        + " where \"DB_NAME\" = ? and \"TABLE_NAME\" = ? "
        + " and \"COLUMN_NAME\" in (%1$s) and \"PARTITION_NAME\" in (%2$s)"
        + " group by \"PARTITION_NAME\"";
    List<Long> allCounts = runBatched(colNames, new Batchable<String, Long>() {
      @Override
      public List<Long> run(final List<String> inputColName) throws MetaException {
        return runBatched(partNames, new Batchable<String, Long>() {
          @Override
          public List<Long> run(List<String> inputPartNames) throws MetaException {
            long partsFound = 0;
            String queryText = String.format(queryText0,
                makeParams(inputColName.size()), makeParams(inputPartNames.size()));
            long start = doTrace ? System.nanoTime() : 0;
            Query query = pm.newQuery("javax.jdo.query.SQL", queryText);
            try {
              Object qResult = executeWithArray(query, prepareParams(
                  dbName, tableName, inputPartNames, inputColName), queryText);
              long end = doTrace ? System.nanoTime() : 0;
              timingTrace(doTrace, queryText, start, end);
              ForwardQueryResult<?> fqr = (ForwardQueryResult<?>) qResult;
              Iterator<?> iter = fqr.iterator();
              while (iter.hasNext()) {
                if (extractSqlLong(iter.next()) == inputColName.size()) {
                  partsFound++;
                }
              }
              return Lists.<Long>newArrayList(partsFound);
            } finally {
              query.closeAll();
            }
          }
        });
      }
    });
    long partsFound = 0;
    for (Long val : allCounts) {
      partsFound += val;
    }
    return partsFound;
  }

  private List<ColumnStatisticsObj> columnStatisticsObjForPartitions(final String dbName,
    final String tableName, final List<String> partNames, List<String> colNames, long partsFound,
    final boolean useDensityFunctionForNDVEstimation, final double ndvTuner, final boolean enableBitVector) throws MetaException {
    final boolean areAllPartsFound = (partsFound == partNames.size());
    return runBatched(colNames, new Batchable<String, ColumnStatisticsObj>() {
      @Override
      public List<ColumnStatisticsObj> run(final List<String> inputColNames) throws MetaException {
        return runBatched(partNames, new Batchable<String, ColumnStatisticsObj>() {
          @Override
          public List<ColumnStatisticsObj> run(List<String> inputPartNames) throws MetaException {
            return columnStatisticsObjForPartitionsBatch(dbName, tableName, inputPartNames,
                inputColNames, areAllPartsFound, useDensityFunctionForNDVEstimation, ndvTuner, enableBitVector);
          }
        });
      }
    });
  }

  public List<ColStatsObjWithSourceInfo> getColStatsForAllTablePartitions(String dbName,
      boolean enableBitVector) throws MetaException {
    String queryText = "select \"TABLE_NAME\", \"PARTITION_NAME\", " + getStatsList(enableBitVector)
        + " from " + " " + PART_COL_STATS + " where \"DB_NAME\" = ?";
    long start = 0;
    long end = 0;
    Query query = null;
    boolean doTrace = LOG.isDebugEnabled();
    Object qResult = null;
    start = doTrace ? System.nanoTime() : 0;
    List<ColStatsObjWithSourceInfo> colStatsForDB = new ArrayList<ColStatsObjWithSourceInfo>();
    try {
      query = pm.newQuery("javax.jdo.query.SQL", queryText);
      qResult = executeWithArray(query, new Object[] { dbName }, queryText);
      if (qResult == null) {
        query.closeAll();
        return colStatsForDB;
      }
      end = doTrace ? System.nanoTime() : 0;
      timingTrace(doTrace, queryText, start, end);
      List<Object[]> list = ensureList(qResult);
      for (Object[] row : list) {
        String tblName = (String) row[0];
        String partName = (String) row[1];
        ColumnStatisticsObj colStatObj = prepareCSObj(row, 2);
        colStatsForDB.add(new ColStatsObjWithSourceInfo(colStatObj, dbName, tblName, partName));
        Deadline.checkTimeout();
      }
    } finally {
      query.closeAll();
    }
    return colStatsForDB;
  }

  /** Should be called with the list short enough to not trip up Oracle/etc. */
  private List<ColumnStatisticsObj> columnStatisticsObjForPartitionsBatch(String dbName,
      String tableName, List<String> partNames, List<String> colNames, boolean areAllPartsFound,
      boolean useDensityFunctionForNDVEstimation, double ndvTuner, boolean enableBitVector)
      throws MetaException {
    if (enableBitVector) {
      return aggrStatsUseJava(dbName, tableName, partNames, colNames, areAllPartsFound,
          useDensityFunctionForNDVEstimation, ndvTuner);
    } else {
      return aggrStatsUseDB(dbName, tableName, partNames, colNames, areAllPartsFound,
          useDensityFunctionForNDVEstimation, ndvTuner);
    }
  }

  private List<ColumnStatisticsObj> aggrStatsUseJava(String dbName, String tableName,
      List<String> partNames, List<String> colNames, boolean areAllPartsFound,
      boolean useDensityFunctionForNDVEstimation, double ndvTuner) throws MetaException {
    // 1. get all the stats for colNames in partNames;
    List<ColumnStatistics> partStats =
        getPartitionStats(dbName, tableName, partNames, colNames, true);
    // 2. use util function to aggr stats
    return MetaStoreUtils.aggrPartitionStats(partStats, dbName, tableName, partNames, colNames,
        areAllPartsFound, useDensityFunctionForNDVEstimation, ndvTuner);
  }

  private List<ColumnStatisticsObj> aggrStatsUseDB(String dbName,
      String tableName, List<String> partNames, List<String> colNames, boolean areAllPartsFound,
      boolean useDensityFunctionForNDVEstimation, double ndvTuner) throws MetaException {
    // TODO: all the extrapolation logic should be moved out of this class,
    // only mechanical data retrieval should remain here.
    String commonPrefix = "select \"COLUMN_NAME\", \"COLUMN_TYPE\", "
        + "min(\"LONG_LOW_VALUE\"), max(\"LONG_HIGH_VALUE\"), min(\"DOUBLE_LOW_VALUE\"), max(\"DOUBLE_HIGH_VALUE\"), "
        + "min(cast(\"BIG_DECIMAL_LOW_VALUE\" as decimal)), max(cast(\"BIG_DECIMAL_HIGH_VALUE\" as decimal)), "
        + "sum(\"NUM_NULLS\"), max(\"NUM_DISTINCTS\"), "
        + "max(\"AVG_COL_LEN\"), max(\"MAX_COL_LEN\"), sum(\"NUM_TRUES\"), sum(\"NUM_FALSES\"), "
        // The following data is used to compute a partitioned table's NDV based
        // on partitions' NDV when useDensityFunctionForNDVEstimation = true. Global NDVs cannot be
        // accurately derived from partition NDVs, because the domain of column value two partitions
        // can overlap. If there is no overlap then global NDV is just the sum
        // of partition NDVs (UpperBound). But if there is some overlay then
        // global NDV can be anywhere between sum of partition NDVs (no overlap)
        // and same as one of the partition NDV (domain of column value in all other
        // partitions is subset of the domain value in one of the partition)
        // (LowerBound).But under uniform distribution, we can roughly estimate the global
        // NDV by leveraging the min/max values.
        // And, we also guarantee that the estimation makes sense by comparing it to the
        // UpperBound (calculated by "sum(\"NUM_DISTINCTS\")")
        // and LowerBound (calculated by "max(\"NUM_DISTINCTS\")")
        + "avg((\"LONG_HIGH_VALUE\"-\"LONG_LOW_VALUE\")/cast(\"NUM_DISTINCTS\" as decimal)),"
        + "avg((\"DOUBLE_HIGH_VALUE\"-\"DOUBLE_LOW_VALUE\")/\"NUM_DISTINCTS\"),"
        + "avg((cast(\"BIG_DECIMAL_HIGH_VALUE\" as decimal)-cast(\"BIG_DECIMAL_LOW_VALUE\" as decimal))/\"NUM_DISTINCTS\"),"
        + "sum(\"NUM_DISTINCTS\")" + " from " + PART_COL_STATS + ""
        + " where \"DB_NAME\" = ? and \"TABLE_NAME\" = ? ";
    String queryText = null;
    long start = 0;
    long end = 0;
    Query query = null;
    boolean doTrace = LOG.isDebugEnabled();
    Object qResult = null;
    ForwardQueryResult<?> fqr = null;
    // Check if the status of all the columns of all the partitions exists
    // Extrapolation is not needed.
    if (areAllPartsFound) {
      queryText = commonPrefix + " and \"COLUMN_NAME\" in (" + makeParams(colNames.size()) + ")"
          + " and \"PARTITION_NAME\" in (" + makeParams(partNames.size()) + ")"
          + " group by \"COLUMN_NAME\", \"COLUMN_TYPE\"";
      start = doTrace ? System.nanoTime() : 0;
      query = pm.newQuery("javax.jdo.query.SQL", queryText);
      qResult = executeWithArray(query, prepareParams(dbName, tableName, partNames, colNames),
          queryText);
      if (qResult == null) {
        query.closeAll();
        return Collections.emptyList();
      }
      end = doTrace ? System.nanoTime() : 0;
      timingTrace(doTrace, queryText, start, end);
      List<Object[]> list = ensureList(qResult);
      List<ColumnStatisticsObj> colStats = new ArrayList<ColumnStatisticsObj>(list.size());
      for (Object[] row : list) {
        colStats.add(prepareCSObjWithAdjustedNDV(row, 0, useDensityFunctionForNDVEstimation, ndvTuner));
        Deadline.checkTimeout();
      }
      query.closeAll();
      return colStats;
    } else {
      // Extrapolation is needed for some columns.
      // In this case, at least a column status for a partition is missing.
      // We need to extrapolate this partition based on the other partitions
      List<ColumnStatisticsObj> colStats = new ArrayList<ColumnStatisticsObj>(colNames.size());
      queryText = "select \"COLUMN_NAME\", \"COLUMN_TYPE\", count(\"PARTITION_NAME\") "
          + " from " + PART_COL_STATS
          + " where \"DB_NAME\" = ? and \"TABLE_NAME\" = ? "
          + " and \"COLUMN_NAME\" in (" + makeParams(colNames.size()) + ")"
          + " and \"PARTITION_NAME\" in (" + makeParams(partNames.size()) + ")"
          + " group by \"COLUMN_NAME\", \"COLUMN_TYPE\"";
      start = doTrace ? System.nanoTime() : 0;
      query = pm.newQuery("javax.jdo.query.SQL", queryText);
      qResult = executeWithArray(query, prepareParams(dbName, tableName, partNames, colNames),
          queryText);
      end = doTrace ? System.nanoTime() : 0;
      timingTrace(doTrace, queryText, start, end);
      if (qResult == null) {
        query.closeAll();
        return Collections.emptyList();
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
        Deadline.checkTimeout();
      }
      query.closeAll();
      // Extrapolation is not needed for columns noExtraColumnNames
      if (noExtraColumnNames.size() != 0) {
        queryText = commonPrefix + " and \"COLUMN_NAME\" in ("
            + makeParams(noExtraColumnNames.size()) + ")" + " and \"PARTITION_NAME\" in ("
            + makeParams(partNames.size()) + ")" + " group by \"COLUMN_NAME\", \"COLUMN_TYPE\"";
        start = doTrace ? System.nanoTime() : 0;
        query = pm.newQuery("javax.jdo.query.SQL", queryText);
        qResult = executeWithArray(query,
            prepareParams(dbName, tableName, partNames, noExtraColumnNames), queryText);
        if (qResult == null) {
          query.closeAll();
          return Collections.emptyList();
        }
        list = ensureList(qResult);
        for (Object[] row : list) {
          colStats.add(prepareCSObjWithAdjustedNDV(row, 0, useDensityFunctionForNDVEstimation, ndvTuner));
          Deadline.checkTimeout();
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
        queryText = "select \"COLUMN_NAME\", sum(\"NUM_NULLS\"), sum(\"NUM_TRUES\"), sum(\"NUM_FALSES\"), sum(\"NUM_DISTINCTS\")"
            + " from " + PART_COL_STATS + " where \"DB_NAME\" = ? and \"TABLE_NAME\" = ? "
            + " and \"COLUMN_NAME\" in (" + makeParams(extraColumnNameTypeParts.size())
            + ") and \"PARTITION_NAME\" in (" + makeParams(partNames.size())
            + ") group by \"COLUMN_NAME\"";
        start = doTrace ? System.nanoTime() : 0;
        query = pm.newQuery("javax.jdo.query.SQL", queryText);
        List<String> extraColumnNames = new ArrayList<String>();
        extraColumnNames.addAll(extraColumnNameTypeParts.keySet());
        qResult = executeWithArray(query,
            prepareParams(dbName, tableName, partNames, extraColumnNames), queryText);
        if (qResult == null) {
          query.closeAll();
          return Collections.emptyList();
        }
        list = ensureList(qResult);
        // see the indexes for colstats in IExtrapolatePartStatus
        Integer[] sumIndex = new Integer[] { 6, 10, 11, 15 };
        for (Object[] row : list) {
          Map<Integer, Object> indexToObject = new HashMap<Integer, Object>();
          for (int ind = 1; ind < row.length; ind++) {
            indexToObject.put(sumIndex[ind - 1], row[ind]);
          }
          // row[0] is the column name
          sumMap.put((String) row[0], indexToObject);
          Deadline.checkTimeout();
        }
        end = doTrace ? System.nanoTime() : 0;
        timingTrace(doTrace, queryText, start, end);
        query.closeAll();
        for (Map.Entry<String, String[]> entry : extraColumnNameTypeParts.entrySet()) {
          Object[] row = new Object[IExtrapolatePartStatus.colStatNames.length + 2];
          String colName = entry.getKey();
          String colType = entry.getValue()[0];
          Long sumVal = Long.parseLong(entry.getValue()[1]);
          // fill in colname
          row[0] = colName;
          // fill in coltype
          row[1] = colType;
          // use linear extrapolation. more complicated one can be added in the
          // future.
          IExtrapolatePartStatus extrapolateMethod = new LinearExtrapolatePartStatus();
          // fill in colstatus
          Integer[] index = null;
          boolean decimal = false;
          if (colType.toLowerCase().startsWith("decimal")) {
            index = IExtrapolatePartStatus.indexMaps.get("decimal");
            decimal = true;
          } else {
            index = IExtrapolatePartStatus.indexMaps.get(colType.toLowerCase());
          }
          // if the colType is not the known type, long, double, etc, then get
          // all index.
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
                row[2 + colStatIndex] = val / sumVal * (partNames.size());
              }
            } else if (IExtrapolatePartStatus.aggrTypes[colStatIndex] == IExtrapolatePartStatus.AggrType.Min
                || IExtrapolatePartStatus.aggrTypes[colStatIndex] == IExtrapolatePartStatus.AggrType.Max) {
              // if the aggregation type is min/max, we extrapolate from the
              // left/right borders
              if (!decimal) {
                queryText = "select \"" + colStatName
                    + "\",\"PARTITION_NAME\" from " + PART_COL_STATS
                    + " where \"DB_NAME\" = ? and \"TABLE_NAME\" = ?" + " and \"COLUMN_NAME\" = ?"
                    + " and \"PARTITION_NAME\" in (" + makeParams(partNames.size()) + ")"
                    + " order by \"" + colStatName + "\"";
              } else {
                queryText = "select \"" + colStatName
                    + "\",\"PARTITION_NAME\" from " + PART_COL_STATS
                    + " where \"DB_NAME\" = ? and \"TABLE_NAME\" = ?" + " and \"COLUMN_NAME\" = ?"
                    + " and \"PARTITION_NAME\" in (" + makeParams(partNames.size()) + ")"
                    + " order by cast(\"" + colStatName + "\" as decimal)";
              }
              start = doTrace ? System.nanoTime() : 0;
              query = pm.newQuery("javax.jdo.query.SQL", queryText);
              qResult = executeWithArray(query,
                  prepareParams(dbName, tableName, partNames, Arrays.asList(colName)), queryText);
              if (qResult == null) {
                query.closeAll();
                return Collections.emptyList();
              }
              fqr = (ForwardQueryResult<?>) qResult;
              Object[] min = (Object[]) (fqr.get(0));
              Object[] max = (Object[]) (fqr.get(fqr.size() - 1));
              end = doTrace ? System.nanoTime() : 0;
              timingTrace(doTrace, queryText, start, end);
              query.closeAll();
              if (min[0] == null || max[0] == null) {
                row[2 + colStatIndex] = null;
              } else {
                row[2 + colStatIndex] = extrapolateMethod.extrapolate(min, max, colStatIndex,
                    indexMap);
              }
            } else {
              // if the aggregation type is avg, we use the average on the existing ones.
              queryText = "select "
                  + "avg((\"LONG_HIGH_VALUE\"-\"LONG_LOW_VALUE\")/cast(\"NUM_DISTINCTS\" as decimal)),"
                  + "avg((\"DOUBLE_HIGH_VALUE\"-\"DOUBLE_LOW_VALUE\")/\"NUM_DISTINCTS\"),"
                  + "avg((cast(\"BIG_DECIMAL_HIGH_VALUE\" as decimal)-cast(\"BIG_DECIMAL_LOW_VALUE\" as decimal))/\"NUM_DISTINCTS\")"
                  + " from " + PART_COL_STATS + "" + " where \"DB_NAME\" = ? and \"TABLE_NAME\" = ?"
                  + " and \"COLUMN_NAME\" = ?" + " and \"PARTITION_NAME\" in ("
                  + makeParams(partNames.size()) + ")" + " group by \"COLUMN_NAME\"";
              start = doTrace ? System.nanoTime() : 0;
              query = pm.newQuery("javax.jdo.query.SQL", queryText);
              qResult = executeWithArray(query,
                  prepareParams(dbName, tableName, partNames, Arrays.asList(colName)), queryText);
              if (qResult == null) {
                query.closeAll();
                return Collections.emptyList();
              }
              fqr = (ForwardQueryResult<?>) qResult;
              Object[] avg = (Object[]) (fqr.get(0));
              // colStatIndex=12,13,14 respond to "AVG_LONG", "AVG_DOUBLE",
              // "AVG_DECIMAL"
              row[2 + colStatIndex] = avg[colStatIndex - 12];
              end = doTrace ? System.nanoTime() : 0;
              timingTrace(doTrace, queryText, start, end);
              query.closeAll();
            }
          }
          colStats.add(prepareCSObjWithAdjustedNDV(row, 0, useDensityFunctionForNDVEstimation, ndvTuner));
          Deadline.checkTimeout();
        }
      }
      return colStats;
    }
  }

  private ColumnStatisticsObj prepareCSObj (Object[] row, int i) throws MetaException {
    ColumnStatisticsData data = new ColumnStatisticsData();
    ColumnStatisticsObj cso = new ColumnStatisticsObj((String)row[i++], (String)row[i++], data);
    Object llow = row[i++], lhigh = row[i++], dlow = row[i++], dhigh = row[i++],
        declow = row[i++], dechigh = row[i++], nulls = row[i++], dist = row[i++], bitVector = row[i++],
        avglen = row[i++], maxlen = row[i++], trues = row[i++], falses = row[i++];
    StatObjectConverter.fillColumnStatisticsData(cso.getColType(), data,
        llow, lhigh, dlow, dhigh, declow, dechigh, nulls, dist, bitVector, avglen, maxlen, trues, falses);
    return cso;
  }

  private ColumnStatisticsObj prepareCSObjWithAdjustedNDV(Object[] row, int i,
      boolean useDensityFunctionForNDVEstimation, double ndvTuner) throws MetaException {
    ColumnStatisticsData data = new ColumnStatisticsData();
    ColumnStatisticsObj cso = new ColumnStatisticsObj((String) row[i++], (String) row[i++], data);
    Object llow = row[i++], lhigh = row[i++], dlow = row[i++], dhigh = row[i++], declow = row[i++], dechigh = row[i++], nulls = row[i++], dist = row[i++], avglen = row[i++], maxlen = row[i++], trues = row[i++], falses = row[i++], avgLong = row[i++], avgDouble = row[i++], avgDecimal = row[i++], sumDist = row[i++];
    StatObjectConverter.fillColumnStatisticsData(cso.getColType(), data, llow, lhigh, dlow, dhigh,
        declow, dechigh, nulls, dist, avglen, maxlen, trues, falses, avgLong, avgDouble,
        avgDecimal, sumDist, useDensityFunctionForNDVEstimation, ndvTuner);
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

  public List<ColumnStatistics> getPartitionStats(final String dbName, final String tableName,
      final List<String> partNames, List<String> colNames, boolean enableBitVector) throws MetaException {
    if (colNames.isEmpty() || partNames.isEmpty()) {
      return Collections.emptyList();
    }
    final boolean doTrace = LOG.isDebugEnabled();
    final String queryText0 = "select \"PARTITION_NAME\", " + getStatsList(enableBitVector) + " from "
        + " " + PART_COL_STATS + " where \"DB_NAME\" = ? and \"TABLE_NAME\" = ? and \"COLUMN_NAME\""
        + "  in (%1$s) AND \"PARTITION_NAME\" in (%2$s) order by \"PARTITION_NAME\"";
    Batchable<String, Object[]> b = new Batchable<String, Object[]>() {
      @Override
      public List<Object[]> run(final List<String> inputColNames) throws MetaException {
        Batchable<String, Object[]> b2 = new Batchable<String, Object[]>() {
          @Override
          public List<Object[]> run(List<String> inputPartNames) throws MetaException {
            String queryText = String.format(queryText0,
                makeParams(inputColNames.size()), makeParams(inputPartNames.size()));
            long start = doTrace ? System.nanoTime() : 0;
            Query query = pm.newQuery("javax.jdo.query.SQL", queryText);
            Object qResult = executeWithArray(query, prepareParams(
                dbName, tableName, inputPartNames, inputColNames), queryText);
            timingTrace(doTrace, queryText0, start, (doTrace ? System.nanoTime() : 0));
            if (qResult == null) {
              query.closeAll();
              return Collections.emptyList();
            }
            addQueryAfterUse(query);
            return ensureList(qResult);
          }
        };
        try {
          return runBatched(partNames, b2);
        } finally {
          addQueryAfterUse(b2);
        }
      }
    };
    List<Object[]> list = runBatched(colNames, b);

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
      Deadline.checkTimeout();
    }
    b.closeAllQueries();
    return result;
  }

  /** The common query part for table and partition stats */
  private final String getStatsList(boolean enableBitVector) {
    return "\"COLUMN_NAME\", \"COLUMN_TYPE\", \"LONG_LOW_VALUE\", \"LONG_HIGH_VALUE\", "
        + "\"DOUBLE_LOW_VALUE\", \"DOUBLE_HIGH_VALUE\", \"BIG_DECIMAL_LOW_VALUE\", "
        + "\"BIG_DECIMAL_HIGH_VALUE\", \"NUM_NULLS\", \"NUM_DISTINCTS\", "
        + (enableBitVector ? "\"BIT_VECTOR\", " : "\'\', ") + "\"AVG_COL_LEN\", "
        + "\"MAX_COL_LEN\", \"NUM_TRUES\", \"NUM_FALSES\", \"LAST_ANALYZED\" ";
  }

  private ColumnStatistics makeColumnStats(
      List<Object[]> list, ColumnStatisticsDesc csd, int offset) throws MetaException {
    ColumnStatistics result = new ColumnStatistics();
    result.setStatsDesc(csd);
    List<ColumnStatisticsObj> csos = new ArrayList<ColumnStatisticsObj>(list.size());
    for (Object[] row : list) {
      // LastAnalyzed is stored per column but thrift has it per several;
      // get the lowest for now as nobody actually uses this field.
      Object laObj = row[offset + 15];
      if (laObj != null && (!csd.isSetLastAnalyzed() || csd.getLastAnalyzed() > extractSqlLong(laObj))) {
        csd.setLastAnalyzed(extractSqlLong(laObj));
      }
      csos.add(prepareCSObj(row, offset));
      Deadline.checkTimeout();
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

  /**
   * This run the necessary logic to prepare for queries. It should be called once, after the
   * txn on DataNucleus connection is opened, and before any queries are issued. What it does
   * currently is run db-specific logic, e.g. setting ansi quotes mode for MySQL. The reason it
   * must be used inside of the txn is connection pooling; there's no way to guarantee that the
   * effect will apply to the connection that is executing the queries otherwise.
   */
  public void prepareTxn() throws MetaException {
    if (dbType != DatabaseProduct.MYSQL) return;
    try {
      assert pm.currentTransaction().isActive(); // must be inside tx together with queries
      executeNoResult("SET @@session.sql_mode=ANSI_QUOTES");
    } catch (SQLException sqlEx) {
      throw new MetaException("Error setting ansi quotes: " + sqlEx.getMessage());
    }
  }


  private static abstract class Batchable<I, R> {
    private List<Query> queries = null;
    public abstract List<R> run(List<I> input) throws MetaException;
    public void addQueryAfterUse(Query query) {
      if (queries == null) {
        queries = new ArrayList<Query>(1);
      }
      queries.add(query);
    }
    protected void addQueryAfterUse(Batchable<?, ?> b) {
      if (b.queries == null) return;
      if (queries == null) {
        queries = new ArrayList<Query>(1);
      }
      queries.addAll(b.queries);
    }
    public void closeAllQueries() {
      for (Query q : queries) {
        try {
          q.closeAll();
        } catch (Throwable t) {
          LOG.error("Failed to close a query", t);
        }
      }
    }
  }

  private <I,R> List<R> runBatched(List<I> input, Batchable<I, R> runnable) throws MetaException {
    if (batchSize == NO_BATCHING || batchSize >= input.size()) {
      return runnable.run(input);
    }
    List<R> result = new ArrayList<R>(input.size());
    for (int fromIndex = 0, toIndex = 0; toIndex < input.size(); fromIndex = toIndex) {
      toIndex = Math.min(fromIndex + batchSize, input.size());
      List<I> batchedInput = input.subList(fromIndex, toIndex);
      List<R> batchedOutput = runnable.run(batchedInput);
      if (batchedOutput != null) {
        result.addAll(batchedOutput);
      }
    }
    return result;
  }

  public List<SQLForeignKey> getForeignKeys(String parent_db_name, String parent_tbl_name, String foreign_db_name, String foreign_tbl_name) throws MetaException {
    List<SQLForeignKey> ret = new ArrayList<SQLForeignKey>();
    String queryText =
      "SELECT  \"D2\".\"NAME\", \"T2\".\"TBL_NAME\", "
      + "CASE WHEN \"C2\".\"COLUMN_NAME\" IS NOT NULL THEN \"C2\".\"COLUMN_NAME\" "
      + "ELSE \"P2\".\"PKEY_NAME\" END, "
      + "" + DBS + ".\"NAME\", " + TBLS + ".\"TBL_NAME\", "
      + "CASE WHEN " + COLUMNS_V2 + ".\"COLUMN_NAME\" IS NOT NULL THEN " + COLUMNS_V2 + ".\"COLUMN_NAME\" "
      + "ELSE " + PARTITION_KEYS + ".\"PKEY_NAME\" END, "
      + "" + KEY_CONSTRAINTS + ".\"POSITION\", " + KEY_CONSTRAINTS + ".\"UPDATE_RULE\", " + KEY_CONSTRAINTS + ".\"DELETE_RULE\", "
      + "" + KEY_CONSTRAINTS + ".\"CONSTRAINT_NAME\" , \"KEY_CONSTRAINTS2\".\"CONSTRAINT_NAME\", " + KEY_CONSTRAINTS + ".\"ENABLE_VALIDATE_RELY\" "
      + " from " + TBLS + " "
      + " INNER JOIN " + KEY_CONSTRAINTS + " ON " + TBLS + ".\"TBL_ID\" = " + KEY_CONSTRAINTS + ".\"CHILD_TBL_ID\" "
      + " INNER JOIN " + KEY_CONSTRAINTS + " \"KEY_CONSTRAINTS2\" ON \"KEY_CONSTRAINTS2\".\"PARENT_TBL_ID\"  = " + KEY_CONSTRAINTS + ".\"PARENT_TBL_ID\" "
      + " AND \"KEY_CONSTRAINTS2\".\"PARENT_CD_ID\"  = " + KEY_CONSTRAINTS + ".\"PARENT_CD_ID\" AND "
      + " \"KEY_CONSTRAINTS2\".\"PARENT_INTEGER_IDX\"  = " + KEY_CONSTRAINTS + ".\"PARENT_INTEGER_IDX\" "
      + " INNER JOIN " + DBS + " ON " + TBLS + ".\"DB_ID\" = " + DBS + ".\"DB_ID\" "
      + " INNER JOIN " + TBLS + " \"T2\" ON  " + KEY_CONSTRAINTS + ".\"PARENT_TBL_ID\" = \"T2\".\"TBL_ID\" "
      + " INNER JOIN " + DBS + " \"D2\" ON \"T2\".\"DB_ID\" = \"D2\".\"DB_ID\" "
      + " LEFT OUTER JOIN " + COLUMNS_V2 + "  ON " + COLUMNS_V2 + ".\"CD_ID\" = " + KEY_CONSTRAINTS + ".\"CHILD_CD_ID\" AND "
      + " " + COLUMNS_V2 + ".\"INTEGER_IDX\" = " + KEY_CONSTRAINTS + ".\"CHILD_INTEGER_IDX\" "
      + " LEFT OUTER JOIN " + PARTITION_KEYS + " ON " + TBLS + ".\"TBL_ID\" = " + PARTITION_KEYS + ".\"TBL_ID\" AND "
      + " " + PARTITION_KEYS + ".\"INTEGER_IDX\" = " + KEY_CONSTRAINTS + ".\"CHILD_INTEGER_IDX\" "
      + " LEFT OUTER JOIN " + COLUMNS_V2 + " \"C2\" ON \"C2\".\"CD_ID\" = " + KEY_CONSTRAINTS + ".\"PARENT_CD_ID\" AND "
      + " \"C2\".\"INTEGER_IDX\" = " + KEY_CONSTRAINTS + ".\"PARENT_INTEGER_IDX\" "
      + " LEFT OUTER JOIN " + PARTITION_KEYS + " \"P2\" ON \"P2\".\"TBL_ID\" = " + TBLS + ".\"TBL_ID\" AND "
      + " \"P2\".\"INTEGER_IDX\" = " + KEY_CONSTRAINTS + ".\"PARENT_INTEGER_IDX\" "
      + " WHERE " + KEY_CONSTRAINTS + ".\"CONSTRAINT_TYPE\" = " + MConstraint.FOREIGN_KEY_CONSTRAINT
      + " AND \"KEY_CONSTRAINTS2\".\"CONSTRAINT_TYPE\" = " + MConstraint.PRIMARY_KEY_CONSTRAINT + " AND"
      + (foreign_db_name == null ? "" : " " + DBS + ".\"NAME\" = ? AND")
      + (foreign_tbl_name == null ? "" : " " + TBLS + ".\"TBL_NAME\" = ? AND")
      + (parent_tbl_name == null ? "" : " \"T2\".\"TBL_NAME\" = ? AND")
      + (parent_db_name == null ? "" : " \"D2\".\"NAME\" = ?") ;

    queryText = queryText.trim();
    if (queryText.endsWith("AND")) {
      queryText = queryText.substring(0, queryText.length()-3);
    }
    List<String> pms = new ArrayList<String>();
    if (foreign_db_name != null) {
      pms.add(foreign_db_name);
    }
    if (foreign_tbl_name != null) {
      pms.add(foreign_tbl_name);
    }
    if (parent_tbl_name != null) {
      pms.add(parent_tbl_name);
    }
    if (parent_db_name != null) {
      pms.add(parent_db_name);
    }

    Query queryParams = pm.newQuery("javax.jdo.query.SQL", queryText);
      List<Object[]> sqlResult = ensureList(executeWithArray(
        queryParams, pms.toArray(), queryText));

    if (!sqlResult.isEmpty()) {
      for (Object[] line : sqlResult) {
        int enableValidateRely = extractSqlInt(line[11]);
        boolean enable = (enableValidateRely & 4) != 0;
        boolean validate = (enableValidateRely & 2) != 0;
        boolean rely = (enableValidateRely & 1) != 0;
        SQLForeignKey currKey = new SQLForeignKey(
          extractSqlString(line[0]),
          extractSqlString(line[1]),
          extractSqlString(line[2]),
          extractSqlString(line[3]),
          extractSqlString(line[4]),
          extractSqlString(line[5]),
          extractSqlInt(line[6]),
          extractSqlInt(line[7]),
          extractSqlInt(line[8]),
          extractSqlString(line[9]),
          extractSqlString(line[10]),
          enable,
          validate,
          rely
          );
          ret.add(currKey);
      }
    }
    return ret;
  }

  public List<SQLPrimaryKey> getPrimaryKeys(String db_name, String tbl_name) throws MetaException {
    List<SQLPrimaryKey> ret = new ArrayList<SQLPrimaryKey>();
    String queryText =
      "SELECT " + DBS + ".\"NAME\", " + TBLS + ".\"TBL_NAME\", "
      + "CASE WHEN " + COLUMNS_V2 + ".\"COLUMN_NAME\" IS NOT NULL THEN " + COLUMNS_V2 + ".\"COLUMN_NAME\" "
      + "ELSE " + PARTITION_KEYS + ".\"PKEY_NAME\" END, " + KEY_CONSTRAINTS + ".\"POSITION\", "
      + "" + KEY_CONSTRAINTS + ".\"CONSTRAINT_NAME\", " + KEY_CONSTRAINTS + ".\"ENABLE_VALIDATE_RELY\" "
      + " from " + TBLS + " "
      + " INNER JOIN " + KEY_CONSTRAINTS + " ON " + TBLS + ".\"TBL_ID\" = " + KEY_CONSTRAINTS + ".\"PARENT_TBL_ID\" "
      + " INNER JOIN " + DBS + " ON " + TBLS + ".\"DB_ID\" = " + DBS + ".\"DB_ID\" "
      + " LEFT OUTER JOIN " + COLUMNS_V2 + " ON " + COLUMNS_V2 + ".\"CD_ID\" = " + KEY_CONSTRAINTS + ".\"PARENT_CD_ID\" AND "
      + " " + COLUMNS_V2 + ".\"INTEGER_IDX\" = " + KEY_CONSTRAINTS + ".\"PARENT_INTEGER_IDX\" "
      + " LEFT OUTER JOIN " + PARTITION_KEYS + " ON " + TBLS + ".\"TBL_ID\" = " + PARTITION_KEYS + ".\"TBL_ID\" AND "
      + " " + PARTITION_KEYS + ".\"INTEGER_IDX\" = " + KEY_CONSTRAINTS + ".\"PARENT_INTEGER_IDX\" "
      + " WHERE " + KEY_CONSTRAINTS + ".\"CONSTRAINT_TYPE\" = "+ MConstraint.PRIMARY_KEY_CONSTRAINT + " AND"
      + (db_name == null ? "" : " " + DBS + ".\"NAME\" = ? AND")
      + (tbl_name == null ? "" : " " + TBLS + ".\"TBL_NAME\" = ? ") ;

    queryText = queryText.trim();
    if (queryText.endsWith("AND")) {
      queryText = queryText.substring(0, queryText.length()-3);
    }
    List<String> pms = new ArrayList<String>();
    if (db_name != null) {
      pms.add(db_name);
    }
    if (tbl_name != null) {
      pms.add(tbl_name);
    }

    Query queryParams = pm.newQuery("javax.jdo.query.SQL", queryText);
      List<Object[]> sqlResult = ensureList(executeWithArray(
        queryParams, pms.toArray(), queryText));

    if (!sqlResult.isEmpty()) {
      for (Object[] line : sqlResult) {
          int enableValidateRely = extractSqlInt(line[5]);
          boolean enable = (enableValidateRely & 4) != 0;
          boolean validate = (enableValidateRely & 2) != 0;
          boolean rely = (enableValidateRely & 1) != 0;
        SQLPrimaryKey currKey = new SQLPrimaryKey(
          extractSqlString(line[0]),
          extractSqlString(line[1]),
          extractSqlString(line[2]),
          extractSqlInt(line[3]), extractSqlString(line[4]),
          enable,
          validate,
          rely);
          ret.add(currKey);
      }
    }
    return ret;
  }

  public List<SQLUniqueConstraint> getUniqueConstraints(String db_name, String tbl_name)
          throws MetaException {
    List<SQLUniqueConstraint> ret = new ArrayList<SQLUniqueConstraint>();
    String queryText =
      "SELECT " + DBS + ".\"NAME\", " + TBLS + ".\"TBL_NAME\", "
      + "CASE WHEN " + COLUMNS_V2 + ".\"COLUMN_NAME\" IS NOT NULL THEN " + COLUMNS_V2 + ".\"COLUMN_NAME\" "
      + "ELSE " + PARTITION_KEYS + ".\"PKEY_NAME\" END, " + KEY_CONSTRAINTS + ".\"POSITION\", "
      + KEY_CONSTRAINTS + ".\"CONSTRAINT_NAME\", " + KEY_CONSTRAINTS + ".\"ENABLE_VALIDATE_RELY\" "
      + " from " + TBLS + " "
      + " INNER JOIN " + KEY_CONSTRAINTS + " ON " + TBLS + ".\"TBL_ID\" = " + KEY_CONSTRAINTS + ".\"PARENT_TBL_ID\" "
      + " INNER JOIN " + DBS + " ON " + TBLS + ".\"DB_ID\" = " + DBS + ".\"DB_ID\" "
      + " LEFT OUTER JOIN " + COLUMNS_V2 + " ON " + COLUMNS_V2 + ".\"CD_ID\" = " + KEY_CONSTRAINTS + ".\"PARENT_CD_ID\" AND "
      + " " + COLUMNS_V2 + ".\"INTEGER_IDX\" = " + KEY_CONSTRAINTS + ".\"PARENT_INTEGER_IDX\" "
      + " LEFT OUTER JOIN " + PARTITION_KEYS + " ON " + TBLS + ".\"TBL_ID\" = " + PARTITION_KEYS + ".\"TBL_ID\" AND "
      + " " + PARTITION_KEYS + ".\"INTEGER_IDX\" = " + KEY_CONSTRAINTS + ".\"PARENT_INTEGER_IDX\" "
      + " WHERE " + KEY_CONSTRAINTS + ".\"CONSTRAINT_TYPE\" = "+ MConstraint.UNIQUE_CONSTRAINT + " AND"
      + (db_name == null ? "" : " " + DBS + ".\"NAME\" = ? AND")
      + (tbl_name == null ? "" : " " + TBLS + ".\"TBL_NAME\" = ? ") ;

    queryText = queryText.trim();
    if (queryText.endsWith("AND")) {
      queryText = queryText.substring(0, queryText.length()-3);
    }
    List<String> pms = new ArrayList<String>();
    if (db_name != null) {
      pms.add(db_name);
    }
    if (tbl_name != null) {
      pms.add(tbl_name);
    }

    Query queryParams = pm.newQuery("javax.jdo.query.SQL", queryText);
      List<Object[]> sqlResult = ensureList(executeWithArray(
        queryParams, pms.toArray(), queryText));

    if (!sqlResult.isEmpty()) {
      for (Object[] line : sqlResult) {
          int enableValidateRely = extractSqlInt(line[5]);
          boolean enable = (enableValidateRely & 4) != 0;
          boolean validate = (enableValidateRely & 2) != 0;
          boolean rely = (enableValidateRely & 1) != 0;
        SQLUniqueConstraint currConstraint = new SQLUniqueConstraint(
          extractSqlString(line[0]),
          extractSqlString(line[1]),
          extractSqlString(line[2]),
          extractSqlInt(line[3]), extractSqlString(line[4]),
          enable,
          validate,
          rely);
          ret.add(currConstraint);
      }
    }
    return ret;
  }

  public List<SQLNotNullConstraint> getNotNullConstraints(String db_name, String tbl_name)
          throws MetaException {
    List<SQLNotNullConstraint> ret = new ArrayList<SQLNotNullConstraint>();
    String queryText =
      "SELECT " + DBS + ".\"NAME\", " + TBLS + ".\"TBL_NAME\","
      + "CASE WHEN " + COLUMNS_V2 + ".\"COLUMN_NAME\" IS NOT NULL THEN " + COLUMNS_V2 + ".\"COLUMN_NAME\" "
      + "ELSE " + PARTITION_KEYS + ".\"PKEY_NAME\" END, "
      + "" + KEY_CONSTRAINTS + ".\"CONSTRAINT_NAME\", " + KEY_CONSTRAINTS + ".\"ENABLE_VALIDATE_RELY\" "
      + " from " + TBLS + " "
      + " INNER JOIN " + KEY_CONSTRAINTS + " ON " + TBLS + ".\"TBL_ID\" = " + KEY_CONSTRAINTS + ".\"PARENT_TBL_ID\" "
      + " INNER JOIN " + DBS + " ON " + TBLS + ".\"DB_ID\" = " + DBS + ".\"DB_ID\" "
      + " LEFT OUTER JOIN " + COLUMNS_V2 + " ON " + COLUMNS_V2 + ".\"CD_ID\" = " + KEY_CONSTRAINTS + ".\"PARENT_CD_ID\" AND "
      + " " + COLUMNS_V2 + ".\"INTEGER_IDX\" = " + KEY_CONSTRAINTS + ".\"PARENT_INTEGER_IDX\" "
      + " LEFT OUTER JOIN " + PARTITION_KEYS + " ON " + TBLS + ".\"TBL_ID\" = " + PARTITION_KEYS + ".\"TBL_ID\" AND "
      + " " + PARTITION_KEYS + ".\"INTEGER_IDX\" = " + KEY_CONSTRAINTS + ".\"PARENT_INTEGER_IDX\" "
      + " WHERE " + KEY_CONSTRAINTS + ".\"CONSTRAINT_TYPE\" = "+ MConstraint.NOT_NULL_CONSTRAINT + " AND"
      + (db_name == null ? "" : " " + DBS + ".\"NAME\" = ? AND")
      + (tbl_name == null ? "" : " " + TBLS + ".\"TBL_NAME\" = ? ") ;

    queryText = queryText.trim();
    if (queryText.endsWith("AND")) {
      queryText = queryText.substring(0, queryText.length()-3);
    }
    List<String> pms = new ArrayList<String>();
    if (db_name != null) {
      pms.add(db_name);
    }
    if (tbl_name != null) {
      pms.add(tbl_name);
    }

    Query queryParams = pm.newQuery("javax.jdo.query.SQL", queryText);
      List<Object[]> sqlResult = ensureList(executeWithArray(
        queryParams, pms.toArray(), queryText));

    if (!sqlResult.isEmpty()) {
      for (Object[] line : sqlResult) {
          int enableValidateRely = extractSqlInt(line[4]);
          boolean enable = (enableValidateRely & 4) != 0;
          boolean validate = (enableValidateRely & 2) != 0;
          boolean rely = (enableValidateRely & 1) != 0;
        SQLNotNullConstraint currConstraint = new SQLNotNullConstraint(
          extractSqlString(line[0]),
          extractSqlString(line[1]),
          extractSqlString(line[2]),
          extractSqlString(line[3]),
          enable,
          validate,
          rely);
          ret.add(currConstraint);
      }
    }
    return ret;
  }

}
