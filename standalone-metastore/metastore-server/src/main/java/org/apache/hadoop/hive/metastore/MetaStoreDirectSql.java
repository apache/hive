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

import static org.apache.commons.lang3.StringUtils.join;
import static org.apache.commons.lang3.StringUtils.normalizeSpace;
import static org.apache.commons.lang3.StringUtils.repeat;
import static org.apache.hadoop.hive.metastore.ColumnType.BIGINT_TYPE_NAME;
import static org.apache.hadoop.hive.metastore.ColumnType.CHAR_TYPE_NAME;
import static org.apache.hadoop.hive.metastore.ColumnType.DATE_TYPE_NAME;
import static org.apache.hadoop.hive.metastore.ColumnType.INT_TYPE_NAME;
import static org.apache.hadoop.hive.metastore.ColumnType.SMALLINT_TYPE_NAME;
import static org.apache.hadoop.hive.metastore.ColumnType.STRING_TYPE_NAME;
import static org.apache.hadoop.hive.metastore.ColumnType.TIMESTAMP_TYPE_NAME;
import static org.apache.hadoop.hive.metastore.ColumnType.TINYINT_TYPE_NAME;
import static org.apache.hadoop.hive.metastore.ColumnType.VARCHAR_TYPE_NAME;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;
import javax.jdo.Transaction;
import javax.jdo.datastore.JDOConnection;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.AggregateStatsCache.AggrColStats;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DatabaseType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.GetPartitionsFilterSpec;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.GetPartitionsArgs;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.model.MConstraint;
import org.apache.hadoop.hive.metastore.model.MCreationMetadata;
import org.apache.hadoop.hive.metastore.model.MDatabase;
import org.apache.hadoop.hive.metastore.model.MNotificationLog;
import org.apache.hadoop.hive.metastore.model.MNotificationNextId;
import org.apache.hadoop.hive.metastore.model.MPartition;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnPrivilege;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnStatistics;
import org.apache.hadoop.hive.metastore.model.MPartitionPrivilege;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.model.MTableColumnStatistics;
import org.apache.hadoop.hive.metastore.model.MWMResourcePlan;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.FilterBuilder;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.LeafNode;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.LogicalOperator;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.Operator;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.TreeNode;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.TreeVisitor;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.ColStatsObjWithSourceInfo;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hive.common.util.BloomFilter;
import org.datanucleus.store.rdbms.query.ForwardQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
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
  private final Configuration conf;
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
  private final boolean isTxnStatsEnabled;

  /**
   * Whether direct SQL can be used with the current datastore backing {@link #pm}.
   */
  private final boolean isCompatibleDatastore;
  private final boolean isAggregateStatsCacheEnabled;
  private final ImmutableMap<String, String> fieldnameToTableName;
  private AggregateStatsCache aggrStatsCache;
  private DirectSqlUpdatePart directSqlUpdatePart;
  private DirectSqlInsertPart directSqlInsertPart;

  /**
   * This method returns a comma separated string consisting of String values of a given list.
   * This is used for preparing "SOMETHING_ID in (...)" to use in SQL queries.
   * @param objectIds the objectId collection
   * @return The concatenated list
   * @throws MetaException If the list contains wrong data
   */
  public static <T> String getIdListForIn(Collection<T> objectIds) throws MetaException {
    return objectIds.stream()
               .map(i -> i.toString())
               .collect(Collectors.joining(","));
  }

  @java.lang.annotation.Target(java.lang.annotation.ElementType.FIELD)
  @java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
  private @interface TableName {}

  // Table names with schema name, if necessary
  @TableName
  private String DBS, TBLS, PARTITIONS, DATABASE_PARAMS, PARTITION_PARAMS, SORT_COLS, SD_PARAMS,
      SDS, SERDES, SKEWED_STRING_LIST_VALUES, SKEWED_VALUES, BUCKETING_COLS, SKEWED_COL_NAMES,
      SKEWED_COL_VALUE_LOC_MAP, COLUMNS_V2, PARTITION_KEYS, SERDE_PARAMS, PART_COL_STATS, KEY_CONSTRAINTS,
      TAB_COL_STATS, PARTITION_KEY_VALS, PART_PRIVS, PART_COL_PRIVS, SKEWED_STRING_LIST, CDS,
      TBL_COL_PRIVS, FUNCS, FUNC_RU;

  public MetaStoreDirectSql(PersistenceManager pm, Configuration conf, String schema) {
    this.pm = pm;
    this.conf = conf;
    this.schema = schema;
    DatabaseProduct dbType = null;

    dbType = DatabaseProduct.determineDatabaseProduct(getProductName(pm), conf);

    this.dbType = dbType;
    int batchSize = MetastoreConf.getIntVar(conf, ConfVars.DIRECT_SQL_PARTITION_BATCH_SIZE);
    this.directSqlInsertPart = new DirectSqlInsertPart(pm, dbType, batchSize);
    if (batchSize == DETECT_BATCHING) {
      batchSize = dbType.needsInBatching() ? 1000 : NO_BATCHING;
    }
    this.batchSize = batchSize;
    this.isTxnStatsEnabled = MetastoreConf.getBoolVar(conf, ConfVars.HIVE_TXN_STATS_ENABLED);
    this.directSqlUpdatePart = new DirectSqlUpdatePart(pm, conf, dbType, batchSize);
    ImmutableMap.Builder<String, String> fieldNameToTableNameBuilder =
        new ImmutableMap.Builder<>();

    for (java.lang.reflect.Field f : this.getClass().getDeclaredFields()) {
      if (f.getAnnotation(TableName.class) == null) {
        continue;
      }
      try {
        String value = getFullyQualifiedName(schema, f.getName());
        f.set(this, value);
        fieldNameToTableNameBuilder.put(f.getName(), value);
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
      boolean isInTest = MetastoreConf.getBoolVar(conf, ConfVars.HIVE_IN_TEST);
      isCompatibleDatastore = (!isInTest || ensureDbInit()) && runTestQuery();
      if (isCompatibleDatastore) {
        LOG.debug("Using direct SQL, underlying DB is " + dbType);
      }
    }

    isAggregateStatsCacheEnabled = MetastoreConf.getBoolVar(
        conf, ConfVars.AGGREGATE_STATS_CACHE_ENABLED);
    if (isAggregateStatsCacheEnabled) {
      aggrStatsCache = AggregateStatsCache.getInstance(conf);
    }

    // now use the tableanames to create the mapping
    // note that some of the optional single-valued fields are not present
    fieldnameToTableName =
        fieldNameToTableNameBuilder
            .put("createTime", PARTITIONS + ".\"CREATE_TIME\"")
            .put("lastAccessTime", PARTITIONS + ".\"LAST_ACCESS_TIME\"")
            .put("writeId", PARTITIONS + ".\"WRITE_ID\"")
            .put("sd.location", SDS + ".\"LOCATION\"")
            .put("sd.inputFormat", SDS + ".\"INPUT_FORMAT\"")
            .put("sd.outputFormat", SDS + ".\"OUTPUT_FORMAT\"")
            .put("sd.storedAsSubDirectories", SDS + ".\"IS_STOREDASSUBDIRECTORIES\"")
            .put("sd.compressed", SDS + ".\"IS_COMPRESSED\"")
            .put("sd.numBuckets", SDS + ".\"NUM_BUCKETS\"")
            .put("sd.serdeInfo.name", SERDES + ".\"NAME\"")
            .put("sd.serdeInfo.serializationLib", SERDES + ".\"SLIB\"")
            .put("PART_ID", PARTITIONS + ".\"PART_ID\"")
            .put("SD_ID", SDS + ".\"SD_ID\"")
            .put("SERDE_ID", SERDES + ".\"SERDE_ID\"")
            .put("CD_ID", SDS + ".\"CD_ID\"")
            .build();
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
      initQueries.add(pm.newQuery(MPartitionPrivilege.class, "principalName == ''"));
      initQueries.add(pm.newQuery(MPartitionColumnPrivilege.class, "principalName == ''"));

      for (Query q : initQueries) {
        q.execute();
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
    boolean doTrace = LOG.isDebugEnabled();
    Transaction tx = pm.currentTransaction();
    boolean doCommit = false;
    if (!tx.isActive()) {
      tx.begin();
      doCommit = true;
    }
    // Run a self-test query. If it doesn't work, we will self-disable. What a PITA...
    String selfTestQuery = "select \"DB_ID\" from " + DBS + " WHERE \"DB_ID\"=1";
    try (QueryWrapper query = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", selfTestQuery))) {
      prepareTxn();
      long start = doTrace ? System.nanoTime() : 0;
      query.execute();
      MetastoreDirectSqlUtils.timingTrace(doTrace, selfTestQuery, start, doTrace ? System.nanoTime() : 0);
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
      MetastoreDirectSqlUtils.timingTrace(doTrace, queryText, start, doTrace ? System.nanoTime() : 0);
    } finally {
      if(statement != null){
          statement.close();
      }
      jdoConn.close(); // We must release the connection before we call other pm methods.
    }
  }

  public Database getDatabase(String catName, String dbName) throws MetaException{
    String queryTextDbSelector= "select "
        + "\"DB_ID\", \"NAME\", \"DB_LOCATION_URI\", \"DESC\", "
        + "\"OWNER_NAME\", \"OWNER_TYPE\", \"CTLG_NAME\" , \"CREATE_TIME\", \"DB_MANAGED_LOCATION_URI\", "
        + "\"TYPE\", \"DATACONNECTOR_NAME\", \"REMOTE_DBNAME\" "
        + "FROM "+ DBS
        + " where \"NAME\" = ? and \"CTLG_NAME\" = ? ";
    String queryTextDbParams = "select \"PARAM_KEY\", \"PARAM_VALUE\" "
        + " from " + DATABASE_PARAMS + " "
        + " WHERE \"DB_ID\" = ? "
        + " AND \"PARAM_KEY\" IS NOT NULL";

    try (QueryWrapper queryDbSelector = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryTextDbSelector));
         QueryWrapper queryDbParams = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryTextDbParams))) {
      dbName = dbName.toLowerCase();
      catName = catName.toLowerCase();
      Object[] params = new Object[] { dbName, catName };

      if (LOG.isTraceEnabled()) {
        LOG.trace("getDatabase:query instantiated : " + queryTextDbSelector
            + " with param [" + params[0] + "]");
      }

      List<Object[]> sqlResult = executeWithArray(
          queryDbSelector.getInnerQuery(), params, queryTextDbSelector);
      if ((sqlResult == null) || sqlResult.isEmpty()) {
        return null;
      }

      assert(sqlResult.size() == 1);
      if (sqlResult.get(0) == null) {
        return null;
      }

      Object[] dbline = sqlResult.get(0);
      Long dbid = MetastoreDirectSqlUtils.extractSqlLong(dbline[0]);

      params[0] = dbid;
      if (LOG.isTraceEnabled()) {
        LOG.trace("getDatabase:query2 instantiated : " + queryTextDbParams
            + " with param [" + params[0] + "]");
      }

      Map<String,String> dbParams = new HashMap<String,String>();
      List<Object[]> sqlResult2 = MetastoreDirectSqlUtils.ensureList(executeWithArray(
          queryDbParams.getInnerQuery(), params, queryTextDbParams));
      if (!sqlResult2.isEmpty()) {
        for (Object[] line : sqlResult2) {
          dbParams.put(MetastoreDirectSqlUtils.extractSqlString(line[0]), MetastoreDirectSqlUtils
              .extractSqlString(line[1]));
        }
      }
      Database db = new Database();
      db.setName(MetastoreDirectSqlUtils.extractSqlString(dbline[1]));
      db.setLocationUri(MetastoreDirectSqlUtils.extractSqlString(dbline[2]));
      db.setDescription(MetastoreDirectSqlUtils.extractSqlString(dbline[3]));
      db.setOwnerName(MetastoreDirectSqlUtils.extractSqlString(dbline[4]));
      String type = MetastoreDirectSqlUtils.extractSqlString(dbline[5]);
      db.setOwnerType(
          (null == type || type.trim().isEmpty()) ? null : PrincipalType.valueOf(type));
      db.setCatalogName(MetastoreDirectSqlUtils.extractSqlString(dbline[6]));
      if (dbline[7] != null) {
        db.setCreateTime(MetastoreDirectSqlUtils.extractSqlInt(dbline[7]));
      }
      db.setManagedLocationUri(MetastoreDirectSqlUtils.extractSqlString(dbline[8]));
      String dbType = MetastoreDirectSqlUtils.extractSqlString(dbline[9]);
      if (dbType != null && dbType.equalsIgnoreCase(DatabaseType.REMOTE.name())) {
        db.setType(DatabaseType.REMOTE);
        db.setConnector_name(MetastoreDirectSqlUtils.extractSqlString(dbline[10]));
        db.setRemote_dbname(MetastoreDirectSqlUtils.extractSqlString(dbline[11]));
      } else {
        db.setType(DatabaseType.NATIVE);
      }
      db.setParameters(MetaStoreServerUtils.trimMapNulls(dbParams,convertMapNullsToEmptyStrings));
      if (LOG.isDebugEnabled()){
        LOG.debug("getDatabase: directsql returning db " + db.getName()
            + " locn["+db.getLocationUri()  +"] desc [" +db.getDescription()
            + "] owner [" + db.getOwnerName() + "] ownertype ["+ db.getOwnerType() +"]");
      }
      return db;
    }
  }

  /**
   * Get table names by using direct SQL queries.
   * @param catName catalog name
   * @param dbName Metastore database namme
   * @param tableType Table type, or null if we want to get all tables
   * @return list of table names
   */
  public List<String> getTables(String catName, String dbName, TableType tableType, int limit)
      throws MetaException {
    String queryText = "SELECT " + TBLS + ".\"TBL_NAME\""
      + " FROM " + TBLS + " "
      + " INNER JOIN " + DBS + " ON " + TBLS + ".\"DB_ID\" = " + DBS + ".\"DB_ID\" "
      + " WHERE " + DBS + ".\"NAME\" = ? AND " + DBS + ".\"CTLG_NAME\" = ? "
      + (tableType == null ? "" : "AND " + TBLS + ".\"TBL_TYPE\" = ? ") ;


    List<String> pms = new ArrayList<>();
    pms.add(dbName);
    pms.add(catName);
    if (tableType != null) {
      pms.add(tableType.toString());
    }

    try (QueryWrapper queryParams = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
      List<String> tableNames = executeWithArray(
          queryParams.getInnerQuery(), pms.toArray(), queryText, limit);
      List<String> results = new ArrayList<String>(tableNames);
      return results;
    }
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

    try (QueryWrapper queryParams = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
      List<String> mvs = executeWithArray(
          queryParams.getInnerQuery(), pms.toArray(), queryText);
      List<String> results = new ArrayList<String>(mvs);
      return results;
    }
  }

  /**
   * Add partitions in batch using direct SQL
   * @param parts list of partitions
   * @param partPrivilegesList list of partition privileges
   * @param partColPrivilegesList list of partition column privileges
   * @throws MetaException
   */
  public void addPartitions(List<MPartition> parts, List<List<MPartitionPrivilege>> partPrivilegesList,
      List<List<MPartitionColumnPrivilege>> partColPrivilegesList) throws MetaException {
    directSqlInsertPart.addPartitions(parts, partPrivilegesList, partColPrivilegesList);
  }

  /**
   * Alter partitions in batch using direct SQL
   * @param table the target table
   * @param partNames list of partition names
   * @param newParts list of new partitions
   * @param queryWriteIdList valid write id list
   * @return
   * @throws MetaException
   */
  public List<Partition> alterPartitions(MTable table, List<String> partNames,
                                         List<Partition> newParts, String queryWriteIdList) throws MetaException {
    List<Object[]> rows = Batchable.runBatched(batchSize, partNames, new Batchable<String, Object[]>() {
      @Override
      public List<Object[]> run(List<String> input) throws Exception {
        String filter = "" + PARTITIONS + ".\"PART_NAME\" in (" + makeParams(input.size()) + ")";
        List<String> columns = Arrays.asList("\"PART_ID\"", "\"PART_NAME\"", "\"SD_ID\"", "\"WRITE_ID\"");
        return getPartitionFieldsViaSqlFilter(table.getDatabase().getCatalogName(), table.getDatabase().getName(),
                table.getTableName(), columns, filter, input, Collections.emptyList(), null);
      }
    });
    Map<List<String>, Long> partValuesToId = new HashMap<>();
    Map<Long, Long> partIdToSdId = new HashMap<>();
    Map<Long, Long> partIdToWriteId = new HashMap<>();
    for (Object[] row : rows) {
      Long partId = MetastoreDirectSqlUtils.extractSqlLong(row[0]);
      Long sdId = MetastoreDirectSqlUtils.extractSqlLong(row[2]);
      Long writeId = MetastoreDirectSqlUtils.extractSqlLong(row[3]);
      partIdToSdId.put(partId, sdId);
      partIdToWriteId.put(partId, writeId);
      List<String> partValues = Warehouse.getPartValuesFromPartName((String) row[1]);
      partValuesToId.put(partValues, partId);
    }

    boolean isTxn = TxnUtils.isTransactionalTable(table.getParameters());
    for (Partition newPart : newParts) {
      Long partId = partValuesToId.get(newPart.getValues());
      boolean useOldWriteId = true;
      // If transactional, add/update the MUPdaterTransaction
      // for the current updater query.
      if (isTxn) {
        if (!isTxnStatsEnabled) {
          StatsSetupConst.setBasicStatsState(newPart.getParameters(), StatsSetupConst.FALSE);
        } else if (queryWriteIdList != null && newPart.getWriteId() > 0) {
          // Check concurrent INSERT case and set false to the flag.
          if (!ObjectStore.isCurrentStatsValidForTheQuery(newPart.getParameters(),
              partIdToWriteId.get(partId), queryWriteIdList, true)) {
            StatsSetupConst.setBasicStatsState(newPart.getParameters(), StatsSetupConst.FALSE);
            LOG.info("Removed COLUMN_STATS_ACCURATE from the parameters of the partition " +
                Warehouse.getQualifiedName(newPart) + " will be made persistent.");
          }
          useOldWriteId = false;
        }
      }

      if (useOldWriteId) {
        newPart.setWriteId(partIdToWriteId.get(partId));
      }
    }

    directSqlUpdatePart.alterPartitions(partValuesToId, partIdToSdId, newParts);
    return newParts;
  }

  /**
   * Get partition names by using direct SQL queries.
   * @param filter filter to use with direct sql
   * @param partitionKeys partition columns
   * @param defaultPartName default partition name
   * @param order the specification for ordering partition names
   * @param max maximum number of partition names to return
   * @return list of partition names
   */
  public List<String> getPartitionNamesViaSql(SqlFilterForPushdown filter, List<FieldSchema> partitionKeys,
      String defaultPartName, String order, Integer max) throws MetaException {
    boolean doTrace = LOG.isDebugEnabled();
    List<Object[]> orderSpecs = MetaStoreUtils.makeOrderSpecs(order);
    String catName = filter.catName.toLowerCase(), dbName = filter.dbName.toLowerCase(),
        tblName = filter.tableName.toLowerCase(), sqlFilter = filter.filter;
    List<Object> paramsForFilter = filter.params;
    List<String> joins =  filter.joins;
    if (joins.isEmpty()) {
      for (int i = 0; i < partitionKeys.size(); i++) {
        joins.add(null);
      }
    }

    StringBuilder orderColumns = new StringBuilder(), orderClause = new StringBuilder();
    int i = 0;
    List<Object> paramsForOrder = new ArrayList<Object>();
    boolean dbHasJoinCastBug = dbType.hasJoinOperationOrderBug();
    for (Object[] orderSpec: orderSpecs) {
      int partColIndex = (int)orderSpec[0];
      String orderAlias = "ODR" + (i++);
      String tableValue, tableAlias;
      if (joins.get(partColIndex) == null) {
        tableAlias = "ORDER"  + partColIndex;
        joins.set(partColIndex, "inner join " + PARTITION_KEY_VALS + " \"" + tableAlias
            + "\" on \""  + tableAlias + "\".\"PART_ID\" = " + PARTITIONS + ".\"PART_ID\""
            + " and \"" + tableAlias + "\".\"INTEGER_IDX\" = " + partColIndex);
        tableValue = " \"" + tableAlias + "\".\"PART_KEY_VAL\" ";
      } else {
        tableAlias = "FILTER" + partColIndex;
        tableValue = " \"" + tableAlias + "\".\"PART_KEY_VAL\" ";
      }

      String tableColumn = tableValue;
      String colType = partitionKeys.get(partColIndex).getType();
      PartitionFilterGenerator.FilterType type =
          PartitionFilterGenerator.FilterType.fromType(colType);
      if (type == PartitionFilterGenerator.FilterType.Date) {
      	tableValue = dbType.toDate(tableValue);
      } else if (type == PartitionFilterGenerator.FilterType.Timestamp) {
        tableValue = dbType.toTimestamp(tableValue);
      } else if (type == PartitionFilterGenerator.FilterType.Integral) {
        tableValue = "CAST(" + tableColumn + " AS decimal(21,0))";
      }
      String tableValue0 = tableValue;
      tableValue = " (case when " + tableColumn + " <> ?";
      paramsForOrder.add(defaultPartName);
      if (dbHasJoinCastBug) {
        tableValue += (" and " + TBLS + ".\"TBL_NAME\" = ? and " + DBS + ".\"NAME\" = ? and "
            + DBS + ".\"CTLG_NAME\" = ? and "
            + "\"" + tableAlias + "\".\"PART_ID\" = " + PARTITIONS + ".\"PART_ID\" and "
            + "\"" + tableAlias + "\".\"INTEGER_IDX\" = " + partColIndex);
        paramsForOrder.add(tblName);
        paramsForOrder.add(dbName);
        paramsForOrder.add(catName);
      }
      tableValue += " then " + tableValue0 + " else null end) AS \"" + orderAlias + "\" ";
      orderColumns.append(tableValue).append(",");
      orderClause.append(" \"").append(orderAlias).append("\" ")
          .append((String)orderSpec[1]).append(",");
    }

    for (int j = 0; j < joins.size(); j++) {
      if (joins.get(j) == null) {
        joins.remove(j--);
      }
    }
    if (orderClause.length() > 0) {
      orderClause.setLength(orderClause.length() - 1);
      orderColumns.setLength(orderColumns.length() - 1);
    }

    String orderCls = " order by " +
        (orderClause.length() > 0 ? orderClause.toString() : "\"PART_NAME\" asc");
    String columns = orderColumns.length() > 0 ? (", " + orderColumns.toString()) : "";
    String queryText =
        "select " + PARTITIONS + ".\"PART_NAME\"" + columns + " from " + PARTITIONS + " "
            + "  inner join " + TBLS + " on " + PARTITIONS + ".\"TBL_ID\" = " + TBLS + ".\"TBL_ID\" "
            + "  and " + TBLS + ".\"TBL_NAME\" = ? "
            + "  inner join " + DBS + " on " + TBLS + ".\"DB_ID\" = " + DBS + ".\"DB_ID\" "
            + "  and " + DBS + ".\"NAME\" = ? "
            + join(joins, ' ')
            + " where " + DBS + ".\"CTLG_NAME\" = ? "
            + (StringUtils.isBlank(sqlFilter) ? "" : (" and " + sqlFilter)) + orderCls;

    Object[] params = new Object[paramsForFilter.size() + paramsForOrder.size() + 3];
    i = 0;
    for (; i < paramsForOrder.size(); i++) {
      params[i] = paramsForOrder.get(i);
    }
    params[i] = tblName;
    params[i+1] = dbName;
    params[i+2] = catName;
    for (int j = 0; j < paramsForFilter.size(); j++) {
      params[i + j + 3] = paramsForFilter.get(j);
    }

    List<String> partNames = new LinkedList<String>();
    int limit = (max == null ? -1 : max);
    try (QueryWrapper query = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
      long start = doTrace ? System.nanoTime() : 0;
      List<Object> sqlResult = executeWithArray(query.getInnerQuery(), params, queryText, limit);
      long queryTime = doTrace ? System.nanoTime() : 0;
      MetastoreDirectSqlUtils.timingTrace(doTrace, queryText, start, queryTime);
      for (Object result : sqlResult) {
        Object obj = !columns.isEmpty() ? ((Object[]) result)[0] : result;
        partNames.add((String)obj);
      }
    }
    return partNames;
  }


  /**
   * Gets partitions by using direct SQL queries.
   * @param catName Metastore catalog name.
   * @param dbName Metastore db name.
   * @param tblName Metastore table name.
   * @param args additional arguments for getting partitions
   * @return List of partitions.
   */
  public List<Partition> getPartitionsViaPartNames(final String catName, final String dbName,
      final String tblName, GetPartitionsArgs args)
      throws MetaException {
    List<String> partNames = args.getPartNames();
    if (partNames.isEmpty()) {
      return Collections.emptyList();
    }
    return Batchable.runBatched(batchSize, partNames, new Batchable<String, Partition>() {
      @Override
      public List<Partition> run(List<String> input) throws MetaException {
        return getPartitionsByNames(catName, dbName, tblName, partNames, false, args);
      }
    });
  }

  /**
   * Gets partitions by using direct SQL queries.
   * @param filter The filter.
   * @param isAcidTable True if the table is ACID
   * @param args additional arguments for getting partitions
   * @return List of partitions.
   */
  public List<Partition> getPartitionsViaSqlFilter(String catName, String dbName, String tableName,
      SqlFilterForPushdown filter, boolean isAcidTable,
      GetPartitionsArgs args) throws MetaException {
    List<Long> partitionIds = getPartitionIdsViaSqlFilter(catName,
        dbName, tableName, filter.filter, filter.params,
        filter.joins, args.getMax());
    if (partitionIds.isEmpty()) {
      return Collections.emptyList(); // no partitions, bail early.
    }
    return Batchable.runBatched(batchSize, partitionIds, new Batchable<Long, Partition>() {
      @Override
      public List<Partition> run(List<Long> input) throws MetaException {
        return getPartitionsByPartitionIds(catName, dbName, tableName, input, isAcidTable, args);
      }
    });
  }

  /**
   * This method can be used to return "partially-filled" partitions when clients are only interested in
   * some fields of the Partition objects. The partitionFields parameter is a list of dot separated
   * partition field names. For example, if a client is interested in only partition location,
   * serializationLib, values and parameters it can specify sd.location, sd.serdeInfo.serializationLib,
   * values, parameters in the partitionFields list. In such a case all the returned partitions will have
   * only the requested fields set and the rest of the fields will remain unset. The implementation of this method
   * runs queries only for the fields which are requested and pushes down the projection to the database to improve
   * performance.
   *
   * @param tbl                    Table whose partitions are being requested
   * @param partitionFields        List of dot separated field names. Each dot separated string represents nested levels. For
   *                               instance sd.serdeInfo.serializationLib represents the serializationLib field of the StorageDescriptor
   *                               for a the partition
   * @param includeParamKeyPattern The SQL regex pattern which is used to include the parameter keys. Can include _ or %
   *                               When this pattern is set, only the partition parameter key-value pairs where the key matches
   *                               the pattern will be returned. This is applied in conjunction with excludeParamKeyPattern if it is set.
   * @param excludeParamKeyPattern The SQL regex paterrn which is used to exclude the parameter keys. Can include _ or %
   *                               When this pattern is set, all the partition parameters where key is NOT LIKE the pattern
   *                               are returned. This is applied in conjunction with the includeParamKeyPattern if it is set.
   * @param filterSpec             The filterSpec from <code>GetPartitionsRequest</code> which includes the filter mode (BY_EXPR, BY_VALUES or BY_NAMES)
   *                               and the list of filter strings to be used to filter the results
   * @param filter                 SqlFilterForPushDown which is set in the <code>canUseDirectSql</code> method before this method is called.
   *                               The filter is used only when the mode is BY_EXPR
   * @return
   * @throws MetaException
   */
  public List<Partition> getPartitionsUsingProjectionAndFilterSpec(Table tbl,
      final List<String> partitionFields, final String includeParamKeyPattern,
      final String excludeParamKeyPattern, GetPartitionsFilterSpec filterSpec, SqlFilterForPushdown filter)
      throws MetaException {
    final String tblName = tbl.getTableName();
    final String dbName = tbl.getDbName();
    final String catName = tbl.getCatName();
    List<Long> partitionIds = null;
    if (filterSpec.isSetFilterMode()) {
      List<String> filters = filterSpec.getFilters();
      if (filters == null || filters.isEmpty()) {
        throw new MetaException("Invalid filter expressions in the filter spec");
      }
      switch(filterSpec.getFilterMode()) {
      case BY_EXPR:
        partitionIds =
            getPartitionIdsViaSqlFilter(catName, dbName, tblName, filter.filter, filter.params,
                filter.joins, null);
        break;
      case BY_NAMES:
        String partNamesFilter =
            "" + PARTITIONS + ".\"PART_NAME\" in (" + makeParams(filterSpec.getFilters().size())
                + ")";
        partitionIds = getPartitionIdsViaSqlFilter(catName, dbName, tblName, partNamesFilter,
            filterSpec.getFilters(), Collections.EMPTY_LIST, null);
        break;
      case BY_VALUES:
        // we are going to use the SQL regex pattern in the LIKE clause below. So the default string
        // is _% and not .*
        String partNameMatcher = MetaStoreUtils.makePartNameMatcher(tbl, filters, "_%");
        String partNamesLikeFilter =
            "" + PARTITIONS + ".\"PART_NAME\" LIKE (?)";
        partitionIds =
            getPartitionIdsViaSqlFilter(catName, dbName, tblName, partNamesLikeFilter, Arrays.asList(partNameMatcher),
                Collections.EMPTY_LIST, null);
        break;
        default:
          throw new MetaException("Unsupported filter mode " + filterSpec.getFilterMode());
      }
    } else {
      // there is no filter mode. Fetch all the partition ids
      partitionIds =
          getPartitionIdsViaSqlFilter(catName, dbName, tblName, null, Collections.EMPTY_LIST,
              Collections.EMPTY_LIST, null);
    }

    if (partitionIds.isEmpty()) {
      return Collections.emptyList();
    }
    // check if table object has table type as view
    Boolean isView = isViewTable(tbl);
    if (isView == null) {
      isView = isViewTable(catName, dbName, tblName);
    }
    PartitionProjectionEvaluator projectionEvaluator =
        new PartitionProjectionEvaluator(pm, fieldnameToTableName, partitionFields,
            convertMapNullsToEmptyStrings, isView, includeParamKeyPattern, excludeParamKeyPattern);
    // Get full objects. For Oracle/etc. do it in batches.
    return Batchable.runBatched(batchSize, partitionIds, new Batchable<Long, Partition>() {
      @Override
      public List<Partition> run(List<Long> input) throws MetaException {
        return projectionEvaluator.getPartitionsUsingProjectionList(input);
      }
    });
  }

  public static class SqlFilterForPushdown {
    private final List<Object> params = new ArrayList<>();
    private final List<String> joins = new ArrayList<>();
    private String filter;
    private String catName;
    private String dbName;
    private String tableName;
    // whether should compact null elements in joins when generating sql filter.
    private boolean compactJoins = true;
    SqlFilterForPushdown() {

    }
    SqlFilterForPushdown(Table table, boolean compactJoins) {
      this.catName = table.getCatName();
      this.dbName = table.getDbName();
      this.tableName = table.getTableName();
      this.compactJoins = compactJoins;
    }
  }

  public boolean generateSqlFilterForPushdown(String catName, String dbName, String tableName,
      List<FieldSchema> partitionKeys, ExpressionTree tree, String defaultPartitionName,
      SqlFilterForPushdown result) throws MetaException {
    // Derby and Oracle do not interpret filters ANSI-properly in some cases and need a workaround.
    assert partitionKeys != null;
    boolean dbHasJoinCastBug = dbType.hasJoinOperationOrderBug();
    result.tableName = tableName;
    result.dbName = dbName;
    result.catName = catName;
    result.filter = PartitionFilterGenerator.generateSqlFilter(catName, dbName, tableName,
        partitionKeys, tree, result.params, result.joins, dbHasJoinCastBug,
        ((defaultPartitionName == null) ? defaultPartName : defaultPartitionName),
        dbType, schema, result.compactJoins);
    return result.filter != null;
  }

  /**
   * Gets all partitions of a table by using direct SQL queries.
   * @param catName Metastore catalog name.
   * @param dbName Metastore db name.
   * @param tblName Metastore table name.
   * @param args additional arguments for getting partitions
   * @return List of partitions.
   */
  public List<Partition> getPartitions(String catName,
      String dbName, String tblName, GetPartitionsArgs args) throws MetaException {
    List<Long> partitionIds = getPartitionIdsViaSqlFilter(catName, dbName,
        tblName, null, Collections.<String>emptyList(), Collections.<String>emptyList(), args.getMax());
    if (partitionIds.isEmpty()) {
      return Collections.emptyList(); // no partitions, bail early.
    }

    // Get full objects. For Oracle/etc. do it in batches.
    List<Partition> result = Batchable.runBatched(batchSize, partitionIds, new Batchable<Long, Partition>() {
      @Override
      public List<Partition> run(List<Long> input) throws MetaException {
        return getPartitionsByPartitionIds(catName, dbName, tblName, input, false, args);
      }
    });
    return result;
  }

  private static Boolean isViewTable(Table t) {
    return t.isSetTableType() ?
        t.getTableType().equals(TableType.VIRTUAL_VIEW.toString()) : null;
  }

  private boolean isViewTable(String catName, String dbName, String tblName) throws MetaException {
    String queryText = "select \"TBL_TYPE\" from " + TBLS + "" +
        " inner join " + DBS + " on " + TBLS + ".\"DB_ID\" = " + DBS + ".\"DB_ID\" " +
        " where " + TBLS + ".\"TBL_NAME\" = ? and " + DBS + ".\"NAME\" = ? and " + DBS + ".\"CTLG_NAME\" = ?";
    try (QueryWrapper query = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
      Object[] params = new Object[] { tblName, dbName, catName };
      query.setUnique(true);
      Object result = executeWithArray(query.getInnerQuery(), params, queryText);
      return (result != null) && result.toString().equals(TableType.VIRTUAL_VIEW.toString());
    }
  }

  /**
   * Get partition ids for the query using direct SQL queries, to avoid bazillion
   * queries created by DN retrieving stuff for each object individually.
   * @param catName MetaStore catalog name
   * @param dbName MetaStore db name
   * @param tblName MetaStore table name
   * @param sqlFilter SQL filter to use. Better be SQL92-compliant.
   * @param paramsForFilter params for ?-s in SQL filter text. Params must be in order.
   * @param joinsForFilter if the filter needs additional join statement, they must be in
   *                       this list. Better be SQL92-compliant.
   * @param max The maximum number of partitions to return.
   * @return List of partition objects.
   */
  private List<Long> getPartitionIdsViaSqlFilter(
      String catName, String dbName, String tblName, String sqlFilter,
      List<? extends Object> paramsForFilter, List<String> joinsForFilter, Integer max)
      throws MetaException {
    return getPartitionFieldsViaSqlFilter(catName, dbName, tblName,
        Arrays.asList("\"PART_ID\""), sqlFilter, paramsForFilter, joinsForFilter, max);
  }

  /**
   * Get partition fields for the query using direct SQL queries, to avoid bazillion
   * queries created by DN retrieving stuff for each object individually.
   * @param catName MetaStore catalog name
   * @param dbName MetaStore db name
   * @param tblName MetaStore table name
   * @param partColumns part fields want to get
   * @param sqlFilter SQL filter to use. Better be SQL92-compliant.
   * @param paramsForFilter params for ?-s in SQL filter text. Params must be in order.
   * @param joinsForFilter if the filter needs additional join statement, they must be in
   *                       this list. Better be SQL92-compliant.
   * @param max The maximum number of partitions to return.
   * @return List of partition objects.
   */
  public <T> List<T> getPartitionFieldsViaSqlFilter(
      String catName, String dbName, String tblName, List<String> partColumns, String sqlFilter,
      List<? extends Object> paramsForFilter, List<String> joinsForFilter, Integer max)
      throws MetaException {
    boolean doTrace = LOG.isDebugEnabled();
    final String dbNameLcase = dbName.toLowerCase();
    final String tblNameLcase = tblName.toLowerCase();
    final String catNameLcase = normalizeSpace(catName).toLowerCase();

    // We have to be mindful of order during filtering if we are not returning all partitions.
    String orderForFilter = (max != null) ? " order by " + MetastoreConf.getVar(conf, ConfVars.PARTITION_ORDER_EXPR) : "";
    String columns = partColumns.stream().map(col -> PARTITIONS + "." + col).collect(Collectors.joining(","));

    String queryText =
        "select " + columns + " from " + PARTITIONS + ""
            + "  inner join " + TBLS + " on " + PARTITIONS + ".\"TBL_ID\" = " + TBLS + ".\"TBL_ID\" "
            + "    and " + TBLS + ".\"TBL_NAME\" = ? "
            + "  inner join " + DBS + " on " + TBLS + ".\"DB_ID\" = " + DBS + ".\"DB_ID\" "
            + "     and " + DBS + ".\"NAME\" = ? "
            + join(joinsForFilter, ' ')
            + " where " + DBS + ".\"CTLG_NAME\" = ? "
            + (StringUtils.isBlank(sqlFilter) ? "" : (" and " + sqlFilter)) + orderForFilter;
    Object[] params = new Object[paramsForFilter.size() + 3];
    params[0] = tblNameLcase;
    params[1] = dbNameLcase;
    params[2] = catNameLcase;
    for (int i = 0; i < paramsForFilter.size(); ++i) {
      params[i + 3] = paramsForFilter.get(i);
    }

    long start = doTrace ? System.nanoTime() : 0;
    try (QueryWrapper query = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
      List<T> sqlResult = executeWithArray(query.getInnerQuery(), params, queryText,
          ((max == null) ? -1 : max.intValue()));
      long queryTime = doTrace ? System.nanoTime() : 0;
      MetastoreDirectSqlUtils.timingTrace(doTrace, queryText, start, queryTime);
      List<T> result = new ArrayList<>(sqlResult);
      return result;
    }
  }


  /** Should be called with the list short enough to not trip up Oracle/etc. */
  private List<Partition> getPartitionsByNames(String catName, String dbName,
      String tblName, List<String> partNameList, boolean isAcidTable, GetPartitionsArgs args)
      throws MetaException {
    // Get most of the fields for the partNames provided.
    // Assume db and table names are the same for all partition, as provided in arguments.
    String quotedPartNames = partNameList.stream()
        .map(DirectSqlUpdatePart::quoteString)
        .collect(Collectors.joining(","));

    String queryText =
        "select " + PARTITIONS + ".\"PART_ID\"," + SDS + ".\"SD_ID\"," + SDS + ".\"CD_ID\","
        + SERDES + ".\"SERDE_ID\"," + PARTITIONS + ".\"CREATE_TIME\"," + PARTITIONS
        + ".\"LAST_ACCESS_TIME\"," + SDS + ".\"INPUT_FORMAT\"," + SDS + ".\"IS_COMPRESSED\","
        + SDS + ".\"IS_STOREDASSUBDIRECTORIES\"," + SDS + ".\"LOCATION\"," + SDS
        + ".\"NUM_BUCKETS\"," + SDS + ".\"OUTPUT_FORMAT\"," + SERDES + ".\"NAME\","
        + SERDES + ".\"SLIB\"," + PARTITIONS + ".\"WRITE_ID\"" + " from " + PARTITIONS
        + " left outer join " + SDS + " on " + PARTITIONS + ".\"SD_ID\" = " + SDS + ".\"SD_ID\" "
        + " left outer join " + SERDES + " on " + SDS + ".\"SERDE_ID\" = " + SERDES + ".\"SERDE_ID\" "
        + " inner join " + TBLS + " on " + TBLS + ".\"TBL_ID\" = " + PARTITIONS + ".\"TBL_ID\" "
        + " inner join " + DBS + " on " + DBS + ".\"DB_ID\" = " + TBLS + ".\"DB_ID\" "
        + " where \"PART_NAME\" in (" + quotedPartNames + ") "
        + " and " + TBLS + ".\"TBL_NAME\" = ? and " + DBS + ".\"NAME\" = ? and " + DBS
        + ".\"CTLG_NAME\" = ? order by \"PART_NAME\" asc";

    Object[] params = new Object[]{tblName, dbName, catName};
    return getPartitionsByQuery(catName, dbName, tblName, queryText, params, isAcidTable, args);
  }

  /** Should be called with the list short enough to not trip up Oracle/etc. */
  private List<Partition> getPartitionsByPartitionIds(String catName, String dbName, String tblName,
      List<Long> partIdList, boolean isAcidTable, GetPartitionsArgs args) throws MetaException {
    // Get most of the fields for the IDs provided.
    // Assume db and table names are the same for all partition, as provided in arguments.
    String partIds = getIdListForIn(partIdList);
    String queryText =
        "select " + PARTITIONS + ".\"PART_ID\", " + SDS + ".\"SD_ID\", " + SDS + ".\"CD_ID\"," + " "
            + SERDES + ".\"SERDE_ID\", " + PARTITIONS + ".\"CREATE_TIME\"," + " " + PARTITIONS
            + ".\"LAST_ACCESS_TIME\", " + SDS + ".\"INPUT_FORMAT\", " + SDS + ".\"IS_COMPRESSED\","
            + " " + SDS + ".\"IS_STOREDASSUBDIRECTORIES\", " + SDS + ".\"LOCATION\", " + SDS
            + ".\"NUM_BUCKETS\"," + " " + SDS + ".\"OUTPUT_FORMAT\", " + SERDES + ".\"NAME\", "
            + SERDES + ".\"SLIB\", " + PARTITIONS + ".\"WRITE_ID\"" + " from " + PARTITIONS + ""
            + "  left outer join " + SDS + " on " + PARTITIONS + ".\"SD_ID\" = " + SDS
            + ".\"SD_ID\" " + "  left outer join " + SERDES + " on " + SDS + ".\"SERDE_ID\" = "
            + SERDES + ".\"SERDE_ID\" " + "where \"PART_ID\" in (" + partIds
            + ") order by \"PART_NAME\" asc";
    return getPartitionsByQuery(catName, dbName, tblName, queryText, null, isAcidTable, args);
  }

  private List<Partition> getPartitionsByQuery(String catName, String dbName, String tblName,
      String queryText, Object[] params, boolean isAcidTable, GetPartitionsArgs args)
      throws MetaException {
    boolean doTrace = LOG.isDebugEnabled();

    // Read all the fields and create partitions, SDs and serdes.
    TreeMap<Long, Partition> partitions = new TreeMap<Long, Partition>();
    TreeMap<Long, StorageDescriptor> sds = new TreeMap<Long, StorageDescriptor>();
    TreeMap<Long, SerDeInfo> serdes = new TreeMap<Long, SerDeInfo>();
    TreeMap<Long, List<FieldSchema>> colss = new TreeMap<Long, List<FieldSchema>>();
    // Keep order by name, consistent with JDO.
    ArrayList<Partition> orderedResult;

    // Prepare StringBuilder-s for "in (...)" lists to use in one-to-many queries.
    StringBuilder sdSb = new StringBuilder(), serdeSb = new StringBuilder();
    StringBuilder colsSb = new StringBuilder(7); // We expect that there's only one field schema.
    tblName = tblName.toLowerCase();
    dbName = dbName.toLowerCase();
    catName = normalizeSpace(catName).toLowerCase();
    partitions.navigableKeySet();

    try (QueryWrapper query = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
      long start = doTrace ? System.nanoTime() : 0;
      List<Object[]> sqlResult = executeWithArray(query.getInnerQuery(), params, queryText);
      long queryTime = doTrace ? System.nanoTime() : 0;
      Deadline.checkTimeout();
      orderedResult = new ArrayList<>(sqlResult.size());

      for (Object[] fields : sqlResult) {
        // Here comes the ugly part...
        long partitionId = MetastoreDirectSqlUtils.extractSqlLong(fields[0]);
        Long sdId = MetastoreDirectSqlUtils.extractSqlLong(fields[1]);
        Long colId = MetastoreDirectSqlUtils.extractSqlLong(fields[2]);
        Long serdeId = MetastoreDirectSqlUtils.extractSqlLong(fields[3]);

        Partition part = new Partition();
        orderedResult.add(part);
        // Set the collection fields; some code might not check presence before accessing them.
        part.setParameters(new HashMap<>());
        part.setValues(new ArrayList<String>());
        part.setCatName(catName);
        part.setDbName(dbName);
        part.setTableName(tblName);
        if (fields[4] != null) {
          part.setCreateTime(MetastoreDirectSqlUtils.extractSqlInt(fields[4]));
        }
        if (fields[5] != null) {
          part.setLastAccessTime(MetastoreDirectSqlUtils.extractSqlInt(fields[5]));
        }
        Long writeId = MetastoreDirectSqlUtils.extractSqlLong(fields[14]);
        if (writeId != null && writeId > 0) {
          part.setWriteId(writeId);
        } else {
          part.setWriteId(-1L);
        }
        partitions.put(partitionId, part);


        if (sdId == null) {
          continue; // Probably a view.
        }
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
        sd.setInputFormat((String) fields[6]);
        Boolean tmpBoolean = MetastoreDirectSqlUtils.extractSqlBoolean(fields[7]);
        if (tmpBoolean != null) {
          sd.setCompressed(tmpBoolean);
        }
        tmpBoolean = MetastoreDirectSqlUtils.extractSqlBoolean(fields[8]);
        if (tmpBoolean != null) {
          sd.setStoredAsSubDirectories(tmpBoolean);
        }
        sd.setLocation((String) fields[9]);
        if (fields[10] != null) {
          sd.setNumBuckets(MetastoreDirectSqlUtils.extractSqlInt(fields[10]));
        }
        sd.setOutputFormat((String) fields[11]);
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
        serde.setName((String) fields[12]);
        serde.setSerializationLib((String) fields[13]);
        serdeSb.append(serdeId).append(",");
        sd.setSerdeInfo(serde);

        Deadline.checkTimeout();
      }
      MetastoreDirectSqlUtils.timingTrace(doTrace, queryText, start, queryTime);
    }
    String partIds = getIdListForIn(partitions.keySet());
    // Now get all the one-to-many things. Start with partitions.
    MetastoreDirectSqlUtils
        .setPartitionParametersWithFilter(PARTITION_PARAMS, convertMapNullsToEmptyStrings, pm,
            partIds, partitions, args.getIncludeParamKeyPattern(), args.getExcludeParamKeyPattern());

    MetastoreDirectSqlUtils.setPartitionValues(PARTITION_KEY_VALS, pm, partIds, partitions);

    // Prepare IN (blah) lists for the following queries. Cut off the final ','s.
    if (sdSb.length() == 0) {
      assert serdeSb.length() == 0 && colsSb.length() == 0;
      return orderedResult; // No SDs, probably a view.
    }

    String sdIds = trimCommaList(sdSb);
    String serdeIds = trimCommaList(serdeSb);
    String colIds = trimCommaList(colsSb);

    if (!isAcidTable) {
      // Get all the stuff for SD. Don't do empty-list check - we expect partitions do have SDs.
      MetastoreDirectSqlUtils.setSDParameters(SD_PARAMS, convertMapNullsToEmptyStrings, pm, sds, sdIds);
    }

    boolean hasSkewedColumns = false;
    if (!isAcidTable) {
      MetastoreDirectSqlUtils.setSDSortCols(SORT_COLS, pm, sds, sdIds);
    }

    MetastoreDirectSqlUtils.setSDBucketCols(BUCKETING_COLS, pm, sds, sdIds);

    if (!isAcidTable) {
      // Skewed columns stuff.
      hasSkewedColumns = MetastoreDirectSqlUtils.setSkewedColNames(SKEWED_COL_NAMES, pm, sds, sdIds);
    }

    // Assume we don't need to fetch the rest of the skewed column data if we have no columns.
    if (hasSkewedColumns) {
      // We are skipping the SKEWED_STRING_LIST table here, as it seems to be totally useless.
      MetastoreDirectSqlUtils
          .setSkewedColValues(SKEWED_STRING_LIST_VALUES, SKEWED_VALUES, pm, sds, sdIds);

      // We are skipping the SKEWED_STRING_LIST table here, as it seems to be totally useless.
      MetastoreDirectSqlUtils
          .setSkewedColLocationMaps(SKEWED_COL_VALUE_LOC_MAP, SKEWED_STRING_LIST_VALUES, pm, sds, sdIds);
    } // if (hasSkewedColumns)

    // Get FieldSchema stuff if any.
    if (!colss.isEmpty() && !args.isSkipColumnSchemaForPartition()) {
      // We are skipping the CDS table here, as it seems to be totally useless.
      MetastoreDirectSqlUtils.setSDCols(COLUMNS_V2, pm, colss, colIds);
    }

    // Finally, get all the stuff for serdes - just the params.
    if (!isAcidTable) {
      MetastoreDirectSqlUtils.setSerdeParams(SERDE_PARAMS, convertMapNullsToEmptyStrings, pm, serdes, serdeIds);
    }

    return orderedResult;
  }

  public int getNumPartitionsViaSqlFilter(SqlFilterForPushdown filter) throws MetaException {
    boolean doTrace = LOG.isDebugEnabled();
    String catName = filter.catName.toLowerCase();
    String dbName = filter.dbName.toLowerCase();
    String tblName = filter.tableName.toLowerCase();

    // Get number of partitions by doing count on PART_ID.
    String queryText = "select count(" + PARTITIONS + ".\"PART_ID\") from " + PARTITIONS + ""
      + "  inner join " + TBLS + " on " + PARTITIONS + ".\"TBL_ID\" = " + TBLS + ".\"TBL_ID\" "
      + "    and " + TBLS + ".\"TBL_NAME\" = ? "
      + "  inner join " + DBS + " on " + TBLS + ".\"DB_ID\" = " + DBS + ".\"DB_ID\" "
      + "     and " + DBS + ".\"NAME\" = ? "
      + join(filter.joins, ' ')
      + " where " + DBS + ".\"CTLG_NAME\" = ? "
      + (filter.filter == null || filter.filter.trim().isEmpty() ? "" : (" and " + filter.filter));

    Object[] params = new Object[filter.params.size() + 3];
    params[0] = tblName;
    params[1] = dbName;
    params[2] = catName;
    for (int i = 0; i < filter.params.size(); ++i) {
      params[i + 3] = filter.params.get(i);
    }

    long start = doTrace ? System.nanoTime() : 0;
    try (QueryWrapper query = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
      query.setUnique(true);
      int sqlResult = MetastoreDirectSqlUtils.extractSqlInt(query.executeWithArray(params));
      long queryTime = doTrace ? System.nanoTime() : 0;
      MetastoreDirectSqlUtils.timingTrace(doTrace, queryText, start, queryTime);
      return sqlResult;
    }
  }

  private static String trimCommaList(StringBuilder sb) {
    if (sb.length() > 0) {
      sb.setLength(sb.length() - 1);
    }
    return sb.toString();
  }

  private static class PartitionFilterGenerator extends TreeVisitor {
    private final String catName;
    private final String dbName;
    private final String tableName;
    private final List<FieldSchema> partitionKeys;
    private final FilterBuilder filterBuffer;
    private final List<Object> params;
    private final List<String> joins;
    private final boolean dbHasJoinCastBug;
    private final String defaultPartName;
    private final DatabaseProduct dbType;
    private final String PARTITION_KEY_VALS, PARTITIONS, DBS, TBLS;

    private PartitionFilterGenerator(String catName, String dbName, String tableName,
        List<FieldSchema> partitionKeys, List<Object> params, List<String> joins,
        boolean dbHasJoinCastBug, String defaultPartName, DatabaseProduct dbType, String schema) {
      this.catName = catName;
      this.dbName = dbName;
      this.tableName = tableName;
      this.partitionKeys = partitionKeys;
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
     * @param catName catalog name
     * @param dbName db name
     * @param tableName table name
     * @param partitionKeys partition keys
     * @param params the ordered parameters for the resulting expression
     * @param joins the joins necessary for the resulting expression
     * @return the string representation of the expression tree
     */
    private static String generateSqlFilter(String catName, String dbName, String tableName,
        List<FieldSchema> partitionKeys, ExpressionTree tree, List<Object> params,
        List<String> joins, boolean dbHasJoinCastBug, String defaultPartName,
        DatabaseProduct dbType, String schema, boolean compactJoins) throws MetaException {
      if (tree == null) {
        // consistent with other APIs like makeExpressionTree, null is returned to indicate that
        // the filter could not pushed down due to parsing issue etc
        return null;
      }
      if (tree.getRoot() == null) {
        return "";
      }
      PartitionFilterGenerator visitor = new PartitionFilterGenerator(
          catName, dbName, tableName, partitionKeys,
          params, joins, dbHasJoinCastBug, defaultPartName, dbType, schema);
      tree.accept(visitor);
      if (visitor.filterBuffer.hasError()) {
        LOG.info("Unable to push down SQL filter: " + visitor.filterBuffer.getErrorMessage());
        return null;
      }

      // Some joins might be null (see processNode for LeafNode), clean them up.
      if (compactJoins) {
        for (int i = 0; i < joins.size(); ++i) {
          if (joins.get(i) != null) {
            continue;
          }
          joins.remove(i--);
        }
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
      Integral(ImmutableSet.of(TINYINT_TYPE_NAME, SMALLINT_TYPE_NAME, INT_TYPE_NAME, BIGINT_TYPE_NAME), Long.class),
      String(ImmutableSet.of(STRING_TYPE_NAME, CHAR_TYPE_NAME, VARCHAR_TYPE_NAME), String.class),
      Date(ImmutableSet.of(DATE_TYPE_NAME), java.sql.Date.class),
      Timestamp(ImmutableSet.of(TIMESTAMP_TYPE_NAME), java.sql.Timestamp.class),

      Invalid(Collections.emptySet(), Void.class);

      private final Set<String> colTypes;
      private final Class<?> clazz;

      FilterType(Set<String> colTypes, Class<?> clazz) {
        this.colTypes = colTypes;
        this.clazz = clazz;
      }

      public Set<String> getType() {
        return colTypes;
      }

      public Class<?> getClazz() {
        return clazz;
      }

      public static FilterType fromType(String colTypeStr) {
        for (FilterType filterType : FilterType.values()) {
          if (filterType.colTypes.contains(colTypeStr)) {
            return filterType;
          }
        }
        return FilterType.Invalid;
      }

      public static FilterType fromClass(Object value){
        for (FilterType filterType : FilterType.values()) {
          if (filterType.clazz.isInstance(value)) {
            return filterType;
          }
        }
        return FilterType.Invalid;
      }
    }

    @Override
    public void visit(LeafNode node) throws MetaException {
      int partColCount = partitionKeys.size();
      int partColIndex = LeafNode.getPartColIndexForFilter(node.keyName, partitionKeys, filterBuffer);
      if (filterBuffer.hasError()) {
        return;
      }

      FieldSchema partCol = partitionKeys.get(partColIndex);
      String colTypeStr = ColumnType.getTypeName(partCol.getType());
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

      String nodeValue0 = "?";
      // if Filter.g does date parsing for quoted strings, we'd need to verify there's no
      // type mismatch when string col is filtered by a string that looks like date.
      if (colType == FilterType.Date) {
        try {
          nodeValue = MetaStoreUtils.normalizeDate((String) nodeValue);
          valType = FilterType.Date;
          if (dbType.isPOSTGRES() || dbType.isORACLE()) {
            nodeValue0 = "date '" + nodeValue + "'";
            nodeValue = null;
          }
        } catch (Exception e) {  // do nothing, handled below - types will mismatch
        }
      } else if (colType == FilterType.Timestamp) {
        if (dbType.isDERBY() || dbType.isMYSQL()) {
          filterBuffer.setError("Filter pushdown on timestamp not supported for " + dbType.dbType);
          return;
        }
        try {
          MetaStoreUtils.convertStringToTimestamp((String) nodeValue);
          valType = FilterType.Timestamp;
          if (dbType.isPOSTGRES() || dbType.isORACLE()) {
            nodeValue0 = "timestamp '" + nodeValue + "'";
            nodeValue = null;
          }
        } catch (Exception e) { //nodeValue could be '__HIVE_DEFAULT_PARTITION__'
        }
      }

      boolean isDefaultPartition = (valType == FilterType.String) && defaultPartName.equals(nodeValue);
      if ((colType != valType) && (!isDefaultPartition)) {
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

      if (node.isReverseOrder && nodeValue != null) {
        params.add(nodeValue);
      }
      String tableColumn = tableValue;
      if ((colType != FilterType.String) && (!isDefaultPartition)) {
        // The underlying database field is varchar, we need to compare numbers.
        if (colType == FilterType.Integral) {
          tableValue = "cast(" + tableValue + " as decimal(21,0))";
        } else if (colType == FilterType.Date) {
        	tableValue = dbType.toDate(tableValue);
        } else if (colType == FilterType.Timestamp) {
          tableValue = dbType.toTimestamp(tableValue);
        }

        // Workaround for HIVE_DEFAULT_PARTITION - ignore it like JDO does, for now.
        String tableValue0 = tableValue;
        tableValue = "(case when " + tableColumn + " <> ?";
        params.add(defaultPartName);

        if (dbHasJoinCastBug) {
          // This is a workaround for DERBY-6358 and Oracle bug; it is pretty horrible.
          tableValue += (" and " + TBLS + ".\"TBL_NAME\" = ? and " + DBS + ".\"NAME\" = ? and "
              + DBS + ".\"CTLG_NAME\" = ? and "
              + "\"FILTER" + partColIndex + "\".\"PART_ID\" = " + PARTITIONS + ".\"PART_ID\" and "
                + "\"FILTER" + partColIndex + "\".\"INTEGER_IDX\" = " + partColIndex);
          params.add(tableName.toLowerCase());
          params.add(dbName.toLowerCase());
          params.add(catName.toLowerCase());
        }
        tableValue += " then " + tableValue0 + " else null end)";
      }

      if (!node.isReverseOrder && nodeValue != null) {
        params.add(nodeValue);
      }

      // The following syntax is required for using LIKE clause wildcards '_' and '%' as literals.
      if (node.operator == Operator.LIKE) {
        nodeValue0 = nodeValue0 + " ESCAPE '\\' ";
      }
      String filter = node.isReverseOrder
              ? nodeValue0 + " " + node.operator.getSqlOp() + " " + tableValue
              : tableValue + " " + node.operator.getSqlOp() + " " + nodeValue0;
      // For equals and not-equals filter, we can add partition name filter to improve performance.
      boolean isOpEquals = Operator.isEqualOperator(node.operator);
      if (isOpEquals || Operator.isNotEqualOperator(node.operator)) {
        Map<String, String> partKeyToVal = new HashMap<>();
        partKeyToVal.put(partCol.getName(), node.value.toString());
        String escapedNameFragment = Warehouse.makePartName(partKeyToVal, false);
        if (colType == FilterType.Date) {
          // Some engines like Pig will record both date and time values, in which case we need
          // match PART_NAME by like clause.
          escapedNameFragment += "%";
        }
        if (colType != FilterType.Date && partColCount == 1) {
          // Case where partition column type is not date and there is no other partition columns
          params.add(escapedNameFragment);
          filter += " and " + PARTITIONS + ".\"PART_NAME\"" + (isOpEquals ? " =? " : " !=? ");
        } else {
          if (partColCount == 1) {
            // Case where partition column type is date and there is no other partition columns
            params.add(escapedNameFragment);
          } else if (partColIndex + 1 == partColCount) {
            // Case where the partition column is at the end of the name.
            params.add("%/" + escapedNameFragment);
          } else if (partColIndex == 0) {
            // Case where the partition column is at the beginning of the name.
            params.add(escapedNameFragment + "/%");
          } else {
            // Case where the partition column is in the middle of the name.
            params.add("%/" + escapedNameFragment + "/%");
          }
          filter += " and " + PARTITIONS + ".\"PART_NAME\"" + (isOpEquals ? " like ? " : " not like ? ");
        }
      }

      filterBuffer.append("(" + filter + ")");
    }
  }

  /**
   * Retrieve the column statistics for the specified columns of the table. NULL
   * is returned if the columns are not provided.
   * @param catName     the catalog name of the table
   * @param dbName      the database name of the table
   * @param tableName   the table name
   * @param colNames    the list of the column names
   * @param engine      engine making the request
   * @return            the column statistics for the specified columns
   * @throws MetaException
   */
  public ColumnStatistics getTableStats(final String catName, final String dbName,
      final String tableName, List<String> colNames, String engine,
      boolean enableBitVector, boolean enableKll) throws MetaException {
    if (colNames == null || colNames.isEmpty()) {
      return null;
    }
    final boolean doTrace = LOG.isDebugEnabled();
    final String queryText0 = "select " + getStatsList(enableBitVector, enableKll) + " from " + TAB_COL_STATS
          + " where \"CAT_NAME\" = ? and \"DB_NAME\" = ? and \"TABLE_NAME\" = ? "
          + " and \"ENGINE\" = ? and \"COLUMN_NAME\" in (";
    Batchable<String, Object[]> b = new Batchable<String, Object[]>() {
      @Override
      public List<Object[]> run(List<String> input) throws MetaException {
        String queryText = queryText0 + makeParams(input.size()) + ")";
        Object[] params = new Object[input.size() + 4];
        params[0] = catName;
        params[1] = dbName;
        params[2] = tableName;
        params[3] = engine;
        for (int i = 0; i < input.size(); ++i) {
          params[i + 4] = input.get(i);
        }
        long start = doTrace ? System.nanoTime() : 0;
        Query query = pm.newQuery("javax.jdo.query.SQL", queryText);
        try {
          Object qResult = executeWithArray(query, params, queryText);
          MetastoreDirectSqlUtils.timingTrace(doTrace, queryText0 + "...)", start, (doTrace ? System.nanoTime() : 0));
          if (qResult == null) {
            return null;
          }
          return MetastoreDirectSqlUtils.ensureList(qResult);
        } finally {
          addQueryAfterUse(query);
        }
      }
    };
    List<Object[]> list;
    try {
      list = Batchable.runBatched(batchSize, colNames, b);
      if (list != null) {
        list = new ArrayList<>(list);
      }
    } finally {
      b.closeAllQueries();
    }

    if (list == null || list.isEmpty()) {
      return null;
    }
    ColumnStatisticsDesc csd = new ColumnStatisticsDesc(true, dbName, tableName);
    csd.setCatName(catName);
    return makeColumnStats(list, csd, 0, engine);
  }

  public List<HiveObjectPrivilege> getTableAllColumnGrants(String catName, String dbName,
                                                           String tableName, String authorizer) throws MetaException {
    // These constants should match the SELECT clause of the query.
    final int authorizerIndex = 0;
    final int columnNameIndex = 1;
    final int createTimeIndex = 2;
    final int grantOptionIndex = 3;
    final int grantorIndex = 4;
    final int grantorTypeIndex = 5;
    final int principalNameIndex = 6;
    final int principalTypeIndex = 7;
    final int privilegeIndex = 8;

    // Retrieve the privileges from the object store. Just grab only the required fields.
    String queryText = "select " +
            TBL_COL_PRIVS + ".\"AUTHORIZER\", " +
            TBL_COL_PRIVS + ".\"COLUMN_NAME\", " +
            TBL_COL_PRIVS + ".\"CREATE_TIME\", " +
            TBL_COL_PRIVS + ".\"GRANT_OPTION\", " +
            TBL_COL_PRIVS + ".\"GRANTOR\", " +
            TBL_COL_PRIVS + ".\"GRANTOR_TYPE\", " +
            TBL_COL_PRIVS + ".\"PRINCIPAL_NAME\", " +
            TBL_COL_PRIVS + ".\"PRINCIPAL_TYPE\", " +
            TBL_COL_PRIVS + ".\"TBL_COL_PRIV\" " +
            "FROM " + TBL_COL_PRIVS + " JOIN " + TBLS +
            " ON " + TBL_COL_PRIVS + ".\"TBL_ID\" = " + TBLS + ".\"TBL_ID\"" +
            " JOIN " + DBS + " ON " + TBLS + ".\"DB_ID\" = " + DBS + ".\"DB_ID\" " +
            " WHERE " + TBLS + ".\"TBL_NAME\" = ?" +
            " AND " + DBS + ".\"NAME\" = ?" +
            " AND " + DBS + ".\"CTLG_NAME\" = ?";

    // Build the parameters, they should match the WHERE clause of the query.
    int numParams = authorizer != null ? 4 : 3;
    Object[] params = new Object[numParams];
    params[0] = tableName;
    params[1] = dbName;
    params[2] = catName;
    if (authorizer != null) {
      queryText = queryText + " AND " + TBL_COL_PRIVS + ".\"AUTHORIZER\" = ?";
      params[3] = authorizer;
    }

    // Collect the results into a list that the caller can consume.
    List<HiveObjectPrivilege> result = new ArrayList<>();
    final boolean doTrace = LOG.isDebugEnabled();
    long start = doTrace ? System.nanoTime() : 0;
    try (QueryWrapper query = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
      List<Object[]> queryResult = MetastoreDirectSqlUtils.ensureList(
              executeWithArray(query.getInnerQuery(), params, queryText));
      long end = doTrace ? System.nanoTime() : 0;
      MetastoreDirectSqlUtils.timingTrace(doTrace, queryText, start, end);

      // If there is some result convert it into HivePrivilege bag and return.
      for (Object[] privLine : queryResult) {
        String privAuthorizer = MetastoreDirectSqlUtils.extractSqlString(privLine[authorizerIndex]);
        String principalName = MetastoreDirectSqlUtils.extractSqlString(privLine[principalNameIndex]);
        PrincipalType ptype = PrincipalType.valueOf(
                MetastoreDirectSqlUtils.extractSqlString(privLine[principalTypeIndex]));
        String columnName = MetastoreDirectSqlUtils.extractSqlString(privLine[columnNameIndex]);
        String privilege = MetastoreDirectSqlUtils.extractSqlString(privLine[privilegeIndex]);
        int createTime = MetastoreDirectSqlUtils.extractSqlInt(privLine[createTimeIndex]);
        String grantor = MetastoreDirectSqlUtils.extractSqlString(privLine[grantorIndex]);
        PrincipalType grantorType =
                PrincipalType.valueOf(
                        MetastoreDirectSqlUtils.extractSqlString(privLine[grantorTypeIndex]));
        boolean grantOption = MetastoreDirectSqlUtils.extractSqlBoolean(privLine[grantOptionIndex]);

        HiveObjectRef objectRef = new HiveObjectRef(HiveObjectType.COLUMN, dbName, tableName, null,
                columnName);
        objectRef.setCatName(catName);
        PrivilegeGrantInfo grantInfo = new PrivilegeGrantInfo(privilege, createTime, grantor,
                grantorType, grantOption);

        result.add(new HiveObjectPrivilege(objectRef, principalName, ptype, grantInfo,
                privAuthorizer));
      }
    }

    return result;
  }

  public AggrStats aggrColStatsForPartitions(String catName, String dbName, String tableName,
      List<String> partNames, List<String> colNames, String engine,
      boolean useDensityFunctionForNDVEstimation, double ndvTuner, boolean enableBitVector, boolean enableKll)
      throws MetaException {
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
        colStatsAggrCached = aggrStatsCache.get(catName, dbName, tableName, colName, partNames);
        if (colStatsAggrCached != null) {
          colStatsList.add(colStatsAggrCached.getColStats());
          partsFound = colStatsAggrCached.getNumPartsCached();
        } else {
          if (computePartsFound) {
            partsFound = partsFoundForPartitions(catName, dbName, tableName, partNames, colNames, engine);
            computePartsFound = false;
          }
          List<String> colNamesForDB = new ArrayList<>();
          colNamesForDB.add(colName);
          // Read aggregated stats for one column
          colStatsAggrFromDB =
              columnStatisticsObjForPartitions(catName, dbName, tableName, partNames, colNamesForDB, engine,
                  partsFound, useDensityFunctionForNDVEstimation, ndvTuner, enableBitVector, enableKll);
          if (!colStatsAggrFromDB.isEmpty()) {
            ColumnStatisticsObj colStatsAggr = colStatsAggrFromDB.get(0);
            colStatsList.add(colStatsAggr);
            // Update the cache to add this new aggregate node
            aggrStatsCache.add(catName, dbName, tableName, colName, partsFound, colStatsAggr, bloomFilter);
          }
        }
      }
    } else {
      partsFound = partsFoundForPartitions(catName, dbName, tableName, partNames, colNames, engine);
      colStatsList =
          columnStatisticsObjForPartitions(catName, dbName, tableName, partNames, colNames, engine, partsFound,
              useDensityFunctionForNDVEstimation, ndvTuner, enableBitVector, enableKll);
    }
    LOG.debug("useDensityFunctionForNDVEstimation = " + useDensityFunctionForNDVEstimation
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

  private long partsFoundForPartitions(
      final String catName, final String dbName, final String tableName,
      final List<String> partNames, List<String> colNames, String engine) throws MetaException {
    assert !colNames.isEmpty() && !partNames.isEmpty();
    final boolean doTrace = LOG.isDebugEnabled();
    final String queryText0  = "select count(\"COLUMN_NAME\") from " + PART_COL_STATS + ""
        + " where \"CAT_NAME\" = ? and \"DB_NAME\" = ? and \"TABLE_NAME\" = ? "
        + " and \"COLUMN_NAME\" in (%1$s) and \"PARTITION_NAME\" in (%2$s)"
        + " and \"ENGINE\" = ? "
        + " group by \"PARTITION_NAME\"";
    List<Long> allCounts = Batchable.runBatched(batchSize, colNames, new Batchable<String, Long>() {
      @Override
      public List<Long> run(final List<String> inputColName) throws MetaException {
        return Batchable.runBatched(batchSize, partNames, new Batchable<String, Long>() {
          @Override
          public List<Long> run(List<String> inputPartNames) throws MetaException {
            long partsFound = 0;
            String queryText = String.format(queryText0,
                makeParams(inputColName.size()), makeParams(inputPartNames.size()));
            long start = doTrace ? System.nanoTime() : 0;
            try (QueryWrapper query = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
              Object qResult = executeWithArray(query.getInnerQuery(), prepareParams(
                  catName, dbName, tableName, inputPartNames, inputColName, engine), queryText);
              long end = doTrace ? System.nanoTime() : 0;
              MetastoreDirectSqlUtils.timingTrace(doTrace, queryText, start, end);
              ForwardQueryResult<?> fqr = (ForwardQueryResult<?>) qResult;
              Iterator<?> iter = fqr.iterator();
              while (iter.hasNext()) {
                if (MetastoreDirectSqlUtils.extractSqlLong(iter.next()) == inputColName.size()) {
                  partsFound++;
                }
              }
              return Lists.<Long>newArrayList(partsFound);
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

  private List<ColumnStatisticsObj> columnStatisticsObjForPartitions(
      final String catName, final String dbName, final String tableName, final List<String> partNames,
      List<String> colNames, String engine, long partsFound, final boolean useDensityFunctionForNDVEstimation,
      final double ndvTuner, final boolean enableBitVector, boolean enableKll) throws MetaException {
    final boolean areAllPartsFound = (partsFound == partNames.size());
    return Batchable.runBatched(batchSize, colNames, new Batchable<String, ColumnStatisticsObj>() {
      @Override
      public List<ColumnStatisticsObj> run(final List<String> inputColNames) throws MetaException {
        return Batchable.runBatched(batchSize, partNames, new Batchable<String, ColumnStatisticsObj>() {
          @Override
          public List<ColumnStatisticsObj> run(List<String> inputPartNames) throws MetaException {
            return columnStatisticsObjForPartitionsBatch(catName, dbName, tableName, inputPartNames,
                inputColNames, engine, areAllPartsFound, useDensityFunctionForNDVEstimation, ndvTuner,
                enableBitVector, enableKll);
          }
        });
      }
    });
  }

  public List<ColStatsObjWithSourceInfo> getColStatsForAllTablePartitions(String catName, String dbName,
      boolean enableBitVector, boolean enableKll) throws MetaException {
    String queryText = "select \"TABLE_NAME\", \"PARTITION_NAME\", " + getStatsList(enableBitVector, enableKll)
        + " from " + " " + PART_COL_STATS + " where \"DB_NAME\" = ? and \"CAT_NAME\" = ?";
    long start = 0;
    long end = 0;
    boolean doTrace = LOG.isDebugEnabled();
    Object qResult = null;
    start = doTrace ? System.nanoTime() : 0;
    List<ColStatsObjWithSourceInfo> colStatsForDB = new ArrayList<ColStatsObjWithSourceInfo>();
    try (QueryWrapper query = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
      qResult = executeWithArray(query.getInnerQuery(), new Object[] { dbName, catName }, queryText);
      if (qResult == null) {
        return colStatsForDB;
      }
      end = doTrace ? System.nanoTime() : 0;
      MetastoreDirectSqlUtils.timingTrace(doTrace, queryText, start, end);
      List<Object[]> list = MetastoreDirectSqlUtils.ensureList(qResult);
      for (Object[] row : list) {
        String tblName = (String) row[0];
        String partName = (String) row[1];
        ColumnStatisticsObj colStatObj = prepareCSObj(row, 2);
        colStatsForDB.add(new ColStatsObjWithSourceInfo(colStatObj, catName, dbName, tblName, partName));
        Deadline.checkTimeout();
      }
    }
    return colStatsForDB;
  }

  /** Should be called with the list short enough to not trip up Oracle/etc. */
  private List<ColumnStatisticsObj> columnStatisticsObjForPartitionsBatch(String catName, String dbName,
      String tableName, List<String> partNames, List<String> colNames, String engine,
      boolean areAllPartsFound, boolean useDensityFunctionForNDVEstimation, double ndvTuner,
      boolean enableBitVector, boolean enableKll)
      throws MetaException {
    if (enableBitVector || enableKll) {
      return aggrStatsUseJava(catName, dbName, tableName, partNames, colNames, engine, areAllPartsFound,
          useDensityFunctionForNDVEstimation, ndvTuner, enableBitVector, enableKll);
    } else {
      return aggrStatsUseDB(catName, dbName, tableName, partNames, colNames, engine, areAllPartsFound,
          useDensityFunctionForNDVEstimation, ndvTuner);
    }
  }

  private List<ColumnStatisticsObj> aggrStatsUseJava(String catName, String dbName, String tableName,
      List<String> partNames, List<String> colNames, String engine, boolean areAllPartsFound,
      boolean useDensityFunctionForNDVEstimation, double ndvTuner, boolean enableBitVector,
      boolean enableKll) throws MetaException {
    // 1. get all the stats for colNames in partNames;
    List<ColumnStatistics> partStats =
        getPartitionStats(catName, dbName, tableName, partNames, colNames, engine, enableBitVector, enableKll);
    // 2. use util function to aggr stats
    return MetaStoreServerUtils.aggrPartitionStats(partStats, catName, dbName, tableName, partNames, colNames,
        areAllPartsFound, useDensityFunctionForNDVEstimation, ndvTuner);
  }

  private List<ColumnStatisticsObj> aggrStatsUseDB(String catName, String dbName,
      String tableName, List<String> partNames, List<String> colNames, String engine,
      boolean areAllPartsFound, boolean useDensityFunctionForNDVEstimation, double ndvTuner) throws MetaException {
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
        + " where \"CAT_NAME\" = ? and \"DB_NAME\" = ? and \"TABLE_NAME\" = ? ";
    String queryText = null;
    long start = 0;
    long end = 0;

    boolean doTrace = LOG.isDebugEnabled();
    ForwardQueryResult<?> fqr = null;
    // Check if the status of all the columns of all the partitions exists
    // Extrapolation is not needed.
    if (areAllPartsFound) {
      queryText = commonPrefix + " and \"COLUMN_NAME\" in (" + makeParams(colNames.size()) + ")"
          + " and \"PARTITION_NAME\" in (" + makeParams(partNames.size()) + ")"
          + " and \"ENGINE\" = ? "
          + " group by \"COLUMN_NAME\", \"COLUMN_TYPE\"";
      start = doTrace ? System.nanoTime() : 0;
      try (QueryWrapper query = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
        Object qResult = executeWithArray(query.getInnerQuery(),
            prepareParams(catName, dbName, tableName, partNames, colNames,
                engine), queryText);
        if (qResult == null) {
          return Collections.emptyList();
        }
        end = doTrace ? System.nanoTime() : 0;
        MetastoreDirectSqlUtils.timingTrace(doTrace, queryText, start, end);
        List<Object[]> list = MetastoreDirectSqlUtils.ensureList(qResult);
        List<ColumnStatisticsObj> colStats =
            new ArrayList<ColumnStatisticsObj>(list.size());
        for (Object[] row : list) {
          colStats.add(prepareCSObjWithAdjustedNDV(row, 0,
              useDensityFunctionForNDVEstimation, ndvTuner));
          Deadline.checkTimeout();
        }
        return colStats;
      }
    } else {
      // Extrapolation is needed for some columns.
      // In this case, at least a column status for a partition is missing.
      // We need to extrapolate this partition based on the other partitions
      List<ColumnStatisticsObj> colStats = new ArrayList<ColumnStatisticsObj>(colNames.size());
      queryText = "select \"COLUMN_NAME\", \"COLUMN_TYPE\", count(\"PARTITION_NAME\") "
          + " from " + PART_COL_STATS
          + " where \"CAT_NAME\" = ? and \"DB_NAME\" = ? and \"TABLE_NAME\" = ? "
          + " and \"COLUMN_NAME\" in (" + makeParams(colNames.size()) + ")"
          + " and \"PARTITION_NAME\" in (" + makeParams(partNames.size()) + ")"
          + " and \"ENGINE\" = ? "
          + " group by \"COLUMN_NAME\", \"COLUMN_TYPE\"";
      start = doTrace ? System.nanoTime() : 0;
      List<String> noExtraColumnNames = new ArrayList<String>();
      Map<String, String[]> extraColumnNameTypeParts = new HashMap<String, String[]>();
      try(QueryWrapper query = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
        Object qResult = executeWithArray(query.getInnerQuery(),
            prepareParams(catName, dbName, tableName, partNames, colNames,
                engine), queryText);
        end = doTrace ? System.nanoTime() : 0;
        MetastoreDirectSqlUtils.timingTrace(doTrace, queryText, start, end);
        if (qResult == null) {
          return Collections.emptyList();
        }

        List<Object[]> list = MetastoreDirectSqlUtils.ensureList(qResult);
        for (Object[] row : list) {
          String colName = (String) row[0];
          String colType = (String) row[1];
          // Extrapolation is not needed for this column if
          // count(\"PARTITION_NAME\")==partNames.size()
          // Or, extrapolation is not possible for this column if
          // count(\"PARTITION_NAME\")<2
          Long count = MetastoreDirectSqlUtils.extractSqlLong(row[2]);
          if (count == partNames.size() || count < 2) {
            noExtraColumnNames.add(colName);
          } else {
            extraColumnNameTypeParts.put(colName, new String[] {colType, String.valueOf(count)});
          }
          Deadline.checkTimeout();
        }
      }
      // Extrapolation is not needed for columns noExtraColumnNames
      List<Object[]> list;
      if (noExtraColumnNames.size() != 0) {
        queryText = commonPrefix + " and \"COLUMN_NAME\" in ("
            + makeParams(noExtraColumnNames.size()) + ")" + " and \"PARTITION_NAME\" in ("
            + makeParams(partNames.size()) + ")"
            + " and \"ENGINE\" = ? "
            + " group by \"COLUMN_NAME\", \"COLUMN_TYPE\"";
        start = doTrace ? System.nanoTime() : 0;

        try (QueryWrapper query = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
          Object qResult = executeWithArray(query.getInnerQuery(),
              prepareParams(catName, dbName, tableName, partNames, noExtraColumnNames, engine), queryText);
          if (qResult == null) {
            return Collections.emptyList();
          }
          list = MetastoreDirectSqlUtils.ensureList(qResult);
          for (Object[] row : list) {
            colStats.add(prepareCSObjWithAdjustedNDV(row, 0,
                useDensityFunctionForNDVEstimation, ndvTuner));
            Deadline.checkTimeout();
          }
          end = doTrace ? System.nanoTime() : 0;
          MetastoreDirectSqlUtils.timingTrace(doTrace, queryText, start, end);
        }
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
            + " from " + PART_COL_STATS + " where \"CAT_NAME\" = ? and \"DB_NAME\" = ? and \"TABLE_NAME\" = ? "
            + " and \"COLUMN_NAME\" in (" + makeParams(extraColumnNameTypeParts.size()) + ")"
            + " and \"PARTITION_NAME\" in (" + makeParams(partNames.size()) + ")"
            + " and \"ENGINE\" = ? "
            + " group by \"COLUMN_NAME\"";
        start = doTrace ? System.nanoTime() : 0;
        try (QueryWrapper query = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
          List<String> extraColumnNames = new ArrayList<String>();
          extraColumnNames.addAll(extraColumnNameTypeParts.keySet());
          Object qResult = executeWithArray(query.getInnerQuery(),
              prepareParams(catName, dbName, tableName, partNames,
                  extraColumnNames, engine), queryText);
          if (qResult == null) {
            return Collections.emptyList();
          }
          list = MetastoreDirectSqlUtils.ensureList(qResult);
          // see the indexes for colstats in IExtrapolatePartStatus
          Integer[] sumIndex = new Integer[] {6, 10, 11, 15};
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
          MetastoreDirectSqlUtils.timingTrace(doTrace, queryText, start, end);
        }
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
                Long val = MetastoreDirectSqlUtils.extractSqlLong(o);
                row[2 + colStatIndex] = val / sumVal * (partNames.size());
              }
            } else if (IExtrapolatePartStatus.aggrTypes[colStatIndex] == IExtrapolatePartStatus.AggrType.Min
                || IExtrapolatePartStatus.aggrTypes[colStatIndex] == IExtrapolatePartStatus.AggrType.Max) {
              // if the aggregation type is min/max, we extrapolate from the
              // left/right borders
              if (!decimal) {
                queryText = "select \"" + colStatName
                    + "\",\"PARTITION_NAME\" from " + PART_COL_STATS
                    + " where \"CAT_NAME\" = ? and \"DB_NAME\" = ? and \"TABLE_NAME\" = ?" + " and \"COLUMN_NAME\" = ?"
                    + " and \"PARTITION_NAME\" in (" + makeParams(partNames.size()) + ")"
                    + " and \"ENGINE\" = ? "
                    + " order by \"" + colStatName + "\"";
              } else {
                queryText = "select \"" + colStatName
                    + "\",\"PARTITION_NAME\" from " + PART_COL_STATS
                    + " where \"CAT_NAME\" = ? and \"DB_NAME\" = ? and \"TABLE_NAME\" = ?" + " and \"COLUMN_NAME\" = ?"
                    + " and \"PARTITION_NAME\" in (" + makeParams(partNames.size()) + ")"
                    + " and \"ENGINE\" = ? "
                    + " order by cast(\"" + colStatName + "\" as decimal)";
              }
              start = doTrace ? System.nanoTime() : 0;
              try (QueryWrapper query = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
                Object qResult = executeWithArray(query.getInnerQuery(),
                    prepareParams(catName, dbName, tableName, partNames, Arrays.asList(colName), engine), queryText);
                if (qResult == null) {
                  return Collections.emptyList();
                }
                fqr = (ForwardQueryResult<?>) qResult;
                Object[] min = (Object[]) (fqr.get(0));
                Object[] max = (Object[]) (fqr.get(fqr.size() - 1));
                end = doTrace ? System.nanoTime() : 0;
                MetastoreDirectSqlUtils.timingTrace(doTrace, queryText, start, end);
                if (min[0] == null || max[0] == null) {
                  row[2 + colStatIndex] = null;
                } else {
                  row[2 + colStatIndex] = extrapolateMethod
                      .extrapolate(min, max, colStatIndex, indexMap);
                }
              }
            } else {
              // if the aggregation type is avg, we use the average on the existing ones.
              queryText = "select "
                  + "avg((\"LONG_HIGH_VALUE\"-\"LONG_LOW_VALUE\")/cast(\"NUM_DISTINCTS\" as decimal)),"
                  + "avg((\"DOUBLE_HIGH_VALUE\"-\"DOUBLE_LOW_VALUE\")/\"NUM_DISTINCTS\"),"
                  + "avg((cast(\"BIG_DECIMAL_HIGH_VALUE\" as decimal)-cast(\"BIG_DECIMAL_LOW_VALUE\" as decimal))/\"NUM_DISTINCTS\")"
                  + " from " + PART_COL_STATS + "" + " where \"CAT_NAME\" = ? and \"DB_NAME\" = ? and \"TABLE_NAME\" = ?"
                  + " and \"COLUMN_NAME\" = ?" + " and \"PARTITION_NAME\" in ("
                  + makeParams(partNames.size()) + ")"
                  + " and \"ENGINE\" = ? "
                  + " group by \"COLUMN_NAME\"";
              start = doTrace ? System.nanoTime() : 0;
              try(QueryWrapper query = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
                Object qResult = executeWithArray(query.getInnerQuery(),
                    prepareParams(catName, dbName, tableName, partNames, Arrays.asList(colName), engine), queryText);
                if (qResult == null) {
                  return Collections.emptyList();
                }
                fqr = (ForwardQueryResult<?>) qResult;
                Object[] avg = (Object[]) (fqr.get(0));
                // colStatIndex=12,13,14 respond to "AVG_LONG", "AVG_DOUBLE",
                // "AVG_DECIMAL"
                row[2 + colStatIndex] = avg[colStatIndex - 12];
                end = doTrace ? System.nanoTime() : 0;
                MetastoreDirectSqlUtils.timingTrace(doTrace, queryText, start, end);
              }
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
        histogram = row[i++], avglen = row[i++], maxlen = row[i++], trues = row[i++], falses = row[i];
    StatObjectConverter.fillColumnStatisticsData(cso.getColType(), data,
        llow, lhigh, dlow, dhigh, declow, dechigh, nulls, dist, bitVector, histogram, avglen, maxlen, trues, falses);
    return cso;
  }

  private ColumnStatisticsObj prepareCSObjWithAdjustedNDV(Object[] row, int i,
      boolean useDensityFunctionForNDVEstimation, double ndvTuner) throws MetaException {
    ColumnStatisticsData data = new ColumnStatisticsData();
    ColumnStatisticsObj cso = new ColumnStatisticsObj((String) row[i++], (String) row[i++], data);
    Object llow = row[i++], lhigh = row[i++], dlow = row[i++], dhigh = row[i++], declow = row[i++],
        dechigh = row[i++], nulls = row[i++], dist = row[i++], avglen = row[i++], maxlen = row[i++],
        trues = row[i++], falses = row[i++], avgLong = row[i++], avgDouble = row[i++],
        avgDecimal = row[i++], sumDist = row[i++];
    StatObjectConverter.fillColumnStatisticsData(cso.getColType(), data, llow, lhigh, dlow, dhigh,
        declow, dechigh, nulls, dist, avglen, maxlen, trues, falses, avgLong, avgDouble,
        avgDecimal, sumDist, useDensityFunctionForNDVEstimation, ndvTuner);
    return cso;
  }

  private Object[] prepareParams(String catName, String dbName, String tableName,
      List<String> partNames, List<String> colNames, String engine) throws MetaException {
    Object[] params = new Object[colNames.size() + partNames.size() + 4];
    int paramI = 0;
    params[paramI++] = catName;
    params[paramI++] = dbName;
    params[paramI++] = tableName;
    for (String colName : colNames) {
      params[paramI++] = colName;
    }
    for (String partName : partNames) {
      params[paramI++] = partName;
    }
    params[paramI++] = engine;

    return params;
  }

  public List<ColumnStatistics> getPartitionStats(
      final String catName, final String dbName, final String tableName, final List<String> partNames,
      List<String> colNames, String engine, boolean enableBitVector, boolean enableKll) throws MetaException {
    if (colNames.isEmpty() || partNames.isEmpty()) {
      return Collections.emptyList();
    }
    final boolean doTrace = LOG.isDebugEnabled();
    final String queryText0 = "select \"PARTITION_NAME\", " + getStatsList(enableBitVector, enableKll) + " from "
        + " " + PART_COL_STATS + " where \"CAT_NAME\" = ? and \"DB_NAME\" = ? and \"TABLE_NAME\" = ? and " +
        "\"COLUMN_NAME\""
        + "  in (%1$s) AND \"PARTITION_NAME\" in (%2$s) "
        + " and \"ENGINE\" = ? "
        + " order by \"PARTITION_NAME\"";
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
            try {
              Object qResult = executeWithArray(query, prepareParams(
                  catName, dbName, tableName, inputPartNames, inputColNames, engine), queryText);
              MetastoreDirectSqlUtils.timingTrace(doTrace, queryText0, start, (doTrace ? System.nanoTime() : 0));
              if (qResult == null) {
                return Collections.emptyList();
              }
              return MetastoreDirectSqlUtils.ensureList(qResult);
            } finally {
              addQueryAfterUse(query);
            }
          }
        };
        try {
          return Batchable.runBatched(batchSize, partNames, b2);
        } finally {
          addQueryAfterUse(b2);
        }
      }
    };

    List<ColumnStatistics> result = new ArrayList<ColumnStatistics>(partNames.size());
    String lastPartName = null;
    int from = 0;
    try {
      List<Object[]> list = Batchable.runBatched(batchSize, colNames, b);
      for (int i = 0; i <= list.size(); ++i) {
        boolean isLast = i == list.size();
        String partName = isLast ? null : (String) list.get(i)[0];
        if (!isLast && partName.equals(lastPartName)) {
          continue;
        } else if (from != i) {
          ColumnStatisticsDesc csd =
              new ColumnStatisticsDesc(false, dbName, tableName);
          csd.setCatName(catName);
          csd.setPartName(lastPartName);
          result.add(makeColumnStats(list.subList(from, i), csd, 1, engine));
        }
        lastPartName = partName;
        from = i;
        Deadline.checkTimeout();
      }
    } finally {
      b.closeAllQueries();
    }
    return result;
  }

  /** The common query part for table and partition stats */
  private String getStatsList(boolean enableBitVector, boolean enableKll) {
    return "\"COLUMN_NAME\", \"COLUMN_TYPE\", \"LONG_LOW_VALUE\", \"LONG_HIGH_VALUE\", "
        + "\"DOUBLE_LOW_VALUE\", \"DOUBLE_HIGH_VALUE\", \"BIG_DECIMAL_LOW_VALUE\", "
        + "\"BIG_DECIMAL_HIGH_VALUE\", \"NUM_NULLS\", \"NUM_DISTINCTS\", "
        + (enableBitVector ? "\"BIT_VECTOR\", " : "\'\', ")
        + (enableKll ? "\"HISTOGRAM\", " : "\'\', ")
        + "\"AVG_COL_LEN\", \"MAX_COL_LEN\", \"NUM_TRUES\", \"NUM_FALSES\", \"LAST_ANALYZED\" ";
  }

  private ColumnStatistics makeColumnStats(
      List<Object[]> list, ColumnStatisticsDesc csd, int offset, String engine) throws MetaException {
    ColumnStatistics result = new ColumnStatistics();
    result.setStatsDesc(csd);
    List<ColumnStatisticsObj> csos = new ArrayList<ColumnStatisticsObj>(list.size());
    for (Object[] row : list) {
      // LastAnalyzed is stored per column but thrift has it per several;
      // get the lowest for now as nobody actually uses this field.
      Object laObj = row[offset + 16]; // 16 is the offset of "last analyzed" field
      if (laObj != null && (!csd.isSetLastAnalyzed() || csd.getLastAnalyzed() > MetastoreDirectSqlUtils
          .extractSqlLong(laObj))) {
        csd.setLastAnalyzed(MetastoreDirectSqlUtils.extractSqlLong(laObj));
      }
      csos.add(prepareCSObj(row, offset));
      Deadline.checkTimeout();
    }
    result.setStatsObj(csos);
    result.setEngine(engine);
    return result;
  }

  private String makeParams(int size) {
    // W/ size 0, query will fail, but at least we'd get to see the query in debug output.
    return (size == 0) ? "" : repeat(",?", size).substring(1);
  }

  @SuppressWarnings("unchecked")
  private <T> T executeWithArray(Query query, Object[] params, String sql) throws MetaException {
    return executeWithArray(query, params, sql, -1);
  }

  @SuppressWarnings("unchecked")
  private <T> T executeWithArray(Query query, Object[] params, String sql, int limit) throws MetaException {
    return MetastoreDirectSqlUtils.executeWithArray(query, params, sql, limit);
  }

  /**
   * This run the necessary logic to prepare for queries. It should be called once, after the
   * txn on DataNucleus connection is opened, and before any queries are issued. What it does
   * currently is run db-specific logic, e.g. setting ansi quotes mode for MySQL. The reason it
   * must be used inside of the txn is connection pooling; there's no way to guarantee that the
   * effect will apply to the connection that is executing the queries otherwise.
   */
  public void prepareTxn() throws MetaException {
  	String stmt = dbType.getPrepareTxnStmt();
    if (stmt == null) {
      return;
    }
    try {
      assert pm.currentTransaction().isActive(); // must be inside tx together with queries
      executeNoResult(stmt);
    } catch (SQLException sqlEx) {
      throw new MetaException("Error setting ansi quotes: " + sqlEx.getMessage());
    }
  }


  public List<SQLForeignKey> getForeignKeys(String catName, String parent_db_name,
                                            String parent_tbl_name, String foreign_db_name,
                                            String foreign_tbl_name) throws MetaException {
    List<SQLForeignKey> ret = new ArrayList<>();
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
      + " " + DBS + ".\"CTLG_NAME\" = ? AND"
      + (foreign_db_name == null ? "" : " " + DBS + ".\"NAME\" = ? AND")
      + (foreign_tbl_name == null ? "" : " " + TBLS + ".\"TBL_NAME\" = ? AND")
      + (parent_tbl_name == null ? "" : " \"T2\".\"TBL_NAME\" = ? AND")
      + (parent_db_name == null ? "" : " \"D2\".\"NAME\" = ?") ;

    queryText = queryText.trim();
    if (queryText.endsWith("AND")) {
      queryText = queryText.substring(0, queryText.length()-3);
    }
    List<String> pms = new ArrayList<String>();
    pms.add(catName);
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

    try (QueryWrapper queryParams = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
      List<Object[]> sqlResult = MetastoreDirectSqlUtils.ensureList(executeWithArray(
          queryParams.getInnerQuery(), pms.toArray(), queryText));

      if (!sqlResult.isEmpty()) {
        for (Object[] line : sqlResult) {
          int enableValidateRely = MetastoreDirectSqlUtils.extractSqlInt(line[11]);
          boolean enable = (enableValidateRely & 4) != 0;
          boolean validate = (enableValidateRely & 2) != 0;
          boolean rely = (enableValidateRely & 1) != 0;
          SQLForeignKey currKey = new SQLForeignKey(
              MetastoreDirectSqlUtils.extractSqlString(line[0]),
              MetastoreDirectSqlUtils.extractSqlString(line[1]),
              MetastoreDirectSqlUtils.extractSqlString(line[2]),
              MetastoreDirectSqlUtils.extractSqlString(line[3]),
              MetastoreDirectSqlUtils.extractSqlString(line[4]),
              MetastoreDirectSqlUtils.extractSqlString(line[5]),
              MetastoreDirectSqlUtils.extractSqlInt(line[6]),
              MetastoreDirectSqlUtils.extractSqlInt(line[7]),
              MetastoreDirectSqlUtils.extractSqlInt(line[8]),
              MetastoreDirectSqlUtils.extractSqlString(line[9]),
              MetastoreDirectSqlUtils.extractSqlString(line[10]),
              enable,
              validate,
              rely
          );
          currKey.setCatName(catName);
          ret.add(currKey);
        }
      }
      return ret;
    }
  }

  public List<SQLPrimaryKey> getPrimaryKeys(String catName, String db_name, String tbl_name)
      throws MetaException {
    List<SQLPrimaryKey> ret = new ArrayList<>();
    String queryText =
      "SELECT " + DBS + ".\"NAME\", " + TBLS + ".\"TBL_NAME\", "
      + "CASE WHEN " + COLUMNS_V2 + ".\"COLUMN_NAME\" IS NOT NULL THEN " + COLUMNS_V2 + ".\"COLUMN_NAME\" "
      + "ELSE " + PARTITION_KEYS + ".\"PKEY_NAME\" END, " + KEY_CONSTRAINTS + ".\"POSITION\", "
      + KEY_CONSTRAINTS + ".\"CONSTRAINT_NAME\", " + KEY_CONSTRAINTS + ".\"ENABLE_VALIDATE_RELY\", "
      + DBS + ".\"CTLG_NAME\""
      + " from " + TBLS + " "
      + " INNER JOIN " + KEY_CONSTRAINTS + " ON " + TBLS + ".\"TBL_ID\" = " + KEY_CONSTRAINTS + ".\"PARENT_TBL_ID\" "
      + " INNER JOIN " + DBS + " ON " + TBLS + ".\"DB_ID\" = " + DBS + ".\"DB_ID\" "
      + " LEFT OUTER JOIN " + COLUMNS_V2 + " ON " + COLUMNS_V2 + ".\"CD_ID\" = " + KEY_CONSTRAINTS + ".\"PARENT_CD_ID\" AND "
      + " " + COLUMNS_V2 + ".\"INTEGER_IDX\" = " + KEY_CONSTRAINTS + ".\"PARENT_INTEGER_IDX\" "
      + " LEFT OUTER JOIN " + PARTITION_KEYS + " ON " + TBLS + ".\"TBL_ID\" = " + PARTITION_KEYS + ".\"TBL_ID\" AND "
      + " " + PARTITION_KEYS + ".\"INTEGER_IDX\" = " + KEY_CONSTRAINTS + ".\"PARENT_INTEGER_IDX\" "
      + " WHERE " + KEY_CONSTRAINTS + ".\"CONSTRAINT_TYPE\" = "+ MConstraint.PRIMARY_KEY_CONSTRAINT + " AND"
      + " " + DBS + ".\"CTLG_NAME\" = ? AND"
      + (db_name == null ? "" : " " + DBS + ".\"NAME\" = ? AND")
      + (tbl_name == null ? "" : " " + TBLS + ".\"TBL_NAME\" = ? ") ;

    queryText = queryText.trim();
    if (queryText.endsWith("AND")) {
      queryText = queryText.substring(0, queryText.length()-3);
    }
    List<String> pms = new ArrayList<>();
    pms.add(catName);
    if (db_name != null) {
      pms.add(db_name);
    }
    if (tbl_name != null) {
      pms.add(tbl_name);
    }

    try (QueryWrapper queryParams = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
      List<Object[]> sqlResult = MetastoreDirectSqlUtils.ensureList(executeWithArray(
          queryParams.getInnerQuery(), pms.toArray(), queryText));

      if (!sqlResult.isEmpty()) {
        for (Object[] line : sqlResult) {
          int enableValidateRely = MetastoreDirectSqlUtils.extractSqlInt(line[5]);
          boolean enable = (enableValidateRely & 4) != 0;
          boolean validate = (enableValidateRely & 2) != 0;
          boolean rely = (enableValidateRely & 1) != 0;
          SQLPrimaryKey currKey = new SQLPrimaryKey(
              MetastoreDirectSqlUtils.extractSqlString(line[0]),
              MetastoreDirectSqlUtils.extractSqlString(line[1]),
              MetastoreDirectSqlUtils.extractSqlString(line[2]),
              MetastoreDirectSqlUtils.extractSqlInt(line[3]), MetastoreDirectSqlUtils.extractSqlString(line[4]),
              enable,
              validate,
              rely);
          currKey.setCatName(MetastoreDirectSqlUtils.extractSqlString(line[6]));
          ret.add(currKey);
        }
      }
      return ret;
    }
  }

  public List<SQLUniqueConstraint> getUniqueConstraints(String catName, String db_name, String tbl_name)
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
      + " " + DBS + ".\"CTLG_NAME\" = ? AND"
      + (db_name == null ? "" : " " + DBS + ".\"NAME\" = ? AND")
      + (tbl_name == null ? "" : " " + TBLS + ".\"TBL_NAME\" = ? ") ;

    queryText = queryText.trim();
    if (queryText.endsWith("AND")) {
      queryText = queryText.substring(0, queryText.length()-3);
    }
    List<String> pms = new ArrayList<String>();
    pms.add(catName);
    if (db_name != null) {
      pms.add(db_name);
    }
    if (tbl_name != null) {
      pms.add(tbl_name);
    }

    try (QueryWrapper queryParams = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
      List<Object[]> sqlResult = MetastoreDirectSqlUtils.ensureList(executeWithArray(
          queryParams.getInnerQuery(), pms.toArray(), queryText));

      if (!sqlResult.isEmpty()) {
        for (Object[] line : sqlResult) {
          int enableValidateRely = MetastoreDirectSqlUtils.extractSqlInt(line[5]);
          boolean enable = (enableValidateRely & 4) != 0;
          boolean validate = (enableValidateRely & 2) != 0;
          boolean rely = (enableValidateRely & 1) != 0;
          ret.add(new SQLUniqueConstraint(
              catName,
              MetastoreDirectSqlUtils.extractSqlString(line[0]),
              MetastoreDirectSqlUtils.extractSqlString(line[1]),
              MetastoreDirectSqlUtils.extractSqlString(line[2]),
              MetastoreDirectSqlUtils.extractSqlInt(line[3]), MetastoreDirectSqlUtils.extractSqlString(line[4]),
              enable,
              validate,
              rely));
        }
      }
      return ret;
    }
  }

  public List<SQLNotNullConstraint> getNotNullConstraints(String catName, String db_name, String tbl_name)
          throws MetaException {
    List<SQLNotNullConstraint> ret = new ArrayList<>();
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
      + " " + DBS + ".\"CTLG_NAME\" = ? AND"
      + (db_name == null ? "" : " " + DBS + ".\"NAME\" = ? AND")
      + (tbl_name == null ? "" : " " + TBLS + ".\"TBL_NAME\" = ? ") ;

    queryText = queryText.trim();
    if (queryText.endsWith("AND")) {
      queryText = queryText.substring(0, queryText.length()-3);
    }
    List<String> pms = new ArrayList<>();
    pms.add(catName);
    if (db_name != null) {
      pms.add(db_name);
    }
    if (tbl_name != null) {
      pms.add(tbl_name);
    }

    try (QueryWrapper queryParams = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
      List<Object[]> sqlResult = MetastoreDirectSqlUtils.ensureList(executeWithArray(
          queryParams.getInnerQuery(), pms.toArray(), queryText));

      if (!sqlResult.isEmpty()) {
        for (Object[] line : sqlResult) {
          int enableValidateRely = MetastoreDirectSqlUtils.extractSqlInt(line[4]);
          boolean enable = (enableValidateRely & 4) != 0;
          boolean validate = (enableValidateRely & 2) != 0;
          boolean rely = (enableValidateRely & 1) != 0;
          ret.add(new SQLNotNullConstraint(
              catName,
              MetastoreDirectSqlUtils.extractSqlString(line[0]),
              MetastoreDirectSqlUtils.extractSqlString(line[1]),
              MetastoreDirectSqlUtils.extractSqlString(line[2]),
              MetastoreDirectSqlUtils.extractSqlString(line[3]),
              enable,
              validate,
              rely));
        }
      }
      return ret;
    }
  }

  public List<SQLDefaultConstraint> getDefaultConstraints(String catName, String db_name, String tbl_name)
      throws MetaException {
    List<SQLDefaultConstraint> ret = new ArrayList<SQLDefaultConstraint>();
    String queryText =
        "SELECT " + DBS + ".\"NAME\", " + TBLS + ".\"TBL_NAME\","
            + "CASE WHEN " + COLUMNS_V2 + ".\"COLUMN_NAME\" IS NOT NULL THEN " + COLUMNS_V2 + ".\"COLUMN_NAME\" "
            + "ELSE " + PARTITION_KEYS + ".\"PKEY_NAME\" END, "
            + "" + KEY_CONSTRAINTS + ".\"CONSTRAINT_NAME\", " + KEY_CONSTRAINTS + ".\"ENABLE_VALIDATE_RELY\", "
            + "" + KEY_CONSTRAINTS + ".\"DEFAULT_VALUE\" "
            + " from " + TBLS + " "
            + " INNER JOIN " + KEY_CONSTRAINTS + " ON " + TBLS + ".\"TBL_ID\" = " + KEY_CONSTRAINTS + ".\"PARENT_TBL_ID\" "
            + " INNER JOIN " + DBS + " ON " + TBLS + ".\"DB_ID\" = " + DBS + ".\"DB_ID\" "
            + " LEFT OUTER JOIN " + COLUMNS_V2 + " ON " + COLUMNS_V2 + ".\"CD_ID\" = " + KEY_CONSTRAINTS + ".\"PARENT_CD_ID\" AND "
            + " " + COLUMNS_V2 + ".\"INTEGER_IDX\" = " + KEY_CONSTRAINTS + ".\"PARENT_INTEGER_IDX\" "
            + " LEFT OUTER JOIN " + PARTITION_KEYS + " ON " + TBLS + ".\"TBL_ID\" = " + PARTITION_KEYS + ".\"TBL_ID\" AND "
            + " " + PARTITION_KEYS + ".\"INTEGER_IDX\" = " + KEY_CONSTRAINTS + ".\"PARENT_INTEGER_IDX\" "
            + " WHERE " + KEY_CONSTRAINTS + ".\"CONSTRAINT_TYPE\" = "+ MConstraint.DEFAULT_CONSTRAINT+ " AND"
            + " " + DBS + ".\"CTLG_NAME\" = ? AND"
            + (db_name == null ? "" : " " + DBS + ".\"NAME\" = ? AND")
            + (tbl_name == null ? "" : " " + TBLS + ".\"TBL_NAME\" = ? ") ;

    queryText = queryText.trim();
    if (queryText.endsWith("AND")) {
      queryText = queryText.substring(0, queryText.length()-3);
    }
    if (LOG.isDebugEnabled()){
      LOG.debug("getDefaultConstraints: directsql : " + queryText);
    }
    List<String> pms = new ArrayList<>();
    pms.add(catName);
    if (db_name != null) {
      pms.add(db_name);
    }
    if (tbl_name != null) {
      pms.add(tbl_name);
    }

    try (QueryWrapper queryParams = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
      List<Object[]> sqlResult = MetastoreDirectSqlUtils.ensureList(executeWithArray(
          queryParams.getInnerQuery(), pms.toArray(), queryText));

      if (!sqlResult.isEmpty()) {
        for (Object[] line : sqlResult) {
          int enableValidateRely = MetastoreDirectSqlUtils.extractSqlInt(line[4]);
          boolean enable = (enableValidateRely & 4) != 0;
          boolean validate = (enableValidateRely & 2) != 0;
          boolean rely = (enableValidateRely & 1) != 0;
          SQLDefaultConstraint currConstraint = new SQLDefaultConstraint(
              catName,
              MetastoreDirectSqlUtils.extractSqlString(line[0]),
              MetastoreDirectSqlUtils.extractSqlString(line[1]),
              MetastoreDirectSqlUtils.extractSqlString(line[2]),
              MetastoreDirectSqlUtils.extractSqlString(line[5]),
              MetastoreDirectSqlUtils.extractSqlString(line[3]),
              enable,
              validate,
              rely);
          ret.add(currConstraint);
        }
      }
      return ret;
    }
  }

  public List<SQLCheckConstraint> getCheckConstraints(String catName, String db_name, String tbl_name)
      throws MetaException {
    List<SQLCheckConstraint> ret = new ArrayList<SQLCheckConstraint>();
    String queryText =
        "SELECT " + DBS + ".\"NAME\", " + TBLS + ".\"TBL_NAME\","
            + "CASE WHEN " + COLUMNS_V2 + ".\"COLUMN_NAME\" IS NOT NULL THEN " + COLUMNS_V2 + ".\"COLUMN_NAME\" "
            + "ELSE " + PARTITION_KEYS + ".\"PKEY_NAME\" END, "
            + "" + KEY_CONSTRAINTS + ".\"CONSTRAINT_NAME\", " + KEY_CONSTRAINTS + ".\"ENABLE_VALIDATE_RELY\", "
            + "" + KEY_CONSTRAINTS + ".\"DEFAULT_VALUE\" "
            + " from " + TBLS + " "
            + " INNER JOIN " + KEY_CONSTRAINTS + " ON " + TBLS + ".\"TBL_ID\" = " + KEY_CONSTRAINTS + ".\"PARENT_TBL_ID\" "
            + " INNER JOIN " + DBS + " ON " + TBLS + ".\"DB_ID\" = " + DBS + ".\"DB_ID\" "
            + " LEFT OUTER JOIN " + COLUMNS_V2 + " ON " + COLUMNS_V2 + ".\"CD_ID\" = " + KEY_CONSTRAINTS + ".\"PARENT_CD_ID\" AND "
            + " " + COLUMNS_V2 + ".\"INTEGER_IDX\" = " + KEY_CONSTRAINTS + ".\"PARENT_INTEGER_IDX\" "
            + " LEFT OUTER JOIN " + PARTITION_KEYS + " ON " + TBLS + ".\"TBL_ID\" = " + PARTITION_KEYS + ".\"TBL_ID\" AND "
            + " " + PARTITION_KEYS + ".\"INTEGER_IDX\" = " + KEY_CONSTRAINTS + ".\"PARENT_INTEGER_IDX\" "
            + " WHERE " + KEY_CONSTRAINTS + ".\"CONSTRAINT_TYPE\" = "+ MConstraint.CHECK_CONSTRAINT+ " AND"
            + " " + DBS + ".\"CTLG_NAME\" = ? AND"
            + (db_name == null ? "" : " " + DBS + ".\"NAME\" = ? AND")
            + (tbl_name == null ? "" : " " + TBLS + ".\"TBL_NAME\" = ? ") ;

    queryText = queryText.trim();
    if (queryText.endsWith("AND")) {
      queryText = queryText.substring(0, queryText.length()-3);
    }
    if (LOG.isDebugEnabled()){
      LOG.debug("getCheckConstraints: directsql : " + queryText);
    }
    List<String> pms = new ArrayList<>();
    pms.add(catName);
    if (db_name != null) {
      pms.add(db_name);
    }
    if (tbl_name != null) {
      pms.add(tbl_name);
    }

    try (QueryWrapper queryParams = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
      List<Object[]> sqlResult = MetastoreDirectSqlUtils.ensureList(executeWithArray(
          queryParams.getInnerQuery(), pms.toArray(), queryText));

      if (!sqlResult.isEmpty()) {
        for (Object[] line : sqlResult) {
          int enableValidateRely = MetastoreDirectSqlUtils.extractSqlInt(line[4]);
          boolean enable = (enableValidateRely & 4) != 0;
          boolean validate = (enableValidateRely & 2) != 0;
          boolean rely = (enableValidateRely & 1) != 0;
          SQLCheckConstraint currConstraint = new SQLCheckConstraint(
              catName,
              MetastoreDirectSqlUtils.extractSqlString(line[0]),
              MetastoreDirectSqlUtils.extractSqlString(line[1]),
              MetastoreDirectSqlUtils.extractSqlString(line[2]),
              MetastoreDirectSqlUtils.extractSqlString(line[5]),
              MetastoreDirectSqlUtils.extractSqlString(line[3]),
              enable,
              validate,
              rely);
          ret.add(currConstraint);
        }
      }
      return ret;
    }
  }

  /**
   * Drop partitions by using direct SQL queries.
   * @param catName Metastore catalog name.
   * @param dbName Metastore db name.
   * @param tblName Metastore table name.
   * @param partNames Partition names to get.
   * @return List of partitions.
   */
  public void dropPartitionsViaSqlFilter(final String catName, final String dbName,
                                         final String tblName, List<String> partNames)
      throws MetaException {
    if (partNames.isEmpty()) {
      return;
    }

    Batchable.runBatched(batchSize, partNames, new Batchable<String, Void>() {
      @Override
      public List<Void> run(List<String> input) throws MetaException {
        String filter = "" + PARTITIONS + ".\"PART_NAME\" in (" + makeParams(input.size()) + ")";
        // Get partition ids
        List<Long> partitionIds = getPartitionIdsViaSqlFilter(catName, dbName, tblName,
            filter, input, Collections.<String>emptyList(), null);
        if (partitionIds.isEmpty()) {
          return Collections.emptyList(); // no partitions, bail early.
        }
        dropPartitionsByPartitionIds(partitionIds);
        return Collections.emptyList();
      }
    });
  }


  /**
   * Drops Partition-s. Should be called with the list short enough to not trip up Oracle/etc.
   * @param partitionIdList The partition identifiers to drop
   * @throws MetaException If there is an SQL exception during the execution it converted to
   * MetaException
   */
  private void dropPartitionsByPartitionIds(List<Long> partitionIdList) throws MetaException {
    String queryText;
    if (partitionIdList.isEmpty()) {
      return;
    }

    String partitionIds = getIdListForIn(partitionIdList);

    // Get the corresponding SD_ID-s, CD_ID-s, SERDE_ID-s
    queryText =
        "SELECT " + SDS + ".\"SD_ID\", " + SDS + ".\"CD_ID\", " + SDS + ".\"SERDE_ID\" "
            + "from " + SDS + " "
            + "INNER JOIN " + PARTITIONS + " ON " + PARTITIONS + ".\"SD_ID\" = " + SDS + ".\"SD_ID\" "
            + "WHERE " + PARTITIONS + ".\"PART_ID\" in (" + partitionIds + ")";

    List<Object> sdIdList = new ArrayList<>(partitionIdList.size());
    List<Long> columnDescriptorIdList = new ArrayList<>(1);
    List<Object> serdeIdList = new ArrayList<>(partitionIdList.size());
    try (QueryWrapper wrapper = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
      List<Object[]> sqlResult = MetastoreDirectSqlUtils
          .ensureList(executeWithArray(wrapper.getInnerQuery(), null, queryText));

      if (!sqlResult.isEmpty()) {
        for (Object[] fields : sqlResult) {
          sdIdList.add(MetastoreDirectSqlUtils.extractSqlLong(fields[0]));
          Long colId = MetastoreDirectSqlUtils.extractSqlLong(fields[1]);
          if (!columnDescriptorIdList.contains(colId)) {
            columnDescriptorIdList.add(colId);
          }
          serdeIdList.add(MetastoreDirectSqlUtils.extractSqlLong(fields[2]));
        }
      }
    }
    try {
      // Drop privileges
      queryText = "delete from " + PART_PRIVS + " where \"PART_ID\" in (" + partitionIds + ")";
      executeNoResult(queryText);
      Deadline.checkTimeout();

      // Drop column level privileges
      queryText = "delete from " + PART_COL_PRIVS + " where \"PART_ID\" in (" + partitionIds + ")";
      executeNoResult(queryText);
      Deadline.checkTimeout();

      // Drop partition statistics
      queryText = "delete from " + PART_COL_STATS + " where \"PART_ID\" in (" + partitionIds + ")";
      executeNoResult(queryText);
      Deadline.checkTimeout();

      // Drop the partition params
      queryText = "delete from " + PARTITION_PARAMS + " where \"PART_ID\" in ("
          + partitionIds + ")";
      executeNoResult(queryText);
      Deadline.checkTimeout();

      // Drop the partition key vals
      queryText = "delete from " + PARTITION_KEY_VALS + " where \"PART_ID\" in ("
          + partitionIds + ")";
      executeNoResult(queryText);
      Deadline.checkTimeout();

      // Drop the partitions
      queryText = "delete from " + PARTITIONS + " where \"PART_ID\" in (" + partitionIds + ")";
      executeNoResult(queryText);
      Deadline.checkTimeout();
    } catch (SQLException sqlException) {
      LOG.warn("SQL error executing query while dropping partition", sqlException);
      throw new MetaException("Encountered error while dropping partitions.");
    }
    dropStorageDescriptors(sdIdList);
    Deadline.checkTimeout();

    dropSerdes(serdeIdList);
    Deadline.checkTimeout();

    dropDanglingColumnDescriptors(columnDescriptorIdList);
  }

  /**
   * Drops SD-s. Should be called with the list short enough to not trip up Oracle/etc.
   * @param storageDescriptorIdList The storage descriptor identifiers to drop
   * @throws MetaException If there is an SQL exception during the execution it converted to
   * MetaException
   */
  private void dropStorageDescriptors(List<Object> storageDescriptorIdList) throws MetaException {
    if (storageDescriptorIdList.isEmpty()) {
      return;
    }
    String queryText;
    String sdIds = getIdListForIn(storageDescriptorIdList);

    // Get the corresponding SKEWED_STRING_LIST_ID data
    queryText =
        "select " + SKEWED_VALUES + ".\"STRING_LIST_ID_EID\" "
            + "from " + SKEWED_VALUES + " "
            + "WHERE " + SKEWED_VALUES + ".\"SD_ID_OID\" in  (" + sdIds + ")";
    List<Object> skewedStringListIdList = new ArrayList<>(0);

    try (QueryWrapper query = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
      List<Object> sqlResult = executeWithArray(query.getInnerQuery(), null, queryText);

      if (!sqlResult.isEmpty()) {
        for (Object stringListId : sqlResult) {
          skewedStringListIdList.add(MetastoreDirectSqlUtils.extractSqlLong(stringListId));
        }
      }
    }

    String skewedStringListIds = getIdListForIn(skewedStringListIdList);

    try {
      // Drop the SD params
      queryText = "delete from " + SD_PARAMS + " where \"SD_ID\" in (" + sdIds + ")";
      executeNoResult(queryText);
      Deadline.checkTimeout();

      // Drop the sort cols
      queryText = "delete from " + SORT_COLS + " where \"SD_ID\" in (" + sdIds + ")";
      executeNoResult(queryText);
      Deadline.checkTimeout();

      // Drop the bucketing cols
      queryText = "delete from " + BUCKETING_COLS + " where \"SD_ID\" in (" + sdIds + ")";
      executeNoResult(queryText);
      Deadline.checkTimeout();

      // Drop the skewed string lists
      if (skewedStringListIdList.size() > 0) {
        // Drop the skewed string value loc map
        queryText = "delete from " + SKEWED_COL_VALUE_LOC_MAP + " where \"SD_ID\" in ("
            + sdIds + ")";
        executeNoResult(queryText);
        Deadline.checkTimeout();

        // Drop the skewed values
        queryText = "delete from " + SKEWED_VALUES + " where \"SD_ID_OID\" in (" + sdIds + ")";
        executeNoResult(queryText);
        Deadline.checkTimeout();

        // Drop the skewed string list values
        queryText = "delete from " + SKEWED_STRING_LIST_VALUES + " where \"STRING_LIST_ID\" in ("
            + skewedStringListIds + ")";
        executeNoResult(queryText);
        Deadline.checkTimeout();

        // Drop the skewed string list
        queryText = "delete from " + SKEWED_STRING_LIST + " where \"STRING_LIST_ID\" in ("
            + skewedStringListIds + ")";
        executeNoResult(queryText);
        Deadline.checkTimeout();
      }

      // Drop the skewed cols
      queryText = "delete from " + SKEWED_COL_NAMES + " where \"SD_ID\" in (" + sdIds + ")";
      executeNoResult(queryText);
      Deadline.checkTimeout();

      // Drop the sds
      queryText = "delete from " + SDS + " where \"SD_ID\" in (" + sdIds + ")";
      executeNoResult(queryText);
    } catch (SQLException sqlException) {
      LOG.warn("SQL error executing query while dropping storage descriptor.", sqlException);
      throw new MetaException("Encountered error while dropping storage descriptor.");
    }
  }

  /**
   * Drops Serde-s. Should be called with the list short enough to not trip up Oracle/etc.
   * @param serdeIdList The serde identifiers to drop
   * @throws MetaException If there is an SQL exception during the execution it converted to
   * MetaException
   */
  private void dropSerdes(List<Object> serdeIdList) throws MetaException {
    String queryText;
    if (serdeIdList.isEmpty()) {
      return;
    }
    String serdeIds = getIdListForIn(serdeIdList);

    try {
      // Drop the serde params
      queryText = "delete from " + SERDE_PARAMS + " where \"SERDE_ID\" in (" + serdeIds + ")";
      executeNoResult(queryText);
      Deadline.checkTimeout();

      // Drop the serdes
      queryText = "delete from " + SERDES + " where \"SERDE_ID\" in (" + serdeIds + ")";
      executeNoResult(queryText);
    } catch (SQLException sqlException) {
      LOG.warn("SQL error executing query while dropping serde.", sqlException);
      throw new MetaException("Encountered error while dropping serde.");
    }
  }

  /**
   * Checks if the column descriptors still has references for other SD-s. If not, then removes
   * them. Should be called with the list short enough to not trip up Oracle/etc.
   * @param columnDescriptorIdList The column identifiers
   * @throws MetaException If there is an SQL exception during the execution it converted to
   * MetaException
   */
  private void dropDanglingColumnDescriptors(List<Long> columnDescriptorIdList)
      throws MetaException {
    if (columnDescriptorIdList.isEmpty()) {
      return;
    }
    String queryText;
    String colIds = getIdListForIn(columnDescriptorIdList);

    // Drop column descriptor, if no relation left
    queryText =
        "SELECT " + SDS + ".\"CD_ID\" "
            + "from " + SDS + " "
            + "WHERE " + SDS + ".\"CD_ID\" in (" + colIds + ") "
            + "GROUP BY " + SDS + ".\"CD_ID\"";
    Set<Long> danglingColumnDescriptorIdSet = new HashSet<>(columnDescriptorIdList);
    try (QueryWrapper query = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
      List<Object> sqlResult = executeWithArray(query.getInnerQuery(), null, queryText);

      if (!sqlResult.isEmpty()) {
        for (Object cdId : sqlResult) {
          // the returned CD is not dangling, so remove it from the list
          danglingColumnDescriptorIdSet.remove(MetastoreDirectSqlUtils.extractSqlLong(cdId));
        }
      }
    }
    if (!danglingColumnDescriptorIdSet.isEmpty()) {
      try {
        String danglingCDIds = getIdListForIn(danglingColumnDescriptorIdSet);

        // Drop the columns_v2
        queryText = "delete from " + COLUMNS_V2 + " where \"CD_ID\" in (" + danglingCDIds + ")";
        executeNoResult(queryText);
        Deadline.checkTimeout();

        // Drop the cols
        queryText = "delete from " + CDS + " where \"CD_ID\" in (" + danglingCDIds + ")";
        executeNoResult(queryText);
      } catch (SQLException sqlException) {
        LOG.warn("SQL error executing query while dropping dangling col descriptions", sqlException);
        throw new MetaException("Encountered error while dropping col descriptions");
      }
    }
  }

  public final static Object[] STATS_TABLE_TYPES = new Object[] {
    TableType.MANAGED_TABLE.toString(), TableType.MATERIALIZED_VIEW.toString()
  };

  public List<org.apache.hadoop.hive.common.TableName> getTableNamesWithStats() throws MetaException {
    // Could we also join with ACID tables to only get tables with outdated stats?
    String queryText0 = "SELECT DISTINCT " + TBLS + ".\"TBL_NAME\", " + DBS + ".\"NAME\", "
        + DBS + ".\"CTLG_NAME\" FROM " + TBLS + " INNER JOIN " + DBS + " ON "
        + TBLS + ".\"DB_ID\" = " + DBS + ".\"DB_ID\"";
    String queryText1 = " WHERE " + TBLS + ".\"TBL_TYPE\" IN ("
        + makeParams(STATS_TABLE_TYPES.length) + ")";

    List<org.apache.hadoop.hive.common.TableName> result = new ArrayList<>();

    String queryText = queryText0 + " INNER JOIN " + TAB_COL_STATS
        + " ON " + TBLS + ".\"TBL_ID\" = " + TAB_COL_STATS + ".\"TBL_ID\"" + queryText1;
    getStatsTableListResult(queryText, result);

    queryText = queryText0 + " INNER JOIN " + PARTITIONS + " ON " + TBLS + ".\"TBL_ID\" = "
        + PARTITIONS + ".\"TBL_ID\"" + " INNER JOIN " + PART_COL_STATS + " ON " + PARTITIONS
        + ".\"PART_ID\" = " + PART_COL_STATS + ".\"PART_ID\"" + queryText1;
    getStatsTableListResult(queryText, result);

    return result;
  }

  public Map<String, List<String>> getColAndPartNamesWithStats(
      String catName, String dbName, String tableName) throws MetaException {
    // Could we also join with ACID tables to only get tables with outdated stats?
    String queryText = "SELECT DISTINCT " + PARTITIONS + ".\"PART_NAME\", " + PART_COL_STATS
        + ".\"COLUMN_NAME\" FROM " + TBLS + " INNER JOIN " + DBS + " ON " + TBLS + ".\"DB_ID\" = "
        + DBS + ".\"DB_ID\" INNER JOIN " + PARTITIONS + " ON " + TBLS + ".\"TBL_ID\" = "
        + PARTITIONS + ".\"TBL_ID\"  INNER JOIN " + PART_COL_STATS + " ON " + PARTITIONS
        + ".\"PART_ID\" = " + PART_COL_STATS + ".\"PART_ID\" WHERE " + DBS + ".\"NAME\" = ? AND "
        + DBS + ".\"CTLG_NAME\" = ? AND " + TBLS + ".\"TBL_NAME\" = ? ORDER BY "
        + PARTITIONS + ".\"PART_NAME\"";

    LOG.debug("Running {}", queryText);
    try (QueryWrapper query = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
      List<Object[]> sqlResult = MetastoreDirectSqlUtils.ensureList(executeWithArray(
          query.getInnerQuery(), new Object[] { dbName, catName, tableName }, queryText));
      Map<String, List<String>> result = new HashMap<>();
      String lastPartName = null;
      List<String> cols = null;
      for (Object[] line : sqlResult) {
        String col = MetastoreDirectSqlUtils.extractSqlString(line[1]);
        String part = MetastoreDirectSqlUtils.extractSqlString(line[0]);
        if (!part.equals(lastPartName)) {
          if (lastPartName != null) {
            result.put(lastPartName, cols);
          }
          cols = cols == null ? new ArrayList<>() : new ArrayList<>(cols.size());
          lastPartName = part;
        }
        cols.add(col);
      }
      if (lastPartName != null) {
        result.put(lastPartName, cols);
      }
      return result;
    }
  }

  public List<org.apache.hadoop.hive.common.TableName> getAllTableNamesForStats() throws MetaException {
    String queryText = "SELECT " + TBLS + ".\"TBL_NAME\", " + DBS + ".\"NAME\", "
        + DBS + ".\"CTLG_NAME\" FROM " + TBLS + " INNER JOIN " + DBS + " ON " + TBLS
        + ".\"DB_ID\" = " + DBS + ".\"DB_ID\""
        + " WHERE " + TBLS + ".\"TBL_TYPE\" IN (" + makeParams(STATS_TABLE_TYPES.length) + ")";
    List<org.apache.hadoop.hive.common.TableName> result = new ArrayList<>();
    getStatsTableListResult(queryText, result);
    return result;
  }

  private void getStatsTableListResult(
      String queryText, List<org.apache.hadoop.hive.common.TableName> result) throws MetaException {
    LOG.debug("Running {}", queryText);
    try (QueryWrapper query = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
      List<Object[]> sqlResult = MetastoreDirectSqlUtils
          .ensureList(executeWithArray(query.getInnerQuery(), STATS_TABLE_TYPES, queryText));
      for (Object[] line : sqlResult) {
        result.add(new org.apache.hadoop.hive.common.TableName(
            MetastoreDirectSqlUtils.extractSqlString(line[2]), MetastoreDirectSqlUtils
            .extractSqlString(line[1]), MetastoreDirectSqlUtils.extractSqlString(line[0])));
      }
    }
  }

  public void lockDbTable(String tableName) throws MetaException {
    String lockCommand = "lock table \"" + tableName + "\" in exclusive mode";
    try {
      executeNoResult(lockCommand);
    } catch (SQLException sqle) {
      throw new MetaException("Error while locking table " + tableName + ": " + sqle.getMessage());
    }
  }

  public void deleteColumnStatsState(long tbl_id) throws MetaException {
    String queryText;
    switch (dbType.dbType) {
      case MYSQL:
        // @formatter:off
        queryText = ""
            + "delete pp from " + PARTITION_PARAMS + " pp, " + PARTITIONS + " p"
            + " where"
            + "   p.\"PART_ID\" = pp.\"PART_ID\" AND"
            + "   p.\"TBL_ID\" = " + tbl_id
            + "  and \"PARAM_KEY\" = '"+StatsSetupConst.COLUMN_STATS_ACCURATE + "'";
        // @formatter:on
        break;
      default:
        // @formatter:off
        queryText = ""
            + "delete from " + PARTITION_PARAMS
            + " where"
            + "   \"PART_ID\" in (select p.\"PART_ID\"  from " + PARTITIONS + " p where"
            + "   p.\"TBL_ID\" =  " + tbl_id + ")"
            + "  and \"PARAM_KEY\" = '"+StatsSetupConst.COLUMN_STATS_ACCURATE + "'";
        // @formatter:on
    }

    try {
      executeNoResult(queryText);
    } catch (SQLException e) {
      throw new MetaException("Error removing column stat states:" + e.getMessage());
    }
  }

  public Map<String, Map<String, String>> updatePartitionColumnStatisticsBatch(
                                                      Map<String, ColumnStatistics> partColStatsMap,
                                                      Table tbl,
                                                      List<TransactionalMetaStoreEventListener> listeners,
                                                      String validWriteIds, long writeId)
          throws MetaException {
    long numStats = 0;
    for (Map.Entry entry : partColStatsMap.entrySet()) {
      ColumnStatistics colStats = (ColumnStatistics) entry.getValue();
      numStats += colStats.getStatsObjSize();
    }
    long csId = directSqlUpdatePart.getNextCSIdForMPartitionColumnStatistics(numStats);
    return directSqlUpdatePart.updatePartitionColumnStatistics(partColStatsMap, tbl, csId, validWriteIds, writeId, listeners);
  }

  public List<Function> getFunctions(String catName) throws MetaException {
    List<Long> funcIds = getFunctionIds(catName);
    if (funcIds.isEmpty()) {
      return Collections.emptyList(); // no functions, bail early.
    }
    // Get full objects. For Oracle/etc. do it in batches.
    return Batchable.runBatched(batchSize, funcIds, new Batchable<Long, Function>() {
      @Override
      public List<Function> run(List<Long> input) throws MetaException {
        return getFunctionsFromFunctionIds(input, catName);
      }
    });
  }

  private List<Function> getFunctionsFromFunctionIds(List<Long> funcIdList, String catName) throws MetaException {
    String funcIds = getIdListForIn(funcIdList);
    final int funcIdIndex = 0;
    final int funcNameIndex = 1;
    final int dbNameIndex = 2;
    final int funcClassNameIndex = 3;
    final int funcOwnerNameIndex = 4;
    final int funcOwnerTypeIndex = 5;
    final int funcCreateTimeIndex = 6;
    final int funcTypeIndex = 7;

    String queryText = "SELECT " +
        FUNCS + ".\"FUNC_ID\", " +
        FUNCS + ".\"FUNC_NAME\", " +
        DBS + ".\"NAME\", " +
        FUNCS + ".\"CLASS_NAME\", " +
        FUNCS + ".\"OWNER_NAME\", " +
        FUNCS + ".\"OWNER_TYPE\", " +
        FUNCS + ".\"CREATE_TIME\", " +
        FUNCS + ".\"FUNC_TYPE\"" +
        " FROM " + FUNCS +
        " LEFT JOIN " + DBS + " ON " + FUNCS + ".\"DB_ID\" = " + DBS + ".\"DB_ID\"" +
        " where " + FUNCS +".\"FUNC_ID\" in (" + funcIds + ") order by " + FUNCS + ".\"FUNC_NAME\" asc";

    List<Function> results = new ArrayList<>();
    TreeMap<Long, Function> funcs = new TreeMap<>();
    final boolean doTrace = LOG.isDebugEnabled();
    long start = doTrace ? System.nanoTime() : 0;
    try (QueryWrapper query = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
      List<Object[]> queryResult = executeWithArray(query.getInnerQuery(), null, queryText);
      long end = doTrace ? System.nanoTime() : 0;
      MetastoreDirectSqlUtils.timingTrace(doTrace, queryText, start, end);

      for (Object[] function : queryResult) {
        Long funcId = MetastoreDirectSqlUtils.extractSqlLong(function[funcIdIndex]);
        String funcName = MetastoreDirectSqlUtils.extractSqlString(function[funcNameIndex]);
        String dbName = MetastoreDirectSqlUtils.extractSqlString(function[dbNameIndex]);
        String funcClassName = MetastoreDirectSqlUtils.extractSqlString(function[funcClassNameIndex]);
        String funcOwnerName = MetastoreDirectSqlUtils.extractSqlString(function[funcOwnerNameIndex]);
        String funcOwnerType = MetastoreDirectSqlUtils.extractSqlString(function[funcOwnerTypeIndex]);
        int funcCreateTime = MetastoreDirectSqlUtils.extractSqlInt(function[funcCreateTimeIndex]);
        int funcType = MetastoreDirectSqlUtils.extractSqlInt(function[funcTypeIndex]);

        Function func = new Function();
        func.setFunctionName(funcName);
        func.setDbName(dbName);
        func.setCatName(catName);
        func.setClassName(funcClassName);
        func.setOwnerName(funcOwnerName);
        func.setOwnerType(PrincipalType.valueOf(funcOwnerType));
        func.setCreateTime(funcCreateTime);
        func.setFunctionType(FunctionType.findByValue(funcType));
        func.setResourceUris(new ArrayList<>());

        results.add(func);
        funcs.put(funcId, func);
      }
    }
    MetastoreDirectSqlUtils.setFunctionResourceUris(FUNC_RU, pm, funcIds, funcs);
    return results;
  }

  private List<Long> getFunctionIds(String catName) throws MetaException {
    boolean doTrace = LOG.isDebugEnabled();

    String queryText = "select " + FUNCS + ".\"FUNC_ID\" from " + FUNCS +
        " LEFT JOIN " + DBS + " ON " + FUNCS + ".\"DB_ID\" = " + DBS + ".\"DB_ID\"" +
        " where " + DBS + ".\"CTLG_NAME\" = ? ";

    long start = doTrace ? System.nanoTime() : 0;
    Object[] params = new Object[1];
    params[0] = catName;
    try (QueryWrapper query = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
      List<Object> sqlResult = executeWithArray(query.getInnerQuery(), params, queryText);
      long queryTime = doTrace ? System.nanoTime() : 0;
      MetastoreDirectSqlUtils.timingTrace(doTrace, queryText, start, queryTime);
      final List<Long> result;
      if (sqlResult.isEmpty()) {
        result = Collections.emptyList();
      } else {
        result = new ArrayList<>(sqlResult.size());
        for (Object fields : sqlResult) {
          result.add(MetastoreDirectSqlUtils.extractSqlLong(fields));
        }
      }
      return result;
    }
  }
}
