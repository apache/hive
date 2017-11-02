/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.utils;

import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.columnstats.aggr.ColumnStatsAggregator;
import org.apache.hadoop.hive.metastore.columnstats.aggr.ColumnStatsAggregatorFactory;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MetaStoreUtils {
  /** A fixed date format to be used for hive partition column values. */
  public static final ThreadLocal<DateFormat> PARTITION_DATE_FORMAT =
       new ThreadLocal<DateFormat>() {
    @Override
    protected DateFormat initialValue() {
      DateFormat val = new SimpleDateFormat("yyyy-MM-dd");
      val.setLenient(false); // Without this, 2020-20-20 becomes 2021-08-20.
      return val;
    }
  };
  // Indicates a type was derived from the deserializer rather than Hive's metadata.
  public static final String TYPE_FROM_DESERIALIZER = "<derived from deserializer>";

  private static final Charset ENCODING = StandardCharsets.UTF_8;
  private static final Logger LOG = LoggerFactory.getLogger(MetaStoreUtils.class);

  // Right now we only support one special character '/'.
  // More special characters can be added accordingly in the future.
  // NOTE:
  // If the following array is updated, please also be sure to update the
  // configuration parameter documentation
  // HIVE_SUPPORT_SPECICAL_CHARACTERS_IN_TABLE_NAMES in HiveConf as well.
  private static final char[] specialCharactersInTableNames = new char[] { '/' };

  /**
   * Catches exceptions that can't be handled and bundles them to MetaException
   *
   * @param e exception to wrap.
   * @throws MetaException wrapper for the exception
   */
  public static void logAndThrowMetaException(Exception e) throws MetaException {
    String exInfo = "Got exception: " + e.getClass().getName() + " "
        + e.getMessage();
    LOG.error(exInfo, e);
    LOG.error("Converting exception to MetaException");
    throw new MetaException(exInfo);
  }

  public static String encodeTableName(String name) {
    // The encoding method is simple, e.g., replace
    // all the special characters with the corresponding number in ASCII.
    // Note that unicode is not supported in table names. And we have explicit
    // checks for it.
    StringBuilder sb = new StringBuilder();
    for (char ch : name.toCharArray()) {
      if (Character.isLetterOrDigit(ch) || ch == '_') {
        sb.append(ch);
      } else {
        sb.append('-').append((int) ch).append('-');
      }
    }
    return sb.toString();
  }

  /**
   * convert Exception to MetaException, which sets the cause to such exception
   * @param e cause of the exception
   * @return  the MetaException with the specified exception as the cause
   */
  public static MetaException newMetaException(Exception e) {
    return newMetaException(e != null ? e.getMessage() : null, e);
  }

  /**
   * convert Exception to MetaException, which sets the cause to such exception
   * @param errorMessage  the error message for this MetaException
   * @param e             cause of the exception
   * @return  the MetaException with the specified exception as the cause
   */
  public static MetaException newMetaException(String errorMessage, Exception e) {
    MetaException metaException = new MetaException(errorMessage);
    if (e != null) {
      metaException.initCause(e);
    }
    return metaException;
  }

  /**
   * Helper function to transform Nulls to empty strings.
   */
  private static final com.google.common.base.Function<String,String> transFormNullsToEmptyString
      = new com.google.common.base.Function<String, String>() {
    @Override
    public java.lang.String apply(@Nullable java.lang.String string) {
      return org.apache.commons.lang.StringUtils.defaultString(string);
    }
  };

  /**
   * We have a need to sanity-check the map before conversion from persisted objects to
   * metadata thrift objects because null values in maps will cause a NPE if we send
   * across thrift. Pruning is appropriate for most cases except for databases such as
   * Oracle where Empty strings are stored as nulls, in which case we need to handle that.
   * See HIVE-8485 for motivations for this.
   */
  public static Map<String,String> trimMapNulls(
      Map<String,String> dnMap, boolean retrieveMapNullsAsEmptyStrings){
    if (dnMap == null){
      return null;
    }
    // Must be deterministic order map - see HIVE-8707
    //   => we use Maps.newLinkedHashMap instead of Maps.newHashMap
    if (retrieveMapNullsAsEmptyStrings) {
      // convert any nulls present in map values to empty strings - this is done in the case
      // of backing dbs like oracle which persist empty strings as nulls.
      return Maps.newLinkedHashMap(Maps.transformValues(dnMap, transFormNullsToEmptyString));
    } else {
      // prune any nulls present in map values - this is the typical case.
      return Maps.newLinkedHashMap(Maps.filterValues(dnMap, Predicates.notNull()));
    }
  }


  // given a list of partStats, this function will give you an aggr stats
  public static List<ColumnStatisticsObj> aggrPartitionStats(List<ColumnStatistics> partStats,
                                                             String dbName, String tableName, List<String> partNames, List<String> colNames,
                                                             boolean useDensityFunctionForNDVEstimation, double ndvTuner)
      throws MetaException {
    // 1. group by the stats by colNames
    // map the colName to List<ColumnStatistics>
    Map<String, List<ColumnStatistics>> map = new HashMap<>();
    for (ColumnStatistics css : partStats) {
      List<ColumnStatisticsObj> objs = css.getStatsObj();
      for (ColumnStatisticsObj obj : objs) {
        List<ColumnStatisticsObj> singleObj = new ArrayList<>();
        singleObj.add(obj);
        ColumnStatistics singleCS = new ColumnStatistics(css.getStatsDesc(), singleObj);
        if (!map.containsKey(obj.getColName())) {
          map.put(obj.getColName(), new ArrayList<ColumnStatistics>());
        }
        map.get(obj.getColName()).add(singleCS);
      }
    }
    return MetaStoreUtils.aggrPartitionStats(map,dbName,tableName,partNames,colNames,useDensityFunctionForNDVEstimation, ndvTuner);
  }

  public static List<ColumnStatisticsObj> aggrPartitionStats(
      Map<String, List<ColumnStatistics>> map, String dbName, String tableName,
      final List<String> partNames, List<String> colNames,
      final boolean useDensityFunctionForNDVEstimation,final double ndvTuner) throws MetaException {
    List<ColumnStatisticsObj> colStats = new ArrayList<>();
    // 2. Aggregate stats for each column in a separate thread
    if (map.size()< 1) {
      //stats are absent in RDBMS
      LOG.debug("No stats data found for: dbName=" +dbName +" tblName=" + tableName +
          " partNames= " + partNames + " colNames=" + colNames );
      return colStats;
    }
    final ExecutorService pool = Executors.newFixedThreadPool(Math.min(map.size(), 16),
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("aggr-col-stats-%d").build());
    final List<Future<ColumnStatisticsObj>> futures = Lists.newLinkedList();

    long start = System.currentTimeMillis();
    for (final Map.Entry<String, List<ColumnStatistics>> entry : map.entrySet()) {
      futures.add(pool.submit(new Callable<ColumnStatisticsObj>() {
        @Override
        public ColumnStatisticsObj call() throws Exception {
          List<ColumnStatistics> css = entry.getValue();
          ColumnStatsAggregator aggregator = ColumnStatsAggregatorFactory.getColumnStatsAggregator(css
                  .iterator().next().getStatsObj().iterator().next().getStatsData().getSetField(),
              useDensityFunctionForNDVEstimation, ndvTuner);
          ColumnStatisticsObj statsObj = aggregator.aggregate(entry.getKey(), partNames, css);
          return statsObj;
        }}));
    }
    pool.shutdown();
    for (Future<ColumnStatisticsObj> future : futures) {
      try {
        colStats.add(future.get());
      } catch (InterruptedException | ExecutionException e) {
        pool.shutdownNow();
        LOG.debug(e.toString());
        throw new MetaException(e.toString());
      }
    }
    LOG.debug("Time for aggr col stats in seconds: {} Threads used: {}",
        ((System.currentTimeMillis() - (double)start))/1000, Math.min(map.size(), 16));
    return colStats;
  }

  public static double decimalToDouble(Decimal decimal) {
    return new BigDecimal(new BigInteger(decimal.getUnscaled()), decimal.getScale()).doubleValue();
  }

  public static String[] getQualifiedName(String defaultDbName, String tableName) {
    String[] names = tableName.split("\\.");
    if (names.length == 1) {
      return new String[] { defaultDbName, tableName};
    }
    return names;
  }

  public static void validatePartitionNameCharacters(List<String> partVals,
                                                     Pattern partitionValidationPattern) throws MetaException {

    String invalidPartitionVal = getPartitionValWithInvalidCharacter(partVals, partitionValidationPattern);
    if (invalidPartitionVal != null) {
      throw new MetaException("Partition value '" + invalidPartitionVal +
          "' contains a character " + "not matched by whitelist pattern '" +
          partitionValidationPattern.toString() + "'.  " + "(configure with " +
          MetastoreConf.ConfVars.PARTITION_NAME_WHITELIST_PATTERN.getVarname() + ")");
    }
  }

  public static String getPartitionValWithInvalidCharacter(List<String> partVals,
                                                           Pattern partitionValidationPattern) {
    if (partitionValidationPattern == null) {
      return null;
    }

    for (String partVal : partVals) {
      if (!partitionValidationPattern.matcher(partVal).matches()) {
        return partVal;
      }
    }

    return null;
  }

  /**
   * Produce a hash for the storage descriptor
   * @param sd storage descriptor to hash
   * @param md message descriptor to use to generate the hash
   * @return the hash as a byte array
   */
  public static byte[] hashStorageDescriptor(StorageDescriptor sd, MessageDigest md)  {
    // Note all maps and lists have to be absolutely sorted.  Otherwise we'll produce different
    // results for hashes based on the OS or JVM being used.
    md.reset();
    for (FieldSchema fs : sd.getCols()) {
      md.update(fs.getName().getBytes(ENCODING));
      md.update(fs.getType().getBytes(ENCODING));
      if (fs.getComment() != null) md.update(fs.getComment().getBytes(ENCODING));
    }
    if (sd.getInputFormat() != null) {
      md.update(sd.getInputFormat().getBytes(ENCODING));
    }
    if (sd.getOutputFormat() != null) {
      md.update(sd.getOutputFormat().getBytes(ENCODING));
    }
    md.update(sd.isCompressed() ? "true".getBytes(ENCODING) : "false".getBytes(ENCODING));
    md.update(Integer.toString(sd.getNumBuckets()).getBytes(ENCODING));
    if (sd.getSerdeInfo() != null) {
      SerDeInfo serde = sd.getSerdeInfo();
      if (serde.getName() != null) {
        md.update(serde.getName().getBytes(ENCODING));
      }
      if (serde.getSerializationLib() != null) {
        md.update(serde.getSerializationLib().getBytes(ENCODING));
      }
      if (serde.getParameters() != null) {
        SortedMap<String, String> params = new TreeMap<>(serde.getParameters());
        for (Map.Entry<String, String> param : params.entrySet()) {
          md.update(param.getKey().getBytes(ENCODING));
          md.update(param.getValue().getBytes(ENCODING));
        }
      }
    }
    if (sd.getBucketCols() != null) {
      List<String> bucketCols = new ArrayList<>(sd.getBucketCols());
      for (String bucket : bucketCols) md.update(bucket.getBytes(ENCODING));
    }
    if (sd.getSortCols() != null) {
      SortedSet<Order> orders = new TreeSet<>(sd.getSortCols());
      for (Order order : orders) {
        md.update(order.getCol().getBytes(ENCODING));
        md.update(Integer.toString(order.getOrder()).getBytes(ENCODING));
      }
    }
    if (sd.getSkewedInfo() != null) {
      SkewedInfo skewed = sd.getSkewedInfo();
      if (skewed.getSkewedColNames() != null) {
        SortedSet<String> colnames = new TreeSet<>(skewed.getSkewedColNames());
        for (String colname : colnames) md.update(colname.getBytes(ENCODING));
      }
      if (skewed.getSkewedColValues() != null) {
        SortedSet<String> sortedOuterList = new TreeSet<>();
        for (List<String> innerList : skewed.getSkewedColValues()) {
          SortedSet<String> sortedInnerList = new TreeSet<>(innerList);
          sortedOuterList.add(org.apache.commons.lang.StringUtils.join(sortedInnerList, "."));
        }
        for (String colval : sortedOuterList) md.update(colval.getBytes(ENCODING));
      }
      if (skewed.getSkewedColValueLocationMaps() != null) {
        SortedMap<String, String> sortedMap = new TreeMap<>();
        for (Map.Entry<List<String>, String> smap : skewed.getSkewedColValueLocationMaps().entrySet()) {
          SortedSet<String> sortedKey = new TreeSet<>(smap.getKey());
          sortedMap.put(org.apache.commons.lang.StringUtils.join(sortedKey, "."), smap.getValue());
        }
        for (Map.Entry<String, String> e : sortedMap.entrySet()) {
          md.update(e.getKey().getBytes(ENCODING));
          md.update(e.getValue().getBytes(ENCODING));
        }
      }
      md.update(sd.isStoredAsSubDirectories() ? "true".getBytes(ENCODING) : "false".getBytes(ENCODING));
    }

    return md.digest();
  }

  public static List<String> getColumnNamesForTable(Table table) {
    List<String> colNames = new ArrayList<>();
    Iterator<FieldSchema> colsIterator = table.getSd().getColsIterator();
    while (colsIterator.hasNext()) {
      colNames.add(colsIterator.next().getName());
    }
    return colNames;
  }

  /**
   * validateName
   *
   * Checks the name conforms to our standars which are: "[a-zA-z_0-9]+". checks
   * this is just characters and numbers and _
   *
   * @param name
   *          the name to validate
   * @param conf
   *          hive configuration
   * @return true or false depending on conformance
   *              if it doesn't match the pattern.
   */
  public static boolean validateName(String name, Configuration conf) {
    Pattern tpat = null;
    String allowedCharacters = "\\w_";
    if (conf != null
        && MetastoreConf.getBoolVar(conf,
        MetastoreConf.ConfVars.SUPPORT_SPECICAL_CHARACTERS_IN_TABLE_NAMES)) {
      for (Character c : specialCharactersInTableNames) {
        allowedCharacters += c;
      }
    }
    tpat = Pattern.compile("[" + allowedCharacters + "]+");
    Matcher m = tpat.matcher(name);
    return m.matches();
  }

  /*
   * At the Metadata level there are no restrictions on Column Names.
   */
  public static boolean validateColumnName(String name) {
    return true;
  }

  static public String validateTblColumns(List<FieldSchema> cols) {
    for (FieldSchema fieldSchema : cols) {
      // skip this, as validateColumnName always returns true
      /*
      if (!validateColumnName(fieldSchema.getName())) {
        return "name: " + fieldSchema.getName();
      }
      */
      String typeError = validateColumnType(fieldSchema.getType());
      if (typeError != null) {
        return typeError;
      }
    }
    return null;
  }

  private static String validateColumnType(String type) {
    if (type.equals(TYPE_FROM_DESERIALIZER)) return null;
    int last = 0;
    boolean lastAlphaDigit = isValidTypeChar(type.charAt(last));
    for (int i = 1; i <= type.length(); i++) {
      if (i == type.length()
          || isValidTypeChar(type.charAt(i)) != lastAlphaDigit) {
        String token = type.substring(last, i);
        last = i;
        if (!ColumnType.AllTypes.contains(token)) {
          return "type: " + type;
        }
        break;
      }
    }
    return null;
  }

  private static boolean isValidTypeChar(char c) {
    return Character.isLetterOrDigit(c) || c == '_';
  }

  /**
   * Determines whether a table is an external table.
   *
   * @param table table of interest
   *
   * @return true if external
   */
  public static boolean isExternalTable(Table table) {
    if (table == null) {
      return false;
    }
    Map<String, String> params = table.getParameters();
    if (params == null) {
      return false;
    }

    return "TRUE".equalsIgnoreCase(params.get("EXTERNAL"));
  }

  // check if stats need to be (re)calculated
  public static boolean requireCalStats(Configuration hiveConf, Partition oldPart,
    Partition newPart, Table tbl, EnvironmentContext environmentContext) {

    if (environmentContext != null
        && environmentContext.isSetProperties()
        && StatsSetupConst.TRUE.equals(environmentContext.getProperties().get(
            StatsSetupConst.DO_NOT_UPDATE_STATS))) {
      return false;
    }

    if (MetaStoreUtils.isView(tbl)) {
      return false;
    }

    if  (oldPart == null && newPart == null) {
      return true;
    }

    // requires to calculate stats if new partition doesn't have it
    if ((newPart == null) || (newPart.getParameters() == null)
        || !containsAllFastStats(newPart.getParameters())) {
      return true;
    }

    if (environmentContext != null && environmentContext.isSetProperties()) {
      String statsType = environmentContext.getProperties().get(StatsSetupConst.STATS_GENERATED);
      // no matter STATS_GENERATED is USER or TASK, all need to re-calculate the stats:
      // USER: alter table .. update statistics
      // TASK: from some sql operation which could collect and compute stats
      if (StatsSetupConst.TASK.equals(statsType) || StatsSetupConst.USER.equals(statsType)) {
        return true;
      }
    }

    // requires to calculate stats if new and old have different fast stats
    return !isFastStatsSame(oldPart, newPart);
  }

  public static boolean isView(Table table) {
    if (table == null) {
      return false;
    }
    return TableType.VIRTUAL_VIEW.toString().equals(table.getTableType());
  }

  /**
   * @param partParams
   * @return True if the passed Parameters Map contains values for all "Fast Stats".
   */
  private static boolean containsAllFastStats(Map<String, String> partParams) {
    for (String stat : StatsSetupConst.fastStats) {
      if (!partParams.containsKey(stat)) {
        return false;
      }
    }
    return true;
  }

  public static boolean isFastStatsSame(Partition oldPart, Partition newPart) {
    // requires to calculate stats if new and old have different fast stats
    if ((oldPart != null) && (oldPart.getParameters() != null)) {
      for (String stat : StatsSetupConst.fastStats) {
        if (oldPart.getParameters().containsKey(stat)) {
          Long oldStat = Long.parseLong(oldPart.getParameters().get(stat));
          Long newStat = Long.parseLong(newPart.getParameters().get(stat));
          if (!oldStat.equals(newStat)) {
            return false;
          }
        } else {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  public static boolean updateTableStatsFast(Database db, Table tbl, Warehouse wh,
                                             boolean madeDir, EnvironmentContext environmentContext) throws MetaException {
    return updateTableStatsFast(db, tbl, wh, madeDir, false, environmentContext);
  }

  public static boolean updateTableStatsFast(Database db, Table tbl, Warehouse wh,
                                             boolean madeDir, boolean forceRecompute, EnvironmentContext environmentContext) throws MetaException {
    if (tbl.getPartitionKeysSize() == 0) {
      // Update stats only when unpartitioned
      FileStatus[] fileStatuses = wh.getFileStatusesForUnpartitionedTable(db, tbl);
      return updateTableStatsFast(tbl, fileStatuses, madeDir, forceRecompute, environmentContext);
    } else {
      return false;
    }
  }

  /**
   * Updates the numFiles and totalSize parameters for the passed Table by querying
   * the warehouse if the passed Table does not already have values for these parameters.
   * @param tbl
   * @param fileStatus
   * @param newDir if true, the directory was just created and can be assumed to be empty
   * @param forceRecompute Recompute stats even if the passed Table already has
   * these parameters set
   * @return true if the stats were updated, false otherwise
   */
  public static boolean updateTableStatsFast(Table tbl, FileStatus[] fileStatus, boolean newDir,
                                             boolean forceRecompute, EnvironmentContext environmentContext) throws MetaException {

    Map<String,String> params = tbl.getParameters();

    if ((params!=null) && params.containsKey(StatsSetupConst.DO_NOT_UPDATE_STATS)){
      boolean doNotUpdateStats = Boolean.valueOf(params.get(StatsSetupConst.DO_NOT_UPDATE_STATS));
      params.remove(StatsSetupConst.DO_NOT_UPDATE_STATS);
      tbl.setParameters(params); // to make sure we remove this marker property
      if (doNotUpdateStats){
        return false;
      }
    }

    boolean updated = false;
    if (forceRecompute ||
        params == null ||
        !containsAllFastStats(params)) {
      if (params == null) {
        params = new HashMap<String,String>();
      }
      if (!newDir) {
        // The table location already exists and may contain data.
        // Let's try to populate those stats that don't require full scan.
        LOG.info("Updating table stats fast for " + tbl.getTableName());
        populateQuickStats(fileStatus, params);
        LOG.info("Updated size of table " + tbl.getTableName() +" to "+ params.get(StatsSetupConst.TOTAL_SIZE));
        if (environmentContext != null
            && environmentContext.isSetProperties()
            && StatsSetupConst.TASK.equals(environmentContext.getProperties().get(
            StatsSetupConst.STATS_GENERATED))) {
          StatsSetupConst.setBasicStatsState(params, StatsSetupConst.TRUE);
        } else {
          StatsSetupConst.setBasicStatsState(params, StatsSetupConst.FALSE);
        }
      }
      tbl.setParameters(params);
      updated = true;
    }
    return updated;
  }

  public static void populateQuickStats(FileStatus[] fileStatus, Map<String, String> params) {
    int numFiles = 0;
    long tableSize = 0L;
    for (FileStatus status : fileStatus) {
      // don't take directories into account for quick stats
      if (!status.isDir()) {
        tableSize += status.getLen();
        numFiles += 1;
      }
    }
    params.put(StatsSetupConst.NUM_FILES, Integer.toString(numFiles));
    params.put(StatsSetupConst.TOTAL_SIZE, Long.toString(tableSize));
  }

  public static boolean areSameColumns(List<FieldSchema> oldCols, List<FieldSchema> newCols) {
    return ListUtils.isEqualList(oldCols, newCols);
  }

  public static void updateBasicState(EnvironmentContext environmentContext, Map<String,String>
      params) {
    if (params == null) {
      return;
    }
    if (environmentContext != null
        && environmentContext.isSetProperties()
        && StatsSetupConst.TASK.equals(environmentContext.getProperties().get(
        StatsSetupConst.STATS_GENERATED))) {
      StatsSetupConst.setBasicStatsState(params, StatsSetupConst.TRUE);
    } else {
      StatsSetupConst.setBasicStatsState(params, StatsSetupConst.FALSE);
    }
  }

  public static boolean updatePartitionStatsFast(Partition part, Warehouse wh, EnvironmentContext environmentContext)
      throws MetaException {
    return updatePartitionStatsFast(part, wh, false, false, environmentContext);
  }

  public static boolean updatePartitionStatsFast(Partition part, Warehouse wh, boolean madeDir, EnvironmentContext environmentContext)
      throws MetaException {
    return updatePartitionStatsFast(part, wh, madeDir, false, environmentContext);
  }

  /**
   * Updates the numFiles and totalSize parameters for the passed Partition by querying
   *  the warehouse if the passed Partition does not already have values for these parameters.
   * @param part
   * @param wh
   * @param madeDir if true, the directory was just created and can be assumed to be empty
   * @param forceRecompute Recompute stats even if the passed Partition already has
   * these parameters set
   * @return true if the stats were updated, false otherwise
   */
  public static boolean updatePartitionStatsFast(Partition part, Warehouse wh,
                                                 boolean madeDir, boolean forceRecompute, EnvironmentContext environmentContext) throws MetaException {
    return updatePartitionStatsFast(new PartitionSpecProxy.SimplePartitionWrapperIterator(part),
        wh, madeDir, forceRecompute, environmentContext);
  }
  /**
   * Updates the numFiles and totalSize parameters for the passed Partition by querying
   *  the warehouse if the passed Partition does not already have values for these parameters.
   * @param part
   * @param wh
   * @param madeDir if true, the directory was just created and can be assumed to be empty
   * @param forceRecompute Recompute stats even if the passed Partition already has
   * these parameters set
   * @return true if the stats were updated, false otherwise
   */
  public static boolean updatePartitionStatsFast(PartitionSpecProxy.PartitionIterator part, Warehouse wh,
                                                 boolean madeDir, boolean forceRecompute, EnvironmentContext environmentContext) throws MetaException {
    Map<String,String> params = part.getParameters();
    boolean updated = false;
    if (forceRecompute ||
        params == null ||
        !containsAllFastStats(params)) {
      if (params == null) {
        params = new HashMap<String,String>();
      }
      if (!madeDir) {
        // The partition location already existed and may contain data. Lets try to
        // populate those statistics that don't require a full scan of the data.
        LOG.warn("Updating partition stats fast for: " + part.getTableName());
        FileStatus[] fileStatus = wh.getFileStatusesForLocation(part.getLocation());
        populateQuickStats(fileStatus, params);
        LOG.warn("Updated size to " + params.get(StatsSetupConst.TOTAL_SIZE));
        updateBasicState(environmentContext, params);
      }
      part.setParameters(params);
      updated = true;
    }
    return updated;
  }

  /*
     * This method is to check if the new column list includes all the old columns with same name and
     * type. The column comment does not count.
     */
  public static boolean columnsIncludedByNameType(List<FieldSchema> oldCols,
                                                  List<FieldSchema> newCols) {
    if (oldCols.size() > newCols.size()) {
      return false;
    }

    Map<String, String> columnNameTypePairMap = new HashMap<String, String>(newCols.size());
    for (FieldSchema newCol : newCols) {
      columnNameTypePairMap.put(newCol.getName().toLowerCase(), newCol.getType());
    }
    for (final FieldSchema oldCol : oldCols) {
      if (!columnNameTypePairMap.containsKey(oldCol.getName())
          || !columnNameTypePairMap.get(oldCol.getName()).equalsIgnoreCase(oldCol.getType())) {
        return false;
      }
    }

    return true;
  }

  /** Duplicates AcidUtils; used in a couple places in metastore. */
  public static boolean isInsertOnlyTableParam(Map<String, String> params) {
    String transactionalProp = params.get(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES);
    return (transactionalProp != null && "insert_only".equalsIgnoreCase(transactionalProp));
  }
}
