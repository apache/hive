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

import org.apache.hadoop.hive.metastore.api.WMPoolSchedulingPolicy;

import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.collections.ListUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
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
import org.apache.hadoop.hive.metastore.columnstats.merge.ColumnStatsMerger;
import org.apache.hadoop.hive.metastore.columnstats.merge.ColumnStatsMergerFactory;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.authorize.DefaultImpersonationProvider;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.util.MachineList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
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


  // Given a list of partStats, this function will give you an aggr stats
  public static List<ColumnStatisticsObj> aggrPartitionStats(List<ColumnStatistics> partStats,
      String dbName, String tableName, List<String> partNames, List<String> colNames,
      boolean areAllPartsFound, boolean useDensityFunctionForNDVEstimation, double ndvTuner)
      throws MetaException {
    Map<ColumnStatsAggregator, List<ColStatsObjWithSourceInfo>> colStatsMap =
        new HashMap<ColumnStatsAggregator, List<ColStatsObjWithSourceInfo>>();
    // Group stats by colName for each partition
    Map<String, ColumnStatsAggregator> aliasToAggregator =
        new HashMap<String, ColumnStatsAggregator>();
    for (ColumnStatistics css : partStats) {
      List<ColumnStatisticsObj> objs = css.getStatsObj();
      for (ColumnStatisticsObj obj : objs) {
        String partName = css.getStatsDesc().getPartName();
        if (aliasToAggregator.get(obj.getColName()) == null) {
          aliasToAggregator.put(obj.getColName(),
              ColumnStatsAggregatorFactory.getColumnStatsAggregator(
                  obj.getStatsData().getSetField(), useDensityFunctionForNDVEstimation, ndvTuner));
          colStatsMap.put(aliasToAggregator.get(obj.getColName()),
              new ArrayList<ColStatsObjWithSourceInfo>());
        }
        colStatsMap.get(aliasToAggregator.get(obj.getColName()))
            .add(new ColStatsObjWithSourceInfo(obj, dbName, tableName, partName));
      }
    }
    if (colStatsMap.size() < 1) {
      LOG.debug("No stats data found for: dbName= {},  tblName= {}, partNames= {}, colNames= {}",
          dbName, tableName, partNames, colNames);
      return new ArrayList<ColumnStatisticsObj>();
    }
    return aggrPartitionStats(colStatsMap, partNames, areAllPartsFound,
        useDensityFunctionForNDVEstimation, ndvTuner);
  }

  public static List<ColumnStatisticsObj> aggrPartitionStats(
      Map<ColumnStatsAggregator, List<ColStatsObjWithSourceInfo>> colStatsMap,
      final List<String> partNames, final boolean areAllPartsFound,
      final boolean useDensityFunctionForNDVEstimation, final double ndvTuner)
      throws MetaException {
    List<ColumnStatisticsObj> aggrColStatObjs = new ArrayList<ColumnStatisticsObj>();
    int numProcessors = Runtime.getRuntime().availableProcessors();
    final ExecutorService pool =
        Executors.newFixedThreadPool(Math.min(colStatsMap.size(), numProcessors),
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("aggr-col-stats-%d").build());
    final List<Future<ColumnStatisticsObj>> futures = Lists.newLinkedList();
    LOG.debug("Aggregating column stats. Threads used: {}",
        Math.min(colStatsMap.size(), numProcessors));
    long start = System.currentTimeMillis();
    for (final Entry<ColumnStatsAggregator, List<ColStatsObjWithSourceInfo>> entry : colStatsMap
        .entrySet()) {
      futures.add(pool.submit(new Callable<ColumnStatisticsObj>() {
        @Override
        public ColumnStatisticsObj call() throws MetaException {
          List<ColStatsObjWithSourceInfo> colStatWithSourceInfo = entry.getValue();
          ColumnStatsAggregator aggregator = entry.getKey();
          try {
            ColumnStatisticsObj statsObj =
                aggregator.aggregate(colStatWithSourceInfo, partNames, areAllPartsFound);
            return statsObj;
          } catch (MetaException e) {
            LOG.debug(e.getMessage());
            throw e;
          }
        }
      }));
    }
    pool.shutdown();
    if (!futures.isEmpty()) {
      for (Future<ColumnStatisticsObj> future : futures) {
        try {
          if (future.get() != null) {
            aggrColStatObjs.add(future.get());
          }
        } catch (InterruptedException | ExecutionException e) {
          LOG.debug(e.getMessage());
          pool.shutdownNow();
          throw new MetaException(e.toString());
        }

      }
    }
    LOG.debug("Time for aggr col stats in seconds: {} Threads used: {}",
        ((System.currentTimeMillis() - (double) start)) / 1000,
        Math.min(colStatsMap.size(), numProcessors));
    return aggrColStatObjs;
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

  private static String getPartitionValWithInvalidCharacter(List<String> partVals,
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
    // In case cols are null
    if (sd.getCols() != null) {
      for (FieldSchema fs : sd.getCols()) {
        md.update(fs.getName().getBytes(ENCODING));
        md.update(fs.getType().getBytes(ENCODING));
        if (fs.getComment() != null) md.update(fs.getComment().getBytes(ENCODING));
      }
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
    Pattern tpat;
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
  public static boolean requireCalStats(Partition oldPart,
                                        Partition newPart, Table tbl,
                                        EnvironmentContext environmentContext) {

    if (environmentContext != null
        && environmentContext.isSetProperties()
        && StatsSetupConst.TRUE.equals(environmentContext.getProperties().get(
            StatsSetupConst.DO_NOT_UPDATE_STATS))) {
      return false;
    }

    if (isView(tbl)) {
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
        params = new HashMap<>();
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
        params = new HashMap<>();
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

    Map<String, String> columnNameTypePairMap = new HashMap<>(newCols.size());
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

  /**
   * create listener instances as per the configuration.
   *
   * @param clazz Class of the listener
   * @param conf configuration object
   * @param listenerImplList Implementation class name
   * @return instance of the listener
   * @throws MetaException if there is any failure instantiating the class
   */
  public static <T> List<T> getMetaStoreListeners(Class<T> clazz,
      Configuration conf, String listenerImplList) throws MetaException {
    List<T> listeners = new ArrayList<T>();

    if (StringUtils.isBlank(listenerImplList)) {
      return listeners;
    }

    String[] listenerImpls = listenerImplList.split(",");
    for (String listenerImpl : listenerImpls) {
      try {
        T listener = (T) Class.forName(
            listenerImpl.trim(), true, JavaUtils.getClassLoader()).getConstructor(
                Configuration.class).newInstance(conf);
        listeners.add(listener);
      } catch (InvocationTargetException ie) {
        LOG.error("Got InvocationTargetException", ie);
        throw new MetaException("Failed to instantiate listener named: "+
            listenerImpl + ", reason: " + ie.getCause());
      } catch (Exception e) {
        LOG.error("Got Exception", e);
        throw new MetaException("Failed to instantiate listener named: "+
            listenerImpl + ", reason: " + e);
      }
    }

    return listeners;
  }

  public static String validateSkewedColNames(List<String> cols) {
    if (CollectionUtils.isEmpty(cols)) {
      return null;
    }
    for (String col : cols) {
      if (!validateColumnName(col)) {
        return col;
      }
    }
    return null;
  }

  public static String validateSkewedColNamesSubsetCol(List<String> skewedColNames,
      List<FieldSchema> cols) {
    if (CollectionUtils.isEmpty(skewedColNames)) {
      return null;
    }
    List<String> colNames = new ArrayList<>(cols.size());
    for (FieldSchema fieldSchema : cols) {
      colNames.add(fieldSchema.getName());
    }
    // make a copy
    List<String> copySkewedColNames = new ArrayList<>(skewedColNames);
    // remove valid columns
    copySkewedColNames.removeAll(colNames);
    if (copySkewedColNames.isEmpty()) {
      return null;
    }
    return copySkewedColNames.toString();
  }

  public static boolean isNonNativeTable(Table table) {
    if (table == null || table.getParameters() == null) {
      return false;
    }
    return (table.getParameters().get(hive_metastoreConstants.META_TABLE_STORAGE) != null);
  }

  public static boolean isIndexTable(Table table) {
    if (table == null) {
      return false;
    }
    return TableType.INDEX_TABLE.toString().equals(table.getTableType());
  }

  /**
   * Given a list of partition columns and a partial mapping from
   * some partition columns to values the function returns the values
   * for the column.
   * @param partCols the list of table partition columns
   * @param partSpec the partial mapping from partition column to values
   * @return list of values of for given partition columns, any missing
   *         values in partSpec is replaced by an empty string
   */
  public static List<String> getPvals(List<FieldSchema> partCols,
                                      Map<String, String> partSpec) {
    List<String> pvals = new ArrayList<>(partCols.size());
    for (FieldSchema field : partCols) {
      String val = StringUtils.defaultString(partSpec.get(field.getName()));
      pvals.add(val);
    }
    return pvals;
  }

  /**
   * @param schema1: The first schema to be compared
   * @param schema2: The second schema to be compared
   * @return true if the two schemas are the same else false
   *         for comparing a field we ignore the comment it has
   */
  public static boolean compareFieldColumns(List<FieldSchema> schema1, List<FieldSchema> schema2) {
    if (schema1.size() != schema2.size()) {
      return false;
    }
    Iterator<FieldSchema> its1 = schema1.iterator();
    Iterator<FieldSchema> its2 = schema2.iterator();
    while (its1.hasNext()) {
      FieldSchema f1 = its1.next();
      FieldSchema f2 = its2.next();
      // The default equals provided by thrift compares the comments too for
      // equality, thus we need to compare the relevant fields here.
      if (!StringUtils.equals(f1.getName(), f2.getName()) ||
          !StringUtils.equals(f1.getType(), f2.getType())) {
        return false;
      }
    }
    return true;
  }

  public static boolean isArchived(Partition part) {
    Map<String, String> params = part.getParameters();
    return "TRUE".equalsIgnoreCase(params.get(hive_metastoreConstants.IS_ARCHIVED));
  }

  public static Path getOriginalLocation(Partition part) {
    Map<String, String> params = part.getParameters();
    assert(isArchived(part));
    String originalLocation = params.get(hive_metastoreConstants.ORIGINAL_LOCATION);
    assert( originalLocation != null);

    return new Path(originalLocation);
  }

  private static String ARCHIVING_LEVEL = "archiving_level";
  public static int getArchivingLevel(Partition part) throws MetaException {
    if (!isArchived(part)) {
      throw new MetaException("Getting level of unarchived partition");
    }

    String lv = part.getParameters().get(ARCHIVING_LEVEL);
    if (lv != null) {
      return Integer.parseInt(lv);
    }
    // partitions archived before introducing multiple archiving
    return part.getValues().size();
  }

  public static boolean partitionNameHasValidCharacters(List<String> partVals,
      Pattern partitionValidationPattern) {
    return getPartitionValWithInvalidCharacter(partVals, partitionValidationPattern) == null;
  }

  public static void getMergableCols(ColumnStatistics csNew, Map<String, String> parameters) {
    List<ColumnStatisticsObj> list = new ArrayList<>();
    for (int index = 0; index < csNew.getStatsObj().size(); index++) {
      ColumnStatisticsObj statsObjNew = csNew.getStatsObj().get(index);
      // canColumnStatsMerge guarantees that it is accurate before we do merge
      if (StatsSetupConst.canColumnStatsMerge(parameters, statsObjNew.getColName())) {
        list.add(statsObjNew);
      }
      // in all the other cases, we can not merge
    }
    csNew.setStatsObj(list);
  }

  // this function will merge csOld into csNew.
  public static void mergeColStats(ColumnStatistics csNew, ColumnStatistics csOld)
      throws InvalidObjectException {
    List<ColumnStatisticsObj> list = new ArrayList<>();
    if (csNew.getStatsObj().size() != csOld.getStatsObjSize()) {
      // Some of the columns' stats are missing
      // This implies partition schema has changed. We will merge columns
      // present in both, overwrite stats for columns absent in metastore and
      // leave alone columns stats missing from stats task. This last case may
      // leave stats in stale state. This will be addressed later.
      LOG.debug("New ColumnStats size is {}, but old ColumnStats size is {}",
          csNew.getStatsObj().size(), csOld.getStatsObjSize());
    }
    // In this case, we have to find out which columns can be merged.
    Map<String, ColumnStatisticsObj> map = new HashMap<>();
    // We build a hash map from colName to object for old ColumnStats.
    for (ColumnStatisticsObj obj : csOld.getStatsObj()) {
      map.put(obj.getColName(), obj);
    }
    for (int index = 0; index < csNew.getStatsObj().size(); index++) {
      ColumnStatisticsObj statsObjNew = csNew.getStatsObj().get(index);
      ColumnStatisticsObj statsObjOld = map.get(statsObjNew.getColName());
      if (statsObjOld != null) {
        // because we already confirm that the stats is accurate
        // it is impossible that the column types have been changed while the
        // column stats is still accurate.
        assert (statsObjNew.getStatsData().getSetField() == statsObjOld.getStatsData()
            .getSetField());
        // If statsObjOld is found, we can merge.
        ColumnStatsMerger merger = ColumnStatsMergerFactory.getColumnStatsMerger(statsObjNew,
            statsObjOld);
        merger.merge(statsObjNew, statsObjOld);
      }
      // If statsObjOld is not found, we just use statsObjNew as it is accurate.
      list.add(statsObjNew);
    }
    // in all the other cases, we can not merge
    csNew.setStatsObj(list);
  }

  /**
   * Read and return the meta store Sasl configuration. Currently it uses the default
   * Hadoop SASL configuration and can be configured using "hadoop.rpc.protection"
   * HADOOP-10211, made a backward incompatible change due to which this call doesn't
   * work with Hadoop 2.4.0 and later.
   * @param conf
   * @return The SASL configuration
   */
  public static Map<String, String> getMetaStoreSaslProperties(Configuration conf, boolean useSSL) {
    // As of now Hive Meta Store uses the same configuration as Hadoop SASL configuration

    // If SSL is enabled, override the given value of "hadoop.rpc.protection" and set it to "authentication"
    // This disables any encryption provided by SASL, since SSL already provides it
    String hadoopRpcProtectionVal = conf.get(CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION);
    String hadoopRpcProtectionAuth = SaslRpcServer.QualityOfProtection.AUTHENTICATION.toString();

    if (useSSL && hadoopRpcProtectionVal != null && !hadoopRpcProtectionVal.equals(hadoopRpcProtectionAuth)) {
      LOG.warn("Overriding value of " + CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION + " setting it from "
          + hadoopRpcProtectionVal + " to " + hadoopRpcProtectionAuth + " because SSL is enabled");
      conf.set(CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION, hadoopRpcProtectionAuth);
    }
    return HadoopThriftAuthBridge.getBridge().getHadoopSaslProperties(conf);
  }

  /**
   * Add new elements to the classpath.
   *
   * @param newPaths
   *          Array of classpath elements
   */
  public static ClassLoader addToClassPath(ClassLoader cloader, String[] newPaths) throws Exception {
    URLClassLoader loader = (URLClassLoader) cloader;
    List<URL> curPath = Arrays.asList(loader.getURLs());
    ArrayList<URL> newPath = new ArrayList<>(curPath.size());

    // get a list with the current classpath components
    for (URL onePath : curPath) {
      newPath.add(onePath);
    }
    curPath = newPath;

    for (String onestr : newPaths) {
      URL oneurl = urlFromPathString(onestr);
      if (oneurl != null && !curPath.contains(oneurl)) {
        curPath.add(oneurl);
      }
    }

    return new URLClassLoader(curPath.toArray(new URL[0]), loader);
  }

  /**
   * Create a URL from a string representing a path to a local file.
   * The path string can be just a path, or can start with file:/, file:///
   * @param onestr  path string
   * @return
   */
  private static URL urlFromPathString(String onestr) {
    URL oneurl = null;
    try {
      if (onestr.startsWith("file:/")) {
        oneurl = new URL(onestr);
      } else {
        oneurl = new File(onestr).toURL();
      }
    } catch (Exception err) {
      LOG.error("Bad URL " + onestr + ", ignoring path");
    }
    return oneurl;
  }

  /**
   * Verify if the user is allowed to make DB notification related calls.
   * Only the superusers defined in the Hadoop proxy user settings have the permission.
   *
   * @param user the short user name
   * @param conf that contains the proxy user settings
   * @return if the user has the permission
   */
  public static boolean checkUserHasHostProxyPrivileges(String user, Configuration conf, String ipAddress) {
    DefaultImpersonationProvider sip = ProxyUsers.getDefaultImpersonationProvider();
    // Just need to initialize the ProxyUsers for the first time, given that the conf will not change on the fly
    if (sip == null) {
      ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
      sip = ProxyUsers.getDefaultImpersonationProvider();
    }
    Map<String, Collection<String>> proxyHosts = sip.getProxyHosts();
    Collection<String> hostEntries = proxyHosts.get(sip.getProxySuperuserIpConfKey(user));
    MachineList machineList = new MachineList(hostEntries);
    ipAddress = (ipAddress == null) ? StringUtils.EMPTY : ipAddress;
    return machineList.includes(ipAddress);
  }

  /**
   * Convert FieldSchemas to Thrift DDL.
   */
  public static String getDDLFromFieldSchema(String structName,
                                             List<FieldSchema> fieldSchemas) {
    StringBuilder ddl = new StringBuilder();
    ddl.append("struct ");
    ddl.append(structName);
    ddl.append(" { ");
    boolean first = true;
    for (FieldSchema col : fieldSchemas) {
      if (first) {
        first = false;
      } else {
        ddl.append(", ");
      }
      ddl.append(ColumnType.typeToThriftType(col.getType()));
      ddl.append(' ');
      ddl.append(col.getName());
    }
    ddl.append("}");

    LOG.trace("DDL: {}", ddl);
    return ddl.toString();
  }

  public static Properties getTableMetadata(
      org.apache.hadoop.hive.metastore.api.Table table) {
    return MetaStoreUtils.getSchema(table.getSd(), table.getSd(), table
        .getParameters(), table.getDbName(), table.getTableName(), table.getPartitionKeys());
  }

  public static Properties getPartitionMetadata(
      org.apache.hadoop.hive.metastore.api.Partition partition,
      org.apache.hadoop.hive.metastore.api.Table table) {
    return MetaStoreUtils
        .getSchema(partition.getSd(), partition.getSd(), partition
                .getParameters(), table.getDbName(), table.getTableName(),
            table.getPartitionKeys());
  }

  public static Properties getSchema(
      org.apache.hadoop.hive.metastore.api.Partition part,
      org.apache.hadoop.hive.metastore.api.Table table) {
    return MetaStoreUtils.getSchema(part.getSd(), table.getSd(), table
        .getParameters(), table.getDbName(), table.getTableName(), table.getPartitionKeys());
  }

  /**
   * Get partition level schema from table level schema.
   * This function will use the same column names, column types and partition keys for
   * each partition Properties. Their values are copied from the table Properties. This
   * is mainly to save CPU and memory. CPU is saved because the first time the
   * StorageDescriptor column names are accessed, JDO needs to execute a SQL query to
   * retrieve the data. If we know the data will be the same as the table level schema
   * and they are immutable, we should just reuse the table level schema objects.
   *
   * @param sd The Partition level Storage Descriptor.
   * @param parameters partition level parameters
   * @param tblSchema The table level schema from which this partition should be copied.
   * @return the properties
   */
  public static Properties getPartSchemaFromTableSchema(
      StorageDescriptor sd,
      Map<String, String> parameters,
      Properties tblSchema) {

    // Inherent most properties from table level schema and overwrite some properties
    // in the following code.
    // This is mainly for saving CPU and memory to reuse the column names, types and
    // partition columns in the table level schema.
    Properties schema = (Properties) tblSchema.clone();

    // InputFormat
    String inputFormat = sd.getInputFormat();
    if (inputFormat == null || inputFormat.length() == 0) {
      String tblInput =
          schema.getProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT);
      if (tblInput == null) {
        inputFormat = org.apache.hadoop.mapred.SequenceFileInputFormat.class.getName();
      } else {
        inputFormat = tblInput;
      }
    }
    schema.setProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT,
        inputFormat);

    // OutputFormat
    String outputFormat = sd.getOutputFormat();
    if (outputFormat == null || outputFormat.length() == 0) {
      String tblOutput =
          schema.getProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_OUTPUT_FORMAT);
      if (tblOutput == null) {
        outputFormat = org.apache.hadoop.mapred.SequenceFileOutputFormat.class.getName();
      } else {
        outputFormat = tblOutput;
      }
    }
    schema.setProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_OUTPUT_FORMAT,
        outputFormat);

    // Location
    if (sd.getLocation() != null) {
      schema.setProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_LOCATION,
          sd.getLocation());
    }

    // Bucket count
    schema.setProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.BUCKET_COUNT,
        Integer.toString(sd.getNumBuckets()));

    if (sd.getBucketCols() != null && sd.getBucketCols().size() > 0) {
      schema.setProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.BUCKET_FIELD_NAME,
          sd.getBucketCols().get(0));
    }

    // SerdeInfo
    if (sd.getSerdeInfo() != null) {

      // We should not update the following 3 values if SerDeInfo contains these.
      // This is to keep backward compatible with getSchema(), where these 3 keys
      // are updated after SerDeInfo properties got copied.
      String cols = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
      String colTypes = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;
      String parts = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS;

      for (Map.Entry<String,String> param : sd.getSerdeInfo().getParameters().entrySet()) {
        String key = param.getKey();
        if (schema.get(key) != null &&
            (key.equals(cols) || key.equals(colTypes) || key.equals(parts))) {
          continue;
        }
        schema.put(key, (param.getValue() != null) ? param.getValue() : StringUtils.EMPTY);
      }

      if (sd.getSerdeInfo().getSerializationLib() != null) {
        schema.setProperty(ColumnType.SERIALIZATION_LIB, sd.getSerdeInfo().getSerializationLib());
      }
    }

    // skipping columns since partition level field schemas are the same as table level's
    // skipping partition keys since it is the same as table level partition keys

    if (parameters != null) {
      for (Map.Entry<String, String> e : parameters.entrySet()) {
        schema.setProperty(e.getKey(), e.getValue());
      }
    }

    return schema;
  }

  private static Properties addCols(Properties schema, List<FieldSchema> cols) {

    StringBuilder colNameBuf = new StringBuilder();
    StringBuilder colTypeBuf = new StringBuilder();
    StringBuilder colComment = new StringBuilder();

    boolean first = true;
    String columnNameDelimiter = getColumnNameDelimiter(cols);
    for (FieldSchema col : cols) {
      if (!first) {
        colNameBuf.append(columnNameDelimiter);
        colTypeBuf.append(":");
        colComment.append('\0');
      }
      colNameBuf.append(col.getName());
      colTypeBuf.append(col.getType());
      colComment.append((null != col.getComment()) ? col.getComment() : StringUtils.EMPTY);
      first = false;
    }
    schema.setProperty(
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS,
        colNameBuf.toString());
    schema.setProperty(ColumnType.COLUMN_NAME_DELIMITER, columnNameDelimiter);
    String colTypes = colTypeBuf.toString();
    schema.setProperty(
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES,
        colTypes);
    schema.setProperty("columns.comments", colComment.toString());

    return schema;

  }

  public static Properties getSchemaWithoutCols(StorageDescriptor sd,
                                                Map<String, String> parameters, String databaseName, String tableName,
                                                List<FieldSchema> partitionKeys) {
    Properties schema = new Properties();
    String inputFormat = sd.getInputFormat();
    if (inputFormat == null || inputFormat.length() == 0) {
      inputFormat = org.apache.hadoop.mapred.SequenceFileInputFormat.class
          .getName();
    }
    schema.setProperty(
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT,
        inputFormat);
    String outputFormat = sd.getOutputFormat();
    if (outputFormat == null || outputFormat.length() == 0) {
      outputFormat = org.apache.hadoop.mapred.SequenceFileOutputFormat.class
          .getName();
    }
    schema.setProperty(
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_OUTPUT_FORMAT,
        outputFormat);

    schema.setProperty(
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_NAME,
        databaseName + "." + tableName);

    if (sd.getLocation() != null) {
      schema.setProperty(
          org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_LOCATION,
          sd.getLocation());
    }
    schema.setProperty(
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.BUCKET_COUNT, Integer
            .toString(sd.getNumBuckets()));
    if (sd.getBucketCols() != null && sd.getBucketCols().size() > 0) {
      schema.setProperty(
          org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.BUCKET_FIELD_NAME, sd
              .getBucketCols().get(0));
    }
    if (sd.getSerdeInfo() != null) {
      for (Map.Entry<String,String> param : sd.getSerdeInfo().getParameters().entrySet()) {
        schema.put(param.getKey(), (param.getValue() != null) ? param.getValue() : StringUtils.EMPTY);
      }

      if (sd.getSerdeInfo().getSerializationLib() != null) {
        schema.setProperty(ColumnType.SERIALIZATION_LIB, sd .getSerdeInfo().getSerializationLib());
      }
    }

    if (sd.getCols() != null) {
      schema.setProperty(ColumnType.SERIALIZATION_DDL, getDDLFromFieldSchema(tableName, sd.getCols()));
    }

    String partString = StringUtils.EMPTY;
    String partStringSep = StringUtils.EMPTY;
    String partTypesString = StringUtils.EMPTY;
    String partTypesStringSep = StringUtils.EMPTY;
    for (FieldSchema partKey : partitionKeys) {
      partString = partString.concat(partStringSep);
      partString = partString.concat(partKey.getName());
      partTypesString = partTypesString.concat(partTypesStringSep);
      partTypesString = partTypesString.concat(partKey.getType());
      if (partStringSep.length() == 0) {
        partStringSep = "/";
        partTypesStringSep = ":";
      }
    }
    if (partString.length() > 0) {
      schema
          .setProperty(
              org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS,
              partString);
      schema
          .setProperty(
              org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_PARTITION_COLUMN_TYPES,
              partTypesString);
    }

    if (parameters != null) {
      for (Map.Entry<String, String> e : parameters.entrySet()) {
        // add non-null parameters to the schema
        if ( e.getValue() != null) {
          schema.setProperty(e.getKey(), e.getValue());
        }
      }
    }

    return schema;
  }

  public static Properties getSchema(
      org.apache.hadoop.hive.metastore.api.StorageDescriptor sd,
      org.apache.hadoop.hive.metastore.api.StorageDescriptor tblsd,
      Map<String, String> parameters, String databaseName, String tableName,
      List<FieldSchema> partitionKeys) {

    return addCols(getSchemaWithoutCols(sd, parameters, databaseName, tableName, partitionKeys), tblsd.getCols());
  }

  public static String getColumnNameDelimiter(List<FieldSchema> fieldSchemas) {
    // we first take a look if any fieldSchemas contain COMMA
    for (int i = 0; i < fieldSchemas.size(); i++) {
      if (fieldSchemas.get(i).getName().contains(",")) {
        return String.valueOf(ColumnType.COLUMN_COMMENTS_DELIMITER);
      }
    }
    return String.valueOf(',');
  }

  /**
   * Convert FieldSchemas to columnNames.
   */
  public static String getColumnNamesFromFieldSchema(List<FieldSchema> fieldSchemas) {
    String delimiter = getColumnNameDelimiter(fieldSchemas);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < fieldSchemas.size(); i++) {
      if (i > 0) {
        sb.append(delimiter);
      }
      sb.append(fieldSchemas.get(i).getName());
    }
    return sb.toString();
  }

  /**
   * Convert FieldSchemas to columnTypes.
   */
  public static String getColumnTypesFromFieldSchema(
      List<FieldSchema> fieldSchemas) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < fieldSchemas.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append(fieldSchemas.get(i).getType());
    }
    return sb.toString();
  }

  public static String getColumnCommentsFromFieldSchema(List<FieldSchema> fieldSchemas) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < fieldSchemas.size(); i++) {
      if (i > 0) {
        sb.append(ColumnType.COLUMN_COMMENTS_DELIMITER);
      }
      sb.append(fieldSchemas.get(i).getComment());
    }
    return sb.toString();
  }

  public static int startMetaStore() throws Exception {
    return startMetaStore(HadoopThriftAuthBridge.getBridge(), null);
  }

  public static int startMetaStore(final HadoopThriftAuthBridge bridge, Configuration conf) throws
      Exception {
    int port = findFreePort();
    startMetaStore(port, bridge, conf);
    return port;
  }

  public static int startMetaStore(Configuration conf) throws Exception {
    return startMetaStore(HadoopThriftAuthBridge.getBridge(), conf);
  }

  public static void startMetaStore(final int port, final HadoopThriftAuthBridge bridge) throws Exception {
    startMetaStore(port, bridge, null);
  }

  public static void startMetaStore(final int port,
                                    final HadoopThriftAuthBridge bridge, Configuration hiveConf)
      throws Exception{
    if (hiveConf == null) {
      hiveConf = MetastoreConf.newMetastoreConf();
    }
    final Configuration finalHiveConf = hiveConf;
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          HiveMetaStore.startMetaStore(port, bridge, finalHiveConf);
        } catch (Throwable e) {
          LOG.error("Metastore Thrift Server threw an exception...",e);
        }
      }
    });
    thread.setDaemon(true);
    thread.start();
    loopUntilHMSReady(port);
  }

  /**
   * A simple connect test to make sure that the metastore is up
   * @throws Exception
   */
  private static void loopUntilHMSReady(int port) throws Exception {
    int retries = 0;
    Exception exc;
    while (true) {
      try {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(port), 5000);
        socket.close();
        return;
      } catch (Exception e) {
        if (retries++ > 60) { //give up
          exc = e;
          break;
        }
        Thread.sleep(1000);
      }
    }
    // something is preventing metastore from starting
    // print the stack from all threads for debugging purposes
    LOG.error("Unable to connect to metastore server: " + exc.getMessage());
    LOG.info("Printing all thread stack traces for debugging before throwing exception.");
    LOG.info(getAllThreadStacksAsString());
    throw exc;
  }

  private static String getAllThreadStacksAsString() {
    Map<Thread, StackTraceElement[]> threadStacks = Thread.getAllStackTraces();
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<Thread, StackTraceElement[]> entry : threadStacks.entrySet()) {
      Thread t = entry.getKey();
      sb.append(System.lineSeparator());
      sb.append("Name: ").append(t.getName()).append(" State: ").append(t.getState());
      addStackString(entry.getValue(), sb);
    }
    return sb.toString();
  }

  private static void addStackString(StackTraceElement[] stackElems, StringBuilder sb) {
    sb.append(System.lineSeparator());
    for (StackTraceElement stackElem : stackElems) {
      sb.append(stackElem).append(System.lineSeparator());
    }
  }

  /**
   * Finds a free port on the machine.
   *
   * @return
   * @throws IOException
   */
  public static int findFreePort() throws IOException {
    ServerSocket socket= new ServerSocket(0);
    int port = socket.getLocalPort();
    socket.close();
    return port;
  }

  /**
   * Finds a free port on the machine, but allow the
   * ability to specify a port number to not use, no matter what.
   */
  public static int findFreePortExcepting(int portToExclude) throws IOException {
    ServerSocket socket1 = null;
    ServerSocket socket2 = null;
    try {
      socket1 = new ServerSocket(0);
      socket2 = new ServerSocket(0);
      if (socket1.getLocalPort() != portToExclude) {
        return socket1.getLocalPort();
      }
      // If we're here, then socket1.getLocalPort was the port to exclude
      // Since both sockets were open together at a point in time, we're
      // guaranteed that socket2.getLocalPort() is not the same.
      return socket2.getLocalPort();
    } finally {
      if (socket1 != null){
        socket1.close();
      }
      if (socket2 != null){
        socket2.close();
      }
    }
  }

  public static String getIndexTableName(String dbName, String baseTblName, String indexName) {
    return dbName + "__" + baseTblName + "_" + indexName + "__";
  }

  public static boolean isMaterializedViewTable(Table table) {
    if (table == null) {
      return false;
    }
    return TableType.MATERIALIZED_VIEW.toString().equals(table.getTableType());
  }

  public static List<String> getColumnNames(List<FieldSchema> schema) {
    List<String> cols = new ArrayList<>(schema.size());
    for (FieldSchema fs : schema) {
      cols.add(fs.getName());
    }
    return cols;
  }

  public static boolean isValidSchedulingPolicy(String str) {
    try {
      parseSchedulingPolicy(str);
      return true;
    } catch (IllegalArgumentException ex) {
    }
    return false;
  }

  public static WMPoolSchedulingPolicy parseSchedulingPolicy(String schedulingPolicy) {
    if (schedulingPolicy == null) return WMPoolSchedulingPolicy.FAIR;
    schedulingPolicy = schedulingPolicy.trim().toUpperCase();
    if ("DEFAULT".equals(schedulingPolicy)) return WMPoolSchedulingPolicy.FAIR;
    return Enum.valueOf(WMPoolSchedulingPolicy.class, schedulingPolicy);
  }

  // ColumnStatisticsObj with info about its db, table, partition (if table is partitioned)
  public static class ColStatsObjWithSourceInfo {
    private final ColumnStatisticsObj colStatsObj;
    private final String dbName;
    private final String tblName;
    private final String partName;

    public ColStatsObjWithSourceInfo(ColumnStatisticsObj colStatsObj, String dbName, String tblName,
        String partName) {
      this.colStatsObj = colStatsObj;
      this.dbName = dbName;
      this.tblName = tblName;
      this.partName = partName;
    }

    public ColumnStatisticsObj getColStatsObj() {
      return colStatsObj;
    }

    public String getDbName() {
      return dbName;
    }

    public String getTblName() {
      return tblName;
    }

    public String getPartName() {
      return partName;
    }
  }
}
