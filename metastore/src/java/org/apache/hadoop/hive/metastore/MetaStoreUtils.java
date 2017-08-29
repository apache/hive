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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
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

import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.columnstats.aggr.ColumnStatsAggregator;
import org.apache.hadoop.hive.metastore.columnstats.aggr.ColumnStatsAggregatorFactory;
import org.apache.hadoop.hive.metastore.columnstats.merge.ColumnStatsMerger;
import org.apache.hadoop.hive.metastore.columnstats.merge.ColumnStatsMergerFactory;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.hive.common.util.ReflectionUtil;

import javax.annotation.Nullable;

public class MetaStoreUtils {

  protected static final Logger LOG = LoggerFactory.getLogger("hive.log");

  public static final String DEFAULT_DATABASE_NAME = "default";
  public static final String DEFAULT_DATABASE_COMMENT = "Default Hive database";
  public static final String DEFAULT_SERIALIZATION_FORMAT = "1";

  public static final String DATABASE_WAREHOUSE_SUFFIX = ".db";

  // Right now we only support one special character '/'.
  // More special characters can be added accordingly in the future.
  // NOTE:
  // If the following array is updated, please also be sure to update the
  // configuration parameter documentation
  // HIVE_SUPPORT_SPECICAL_CHARACTERS_IN_TABLE_NAMES in HiveConf as well.
  public static final char[] specialCharactersInTableNames = new char[] { '/' };
  final static Charset ENCODING = StandardCharsets.UTF_8;

  public static Table createColumnsetSchema(String name, List<String> columns,
      List<String> partCols, Configuration conf) throws MetaException {

    if (columns == null) {
      throw new MetaException("columns not specified for table " + name);
    }

    Table tTable = new Table();
    tTable.setTableName(name);
    tTable.setSd(new StorageDescriptor());
    StorageDescriptor sd = tTable.getSd();
    sd.setSerdeInfo(new SerDeInfo());
    SerDeInfo serdeInfo = sd.getSerdeInfo();
    serdeInfo.setSerializationLib(LazySimpleSerDe.class.getName());
    serdeInfo.setParameters(new HashMap<String, String>());
    serdeInfo.getParameters().put(org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT,
        DEFAULT_SERIALIZATION_FORMAT);

    List<FieldSchema> fields = new ArrayList<FieldSchema>(columns.size());
    sd.setCols(fields);
    for (String col : columns) {
      FieldSchema field = new FieldSchema(col,
          org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME, "'default'");
      fields.add(field);
    }

    tTable.setPartitionKeys(new ArrayList<FieldSchema>());
    for (String partCol : partCols) {
      FieldSchema part = new FieldSchema();
      part.setName(partCol);
      part.setType(org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME); // default
                                                                             // partition
                                                                             // key
      tTable.getPartitionKeys().add(part);
    }
    sd.setNumBuckets(-1);
    return tTable;
  }

  /**
   * recursiveDelete
   *
   * just recursively deletes a dir - you'd think Java would have something to
   * do this??
   *
   * @param f
   *          - the file/dir to delete
   * @exception IOException
   *              propogate f.delete() exceptions
   *
   */
  static public void recursiveDelete(File f) throws IOException {
    if (f.isDirectory()) {
      File fs[] = f.listFiles();
      for (File subf : fs) {
        recursiveDelete(subf);
      }
    }
    if (!f.delete()) {
      throw new IOException("could not delete: " + f.getPath());
    }
  }

  /**
   * @param partParams
   * @return True if the passed Parameters Map contains values for all "Fast Stats".
   */
  public static boolean containsAllFastStats(Map<String, String> partParams) {
    for (String stat : StatsSetupConst.fastStats) {
      if (!partParams.containsKey(stat)) {
        return false;
      }
    }
    return true;
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

  static boolean isFastStatsSame(Partition oldPart, Partition newPart) {
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

  static void updateBasicState(EnvironmentContext environmentContext, Map<String,String> params) {
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

  /**
   * getDeserializer
   *
   * Get the Deserializer for a table.
   *
   * @param conf
   *          - hadoop config
   * @param table
   *          the table
   * @return
   *   Returns instantiated deserializer by looking up class name of deserializer stored in
   *   storage descriptor of passed in table. Also, initializes the deserializer with schema
   *   of table.
   * @exception MetaException
   *              if any problems instantiating the Deserializer
   *
   *              todo - this should move somewhere into serde.jar
   *
   */
  static public Deserializer getDeserializer(Configuration conf,
      org.apache.hadoop.hive.metastore.api.Table table, boolean skipConfError) throws
          MetaException {
    String lib = table.getSd().getSerdeInfo().getSerializationLib();
    if (lib == null) {
      return null;
    }
    return getDeserializer(conf, table, skipConfError, lib);
  }

  public static Deserializer getDeserializer(Configuration conf,
      org.apache.hadoop.hive.metastore.api.Table table, boolean skipConfError,
      String lib) throws MetaException {
    try {
      Deserializer deserializer = ReflectionUtil.newInstance(conf.getClassByName(lib).
              asSubclass(Deserializer.class), conf);
      if (skipConfError) {
        SerDeUtils.initializeSerDeWithoutErrorCheck(deserializer, conf,
                MetaStoreUtils.getTableMetadata(table), null);
      } else {
        SerDeUtils.initializeSerDe(deserializer, conf, MetaStoreUtils.getTableMetadata(table), null);
      }
      return deserializer;
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable e) {
      LOG.error("error in initSerDe: " + e.getClass().getName() + " "
          + e.getMessage(), e);
      throw new MetaException(e.getClass().getName() + " " + e.getMessage());
    }
  }

  public static Class<? extends Deserializer> getDeserializerClass(
      Configuration conf, org.apache.hadoop.hive.metastore.api.Table table) throws Exception {
    String lib = table.getSd().getSerdeInfo().getSerializationLib();
    return lib == null ? null : conf.getClassByName(lib).asSubclass(Deserializer.class);
  }

  /**
   * getDeserializer
   *
   * Get the Deserializer for a partition.
   *
   * @param conf
   *          - hadoop config
   * @param part
   *          the partition
   * @param table the table
   * @return
   *   Returns instantiated deserializer by looking up class name of deserializer stored in
   *   storage descriptor of passed in partition. Also, initializes the deserializer with
   *   schema of partition.
   * @exception MetaException
   *              if any problems instantiating the Deserializer
   *
   */
  static public Deserializer getDeserializer(Configuration conf,
      org.apache.hadoop.hive.metastore.api.Partition part,
      org.apache.hadoop.hive.metastore.api.Table table) throws MetaException {
    String lib = part.getSd().getSerdeInfo().getSerializationLib();
    try {
      Deserializer deserializer = ReflectionUtil.newInstance(conf.getClassByName(lib).
        asSubclass(Deserializer.class), conf);
      SerDeUtils.initializeSerDe(deserializer, conf, MetaStoreUtils.getTableMetadata(table),
                                 MetaStoreUtils.getPartitionMetadata(part, table));
      return deserializer;
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable e) {
      LOG.error("error in initSerDe: " + e.getClass().getName() + " "
          + e.getMessage(), e);
      throw new MetaException(e.getClass().getName() + " " + e.getMessage());
    }
  }

  static public void deleteWHDirectory(Path path, Configuration conf,
      boolean use_trash) throws MetaException {

    try {
      if (!path.getFileSystem(conf).exists(path)) {
        LOG.warn("drop data called on table/partition with no directory: "
            + path);
        return;
      }

      if (use_trash) {

        int count = 0;
        Path newPath = new Path("/Trash/Current"
            + path.getParent().toUri().getPath());

        if (path.getFileSystem(conf).exists(newPath) == false) {
          path.getFileSystem(conf).mkdirs(newPath);
        }

        do {
          newPath = new Path("/Trash/Current" + path.toUri().getPath() + "."
              + count);
          if (path.getFileSystem(conf).exists(newPath)) {
            count++;
            continue;
          }
          if (path.getFileSystem(conf).rename(path, newPath)) {
            break;
          }
        } while (++count < 50);
        if (count >= 50) {
          throw new MetaException("Rename failed due to maxing out retries");
        }
      } else {
        // directly delete it
        path.getFileSystem(conf).delete(path, true);
      }
    } catch (IOException e) {
      LOG.error("Got exception trying to delete data dir: " + e);
      throw new MetaException(e.getMessage());
    } catch (MetaException e) {
      LOG.error("Got exception trying to delete data dir: " + e);
      throw e;
    }
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
    List<String> pvals = new ArrayList<String>(partCols.size());
    for (FieldSchema field : partCols) {
      String val = StringUtils.defaultString(partSpec.get(field.getName()));
      pvals.add(val);
    }
    return pvals;
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
  static public boolean validateName(String name, Configuration conf) {
    Pattern tpat = null;
    String allowedCharacters = "\\w_";
    if (conf != null
        && HiveConf.getBoolVar(conf,
            HiveConf.ConfVars.HIVE_SUPPORT_SPECICAL_CHARACTERS_IN_TABLE_NAMES)) {
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
  public static final boolean validateColumnName(String name) {
    return true;
  }

  static public String validateTblColumns(List<FieldSchema> cols) {
    for (FieldSchema fieldSchema : cols) {
      if (!validateColumnName(fieldSchema.getName())) {
        return "name: " + fieldSchema.getName();
      }
      String typeError = validateColumnType(fieldSchema.getType());
      if (typeError != null) {
        return typeError;
      }
    }
    return null;
  }

  static void throwExceptionIfIncompatibleColTypeChange(
      List<FieldSchema> oldCols, List<FieldSchema> newCols)
      throws InvalidOperationException {

    List<String> incompatibleCols = new ArrayList<String>();
    int maxCols = Math.min(oldCols.size(), newCols.size());
    for (int i = 0; i < maxCols; i++) {
      if (!areColTypesCompatible(oldCols.get(i).getType(), newCols.get(i).getType())) {
        incompatibleCols.add(newCols.get(i).getName());
      }
    }
    if (!incompatibleCols.isEmpty()) {
      throw new InvalidOperationException(
          "The following columns have types incompatible with the existing " +
          "columns in their respective positions :\n" +
          StringUtils.join(incompatibleCols, ',')
        );
    }
  }

  static boolean areSameColumns(List<FieldSchema> oldCols, List<FieldSchema> newCols) {
    return ListUtils.isEqualList(oldCols, newCols);
  }

  /*
   * This method is to check if the new column list includes all the old columns with same name and
   * type. The column comment does not count.
   */
  static boolean columnsIncludedByNameType(List<FieldSchema> oldCols, List<FieldSchema> newCols) {
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

  /**
   * @return true if oldType and newType are compatible.
   * Two types are compatible if we have internal functions to cast one to another.
   */
  static private boolean areColTypesCompatible(String oldType, String newType) {

    /*
     * RCFile default serde (ColumnarSerde) serializes the values in such a way that the
     * datatypes can be converted from string to any type. The map is also serialized as
     * a string, which can be read as a string as well. However, with any binary
     * serialization, this is not true.
     *
     * Primitive types like INT, STRING, BIGINT, etc are compatible with each other and are
     * not blocked.
     */

    return TypeInfoUtils.implicitConvertible(TypeInfoUtils.getTypeInfoFromTypeString(oldType),
      TypeInfoUtils.getTypeInfoFromTypeString(newType));
  }

  public static final String TYPE_FROM_DESERIALIZER = "<derived from deserializer>";
  /**
   * validate column type
   *
   * if it is predefined, yes. otherwise no
   * @param type
   * @return
   */
  static public String validateColumnType(String type) {
    if (type.equals(TYPE_FROM_DESERIALIZER)) return null;
    int last = 0;
    boolean lastAlphaDigit = isValidTypeChar(type.charAt(last));
    for (int i = 1; i <= type.length(); i++) {
      if (i == type.length()
          || isValidTypeChar(type.charAt(i)) != lastAlphaDigit) {
        String token = type.substring(last, i);
        last = i;
        if (!hiveThriftTypeMap.contains(token)) {
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
    List<String> colNames = new ArrayList<String>(cols.size());
    for (FieldSchema fieldSchema : cols) {
      colNames.add(fieldSchema.getName());
    }
    // make a copy
    List<String> copySkewedColNames = new ArrayList<String>(skewedColNames);
    // remove valid columns
    copySkewedColNames.removeAll(colNames);
    if (copySkewedColNames.isEmpty()) {
      return null;
    }
    return copySkewedColNames.toString();
  }

  public static String getListType(String t) {
    return "array<" + t + ">";
  }

  public static String getMapType(String k, String v) {
    return "map<" + k + "," + v + ">";
  }

  public static void setSerdeParam(SerDeInfo sdi, Properties schema,
      String param) {
    String val = schema.getProperty(param);
    if (org.apache.commons.lang.StringUtils.isNotBlank(val)) {
      sdi.getParameters().put(param, val);
    }
  }

  static HashMap<String, String> typeToThriftTypeMap;
  static {
    typeToThriftTypeMap = new HashMap<String, String>();
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.BOOLEAN_TYPE_NAME, "bool");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.TINYINT_TYPE_NAME, "byte");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.SMALLINT_TYPE_NAME, "i16");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.INT_TYPE_NAME, "i32");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.BIGINT_TYPE_NAME, "i64");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.DOUBLE_TYPE_NAME, "double");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.FLOAT_TYPE_NAME, "float");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.LIST_TYPE_NAME, "list");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.MAP_TYPE_NAME, "map");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME, "string");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.BINARY_TYPE_NAME, "binary");
    // These 4 types are not supported yet.
    // We should define a complex type date in thrift that contains a single int
    // member, and DynamicSerDe
    // should convert it to date type at runtime.
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.DATE_TYPE_NAME, "date");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.DATETIME_TYPE_NAME, "datetime");
    typeToThriftTypeMap
        .put(org.apache.hadoop.hive.serde.serdeConstants.TIMESTAMP_TYPE_NAME,
            "timestamp");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.DECIMAL_TYPE_NAME, "decimal");
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME,
        org.apache.hadoop.hive.serde.serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME);
    typeToThriftTypeMap.put(
        org.apache.hadoop.hive.serde.serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME,
        org.apache.hadoop.hive.serde.serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME);
  }

  static Set<String> hiveThriftTypeMap; //for validation
  static {
    hiveThriftTypeMap = new HashSet<String>();
    hiveThriftTypeMap.addAll(serdeConstants.PrimitiveTypes);
    hiveThriftTypeMap.addAll(org.apache.hadoop.hive.serde.serdeConstants.CollectionTypes);
    hiveThriftTypeMap.add(org.apache.hadoop.hive.serde.serdeConstants.UNION_TYPE_NAME);
    hiveThriftTypeMap.add(org.apache.hadoop.hive.serde.serdeConstants.STRUCT_TYPE_NAME);
  }

  /**
   * Convert type to ThriftType. We do that by tokenizing the type and convert
   * each token.
   */
  public static String typeToThriftType(String type) {
    StringBuilder thriftType = new StringBuilder();
    int last = 0;
    boolean lastAlphaDigit = Character.isLetterOrDigit(type.charAt(last));
    for (int i = 1; i <= type.length(); i++) {
      if (i == type.length()
          || Character.isLetterOrDigit(type.charAt(i)) != lastAlphaDigit) {
        String token = type.substring(last, i);
        last = i;
        String thriftToken = typeToThriftTypeMap.get(token);
        thriftType.append(thriftToken == null ? token : thriftToken);
        lastAlphaDigit = !lastAlphaDigit;
      }
    }
    return thriftType.toString();
  }

  /**
   * Convert FieldSchemas to Thrift DDL + column names and column types
   *
   * @param structName
   *          The name of the table
   * @param fieldSchemas
   *          List of fields along with their schemas
   * @return String containing "Thrift
   *         DDL#comma-separated-column-names#colon-separated-columntypes
   *         Example:
   *         "struct result { a string, map&lt;int,string&gt; b}#a,b#string:map&lt;int,string&gt;"
   */
  public static String getFullDDLFromFieldSchema(String structName,
      List<FieldSchema> fieldSchemas) {
    StringBuilder ddl = new StringBuilder();
    ddl.append(getDDLFromFieldSchema(structName, fieldSchemas));
    ddl.append('#');
    StringBuilder colnames = new StringBuilder();
    StringBuilder coltypes = new StringBuilder();
    boolean first = true;
    for (FieldSchema col : fieldSchemas) {
      if (first) {
        first = false;
      } else {
        colnames.append(',');
        coltypes.append(':');
      }
      colnames.append(col.getName());
      coltypes.append(col.getType());
    }
    ddl.append(colnames);
    ddl.append('#');
    ddl.append(coltypes);
    return ddl.toString();
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
      ddl.append(typeToThriftType(col.getType()));
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
   * @param tblsd The Table level Storage Descriptor.
   * @param parameters partition level parameters
   * @param databaseName DB name
   * @param tableName table name
   * @param partitionKeys partition columns
   * @param tblSchema The table level schema from which this partition should be copied.
   * @return the properties
   */
  public static Properties getPartSchemaFromTableSchema(
      org.apache.hadoop.hive.metastore.api.StorageDescriptor sd,
      org.apache.hadoop.hive.metastore.api.StorageDescriptor tblsd,
      Map<String, String> parameters, String databaseName, String tableName,
      List<FieldSchema> partitionKeys,
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
        schema.setProperty(org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB,
            sd.getSerdeInfo().getSerializationLib());
      }
    }

    // skipping columns since partition level field schemas are the same as table level's
    // skipping partition keys since it is the same as table level partition keys

    if (parameters != null) {
      for (Entry<String, String> e : parameters.entrySet()) {
        schema.setProperty(e.getKey(), e.getValue());
      }
    }

    return schema;
  }

  public static Properties addCols(Properties schema, List<FieldSchema> cols) {

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
    schema.setProperty(serdeConstants.COLUMN_NAME_DELIMITER, columnNameDelimiter);
    String colTypes = colTypeBuf.toString();
    schema.setProperty(
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES,
        colTypes);
    schema.setProperty("columns.comments", colComment.toString());

    return schema;

  }

  public static Properties getSchemaWithoutCols(org.apache.hadoop.hive.metastore.api.StorageDescriptor sd,
      org.apache.hadoop.hive.metastore.api.StorageDescriptor tblsd,
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
        schema.setProperty(
            org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB, sd
                .getSerdeInfo().getSerializationLib());
      }
    }

    if (sd.getCols() != null) {
      schema.setProperty(
          org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_DDL,
          getDDLFromFieldSchema(tableName, sd.getCols()));
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
      for (Entry<String, String> e : parameters.entrySet()) {
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

    return addCols(getSchemaWithoutCols(sd, tblsd, parameters, databaseName, tableName, partitionKeys), tblsd.getCols());
  }

  public static List<String> getColumnNamesForTable(Table table) {
    List<String> colNames = new ArrayList<String>();
    Iterator<FieldSchema> colsIterator = table.getSd().getColsIterator();
    while (colsIterator.hasNext()) {
      colNames.add(colsIterator.next().getName());
    }
    return colNames;
  }

  public static String getColumnNameDelimiter(List<FieldSchema> fieldSchemas) {
    // we first take a look if any fieldSchemas contain COMMA
    for (int i = 0; i < fieldSchemas.size(); i++) {
      if (fieldSchemas.get(i).getName().contains(",")) {
        return String.valueOf(SerDeUtils.COLUMN_COMMENTS_DELIMITER);
      }
    }
    return String.valueOf(SerDeUtils.COMMA);
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
        sb.append(SerDeUtils.COLUMN_COMMENTS_DELIMITER);
      }
      sb.append(fieldSchemas.get(i).getComment());
    }
    return sb.toString();
  }

  public static void makeDir(Path path, HiveConf hiveConf) throws MetaException {
    FileSystem fs;
    try {
      fs = path.getFileSystem(hiveConf);
      if (!fs.exists(path)) {
        fs.mkdirs(path);
      }
    } catch (IOException e) {
      throw new MetaException("Unable to : " + path);
    }
  }

  public static int startMetaStore() throws Exception {
    return startMetaStore(HadoopThriftAuthBridge.getBridge(), null);
  }

  public static int startMetaStore(final HadoopThriftAuthBridge bridge, HiveConf conf) throws Exception {
    int port = findFreePort();
    startMetaStore(port, bridge, conf);
    return port;
  }

  public static int startMetaStore(HiveConf conf) throws Exception {
    return startMetaStore(HadoopThriftAuthBridge.getBridge(), conf);
  }

  public static void startMetaStore(final int port, final HadoopThriftAuthBridge bridge) throws Exception {
    startMetaStore(port, bridge, null);
  }

  public static void startMetaStore(final int port,
      final HadoopThriftAuthBridge bridge, HiveConf hiveConf)
      throws Exception{
    if (hiveConf == null) {
      hiveConf = new HiveConf(HMSHandler.class);
    }
    final HiveConf finalHiveConf = hiveConf;
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
    Exception exc = null;
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

  /**
   * Catches exceptions that can't be handled and bundles them to MetaException
   *
   * @param e
   * @throws MetaException
   */
  static void logAndThrowMetaException(Exception e) throws MetaException {
    String exInfo = "Got exception: " + e.getClass().getName() + " "
        + e.getMessage();
    LOG.error(exInfo, e);
    LOG.error("Converting exception to MetaException");
    throw new MetaException(exInfo);
  }

  /**
   * @param tableName
   * @param deserializer
   * @return the list of fields
   * @throws SerDeException
   * @throws MetaException
   */
  public static List<FieldSchema> getFieldsFromDeserializer(String tableName,
      Deserializer deserializer) throws SerDeException, MetaException {
    ObjectInspector oi = deserializer.getObjectInspector();
    String[] names = tableName.split("\\.");
    String last_name = names[names.length - 1];
    for (int i = 1; i < names.length; i++) {

      if (oi instanceof StructObjectInspector) {
        StructObjectInspector soi = (StructObjectInspector) oi;
        StructField sf = soi.getStructFieldRef(names[i]);
        if (sf == null) {
          throw new MetaException("Invalid Field " + names[i]);
        } else {
          oi = sf.getFieldObjectInspector();
        }
      } else if (oi instanceof ListObjectInspector
          && names[i].equalsIgnoreCase("$elem$")) {
        ListObjectInspector loi = (ListObjectInspector) oi;
        oi = loi.getListElementObjectInspector();
      } else if (oi instanceof MapObjectInspector
          && names[i].equalsIgnoreCase("$key$")) {
        MapObjectInspector moi = (MapObjectInspector) oi;
        oi = moi.getMapKeyObjectInspector();
      } else if (oi instanceof MapObjectInspector
          && names[i].equalsIgnoreCase("$value$")) {
        MapObjectInspector moi = (MapObjectInspector) oi;
        oi = moi.getMapValueObjectInspector();
      } else {
        throw new MetaException("Unknown type for " + names[i]);
      }
    }

    ArrayList<FieldSchema> str_fields = new ArrayList<FieldSchema>();
    // rules on how to recurse the ObjectInspector based on its type
    if (oi.getCategory() != Category.STRUCT) {
      str_fields.add(new FieldSchema(last_name, oi.getTypeName(),
          FROM_SERIALIZER));
    } else {
      List<? extends StructField> fields = ((StructObjectInspector) oi)
          .getAllStructFieldRefs();
      for (int i = 0; i < fields.size(); i++) {
        StructField structField = fields.get(i);
        String fieldName = structField.getFieldName();
        String fieldTypeName = structField.getFieldObjectInspector().getTypeName();
        String fieldComment = determineFieldComment(structField.getFieldComment());

        str_fields.add(new FieldSchema(fieldName, fieldTypeName, fieldComment));
      }
    }
    return str_fields;
  }

  private static final String FROM_SERIALIZER = "from deserializer";
  private static String determineFieldComment(String comment) {
    return (comment == null) ? FROM_SERIALIZER : comment;
  }

  /**
   * Convert TypeInfo to FieldSchema.
   */
  public static FieldSchema getFieldSchemaFromTypeInfo(String fieldName,
      TypeInfo typeInfo) {
    return new FieldSchema(fieldName, typeInfo.getTypeName(),
        "generated by TypeInfoUtils.getFieldSchemaFromTypeInfo");
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

  /**
   * Determines whether a table is an immutable table.
   * Immutable tables are write-once/replace, and do not support append. Partitioned
   * immutable tables do support additions by way of creation of new partitions, but
   * do not allow the partitions themselves to be appended to. "INSERT INTO" will not
   * work for Immutable tables.
   *
   * @param table table of interest
   *
   * @return true if immutable
   */
  public static boolean isImmutableTable(Table table) {
    if (table == null){
      return false;
    }
    Map<String, String> params = table.getParameters();
    if (params == null) {
      return false;
    }

    return "TRUE".equalsIgnoreCase(params.get(hive_metastoreConstants.IS_IMMUTABLE));
  }

  public static boolean isArchived(
      org.apache.hadoop.hive.metastore.api.Partition part) {
    Map<String, String> params = part.getParameters();
    return "TRUE".equalsIgnoreCase(params.get(hive_metastoreConstants.IS_ARCHIVED));
  }

  public static Path getOriginalLocation(
      org.apache.hadoop.hive.metastore.api.Partition part) {
    Map<String, String> params = part.getParameters();
    assert(isArchived(part));
    String originalLocation = params.get(hive_metastoreConstants.ORIGINAL_LOCATION);
    assert( originalLocation != null);

    return new Path(originalLocation);
  }

  public static boolean isNonNativeTable(Table table) {
    if (table == null || table.getParameters() == null) {
      return false;
    }
    return (table.getParameters().get(hive_metastoreConstants.META_TABLE_STORAGE) != null);
  }

  /**
   * Filter that filters out hidden files
   */
  private static final PathFilter hiddenFileFilter = new PathFilter() {
    @Override
    public boolean accept(Path p) {
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

  /**
   * Utility method that determines if a specified directory already has
   * contents (non-hidden files) or not - useful to determine if an
   * immutable table already has contents, for example.
   *
   * @param path
   * @throws IOException
   */
  public static boolean isDirEmpty(FileSystem fs, Path path) throws IOException {

    if (fs.exists(path)) {
      FileStatus[] status = fs.globStatus(new Path(path, "*"), hiddenFileFilter);
      if (status.length > 0) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns true if partial has the same values as full for all values that
   * aren't empty in partial.
   */

  public static boolean pvalMatches(List<String> partial, List<String> full) {
    if(partial.size() > full.size()) {
      return false;
    }
    Iterator<String> p = partial.iterator();
    Iterator<String> f = full.iterator();

    while(p.hasNext()) {
      String pval = p.next();
      String fval = f.next();

      if (pval.length() != 0 && !pval.equals(fval)) {
        return false;
      }
    }
    return true;
  }

  public static String getIndexTableName(String dbName, String baseTblName, String indexName) {
    return dbName + "__" + baseTblName + "_" + indexName + "__";
  }

  public static boolean isIndexTable(Table table) {
    if (table == null) {
      return false;
    }
    return TableType.INDEX_TABLE.toString().equals(table.getTableType());
  }

  public static boolean isMaterializedViewTable(Table table) {
    if (table == null) {
      return false;
    }
    return TableType.MATERIALIZED_VIEW.toString().equals(table.getTableType());
  }

  /**
   * Given a map of partition column names to values, this creates a filter
   * string that can be used to call the *byFilter methods
   * @param m
   * @return the filter string
   */
  public static String makeFilterStringFromMap(Map<String, String> m) {
    StringBuilder filter = new StringBuilder();
    for (Entry<String, String> e : m.entrySet()) {
      String col = e.getKey();
      String val = e.getValue();
      if (filter.length() == 0) {
        filter.append(col + "=\"" + val + "\"");
      } else {
        filter.append(" and " + col + "=\"" + val + "\"");
      }
    }
    return filter.toString();
  }

  public static boolean isView(Table table) {
    if (table == null) {
      return false;
    }
    return TableType.VIRTUAL_VIEW.toString().equals(table.getTableType());
  }

  /**
   * create listener instances as per the configuration.
   *
   * @param clazz
   * @param conf
   * @param listenerImplList
   * @return
   * @throws MetaException
   */
  static <T> List<T> getMetaStoreListeners(Class<T> clazz,
      HiveConf conf, String listenerImplList) throws MetaException {
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
        throw new MetaException("Failed to instantiate listener named: "+
            listenerImpl + ", reason: " + ie.getCause());
      } catch (Exception e) {
        throw new MetaException("Failed to instantiate listener named: "+
            listenerImpl + ", reason: " + e);
      }
    }

    return listeners;
  }

  @SuppressWarnings("unchecked")
  public static Class<? extends RawStore> getClass(String rawStoreClassName)
      throws MetaException {
    try {
      return (Class<? extends RawStore>)
          Class.forName(rawStoreClassName, true, JavaUtils.getClassLoader());
    } catch (ClassNotFoundException e) {
      throw new MetaException(rawStoreClassName + " class not found");
    }
  }

  /**
   * Create an object of the given class.
   * @param theClass
   * @param parameterTypes
   *          an array of parameterTypes for the constructor
   * @param initargs
   *          the list of arguments for the constructor
   */
  public static <T> T newInstance(Class<T> theClass, Class<?>[] parameterTypes,
      Object[] initargs) {
    // Perform some sanity checks on the arguments.
    if (parameterTypes.length != initargs.length) {
      throw new IllegalArgumentException(
          "Number of constructor parameter types doesn't match number of arguments");
    }
    for (int i = 0; i < parameterTypes.length; i++) {
      Class<?> clazz = parameterTypes[i];
      if (initargs[i] != null && !(clazz.isInstance(initargs[i]))) {
        throw new IllegalArgumentException("Object : " + initargs[i]
            + " is not an instance of " + clazz);
      }
    }

    try {
      Constructor<T> meth = theClass.getDeclaredConstructor(parameterTypes);
      meth.setAccessible(true);
      return meth.newInstance(initargs);
    } catch (Exception e) {
      throw new RuntimeException("Unable to instantiate " + theClass.getName(), e);
    }
  }

  public static void validatePartitionNameCharacters(List<String> partVals,
      Pattern partitionValidationPattern) throws MetaException {

    String invalidPartitionVal =
        HiveStringUtils.getPartitionValWithInvalidCharacter(partVals, partitionValidationPattern);
    if (invalidPartitionVal != null) {
      throw new MetaException("Partition value '" + invalidPartitionVal +
          "' contains a character " + "not matched by whitelist pattern '" +
          partitionValidationPattern.toString() + "'.  " + "(configure with " +
          HiveConf.ConfVars.METASTORE_PARTITION_NAME_WHITELIST_PATTERN.varname + ")");
      }
  }

  public static boolean partitionNameHasValidCharacters(List<String> partVals,
      Pattern partitionValidationPattern) {
    return HiveStringUtils.getPartitionValWithInvalidCharacter(partVals, partitionValidationPattern) == null;
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

  /**
   * Read and return the meta store Sasl configuration. Currently it uses the default
   * Hadoop SASL configuration and can be configured using "hadoop.rpc.protection"
   * HADOOP-10211, made a backward incompatible change due to which this call doesn't
   * work with Hadoop 2.4.0 and later.
   * @param conf
   * @return The SASL configuration
   */
  public static Map<String, String> getMetaStoreSaslProperties(HiveConf conf, boolean useSSL) {
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
    return ShimLoader.getHadoopThriftAuthBridge().getHadoopSaslProperties(conf);
  }


  public static String ARCHIVING_LEVEL = "archiving_level";
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

  public static String[] getQualifiedName(String defaultDbName, String tableName) {
    String[] names = tableName.split("\\.");
    if (names.length == 1) {
      return new String[] { defaultDbName, tableName};
    }
    return names;
  }

  /**
   * Helper function to transform Nulls to empty strings.
   */
  private static final com.google.common.base.Function<String,String> transFormNullsToEmptyString
      = new com.google.common.base.Function<String, String>() {
    @Override
    public java.lang.String apply(@Nullable java.lang.String string) {
      return StringUtils.defaultString(string);
    }
  };

  /**
   * We have aneed to sanity-check the map before conversion from persisted objects to
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
   * Add new elements to the classpath.
   *
   * @param newPaths
   *          Array of classpath elements
   */
  public static ClassLoader addToClassPath(ClassLoader cloader, String[] newPaths) throws Exception {
    URLClassLoader loader = (URLClassLoader) cloader;
    List<URL> curPath = Arrays.asList(loader.getURLs());
    ArrayList<URL> newPath = new ArrayList<URL>(curPath.size());

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
        // If statsObjOld is found, we can merge.
        ColumnStatsMerger merger = ColumnStatsMergerFactory.getColumnStatsMerger(statsObjNew,
            statsObjOld);
        merger.merge(statsObjNew, statsObjOld);
      }
      list.add(statsObjNew);
    }
    csNew.setStatsObj(list);
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

  public static List<String> getColumnNames(List<FieldSchema> schema) {
    List<String> cols = new ArrayList<>(schema.size());
    for (FieldSchema fs : schema) {
      cols.add(fs.getName());
    }
    return cols;
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
    return aggrPartitionStats(map,dbName,tableName,partNames,colNames,useDensityFunctionForNDVEstimation, ndvTuner);
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
    for (final Entry<String, List<ColumnStatistics>> entry : map.entrySet()) {
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
        for (Entry<String, String> param : params.entrySet()) {
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
          sortedOuterList.add(StringUtils.join(sortedInnerList, "."));
        }
        for (String colval : sortedOuterList) md.update(colval.getBytes(ENCODING));
      }
      if (skewed.getSkewedColValueLocationMaps() != null) {
        SortedMap<String, String> sortedMap = new TreeMap<>();
        for (Entry<List<String>, String> smap : skewed.getSkewedColValueLocationMaps().entrySet()) {
          SortedSet<String> sortedKey = new TreeSet<>(smap.getKey());
          sortedMap.put(StringUtils.join(sortedKey, "."), smap.getValue());
        }
        for (Entry<String, String> e : sortedMap.entrySet()) {
          md.update(e.getKey().getBytes(ENCODING));
          md.update(e.getValue().getBytes(ENCODING));
        }
      }
      md.update(sd.isStoredAsSubDirectories() ? "true".getBytes(ENCODING) : "false".getBytes(ENCODING));
    }

    return md.digest();
  }

  public static double decimalToDouble(Decimal decimal) {
    return new BigDecimal(new BigInteger(decimal.getUnscaled()), decimal.getScale()).doubleValue();
  }
}
