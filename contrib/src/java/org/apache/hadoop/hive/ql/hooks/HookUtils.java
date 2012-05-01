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

package org.apache.hadoop.hive.ql.hooks;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.hive.ql.hooks.conf.FBHiveConf;

/**
 * Utilities for writing hooks.
 */
public class HookUtils {
  static final private Log LOG = LogFactory.getLog(HookUtils.class.getName());

  public static final String TABLE_CREATION_CLUSTER = "creation_cluster";

  static final String POST_HOOK_DB_MAX_RETRY_VAR_NAME =
    "fbhive.posthook.mysql.max_retries";

  // The default value is to retry 20 times with maximum retry interval
  // 60 seconds. The expectation is about 22 minutes. After 7 retries, it
  // reaches 60 seconds.
  static final int DEFAULT_SQL_NUM_RETRIES = 20;
  static final int DEFAULT_RETRY_MAX_INTERVAL_SEC = 60;

  private static Connection getConnection(HiveConf conf, String url)
    throws SQLException {
    return DriverManager.getConnection(
      url,
      conf.get(FBHiveConf.FBHIVE_DB_USERNAME),
      conf.get(FBHiveConf.FBHIVE_DB_PASSWORD));
  }

  public static int getSqlNumRetry(HiveConf conf) {
    return conf.getInt(POST_HOOK_DB_MAX_RETRY_VAR_NAME, 30);
  }

  public static void runInsert(HiveConf conf,
                               ConnectionUrlFactory urlFactory,
                               String sql,
                               List<Object> sqlParams) throws Exception {
    runInsert(conf, urlFactory, sql, sqlParams, DEFAULT_SQL_NUM_RETRIES);
  }

  public static List<List<Object>> runInsertSelect(HiveConf conf,
                                                   ConnectionUrlFactory urlFactory,
                                                   String sql,
                                                   List<Object> sqlParams)
    throws Exception {
    return runInsertSelect(conf, urlFactory, sql, sqlParams, true);

  }

  public static List<List<Object>> runInsertSelect(HiveConf conf,
                                                   ConnectionUrlFactory urlFactory,
                                                   String sql,
                                                   List<Object> sqlParams,
                                                   boolean isWrite)
    throws Exception {
    return runInsertSelect(conf, urlFactory, sql, sqlParams, isWrite,
                           DEFAULT_SQL_NUM_RETRIES,
                           DEFAULT_RETRY_MAX_INTERVAL_SEC, false);
  }

  public static void runInsert(HiveConf conf,
                               ConnectionUrlFactory urlFactory,
                               String sql,
                               List<Object> sqlParams,
                               int numRetries)
      throws Exception {
    runInsertSelect(conf, urlFactory, sql, sqlParams, true, numRetries,
                    DEFAULT_RETRY_MAX_INTERVAL_SEC, true);
  }

  /*
   * @param conf -
   * @param parentTierName - the factory to create
   * @param tierName - the factory to create
   * @param tierParam1Name - the name of the first parameter
   * @param tierParam2Name - the name of the second parameter
   */
  public static ConnectionUrlFactory getUrlFactory(
      HiveConf conf,
      String parentTierName,
      String childTierName,
      String tierParam1Name,
      String tierParam2Name) {
    return getUrlFactory(conf, parentTierName, childTierName, tierParam1Name, tierParam2Name, null);
  }

  /*
   * @param conf -
   * @param parentTierName - the factory to create
   * @param tierName - the factory to create
   * @param tierParam1Name - the name of the first parameter
   * @param tierParam2Name - the name of the second parameter
   */
  public static ConnectionUrlFactory getUrlFactory(
        Configuration conf,
        String parentTierName,
        String childTierName,
        String tierParam1Name,
        String tierParam2Name,
        String commonParam) {

    String parentTierValue =
      parentTierName == null ? null : conf.get(parentTierName);
    String childTierValue =
      childTierName == null ? null : conf.get(childTierName);

    String tierValue =
      childTierValue != null && !childTierValue.isEmpty() ? childTierValue :
      (parentTierValue != null && !parentTierValue.isEmpty() ? parentTierValue :
         null);

    if (tierValue == null) {
      return null;
    }

    ConnectionUrlFactory conn =
      (ConnectionUrlFactory)getObject(conf, tierValue);

    String tierParamValue =
      tierParam1Name == null ? null : conf.get(tierParam1Name);

    if ((tierParamValue == null) || tierParamValue.isEmpty()) {
      tierParamValue = tierParam2Name == null ? null : conf.get(tierParam2Name);
    }

    String commonParamValue =
      commonParam == null ? null : conf.get(commonParam);

    conn.init(tierParamValue, commonParamValue);
    return conn;
  }

  // In the case of a select returns a list of lists, where each inner list represents a row
  // returned by the query.  In the case of an insert, returns null.
  public static List<List<Object>> runInsertSelect(
    HiveConf conf,
    ConnectionUrlFactory urlFactory, String sql,
    List<Object> sqlParams, boolean isWrite, int numRetries,
    int retryMaxInternalSec, boolean insert)
      throws Exception {

    // throwing an exception
    int waitMS = 300; // wait for at least 300 msec before next retry.
    Random rand = new Random();
    for (int i = 0; i < numRetries; ++i) {
      try {
        String url = urlFactory.getUrl(isWrite);
        LOG.info("Attepting connection with URL " + url);
        Connection conn = getConnection(conf, url);
        PreparedStatement pstmt = conn.prepareStatement(sql);
        int pos = 1;
        for (Object param : sqlParams) {
          if (param instanceof Integer) {
            pstmt.setInt(pos++, ((Integer) param).intValue());
          } else {
            pstmt.setString(pos++, (String) param);
          }
        }
        if (insert) {
          int recordsUpdated = pstmt.executeUpdate();
          LOG.info("rows inserted: " + recordsUpdated + " sql: " + sql);
          pstmt.close();
          return null;
        }
        else {
          ResultSet result = pstmt.executeQuery();
          List<List<Object>> results = new ArrayList<List<Object>>();
          int numColumns = result.getMetaData().getColumnCount();
          while (result.next()) {
            List<Object> row = new ArrayList<Object>();
            results.add(row);
            for (int index = 1; index <= numColumns; index++) {
              row.add(result.getObject(index));
            }
          }
          pstmt.clearBatch();
          pstmt.close();

          LOG.info("rows selected: " + results.size() + " sql: " + sql);
          return results;
        }
      } catch (Exception e) {
        // We should catch a better exception than Exception, but since
        // ConnectionUrlFactory.getUrl() defines throws Exception, it's hard
        // for us to figure out the complete set it can throw. We follow
        // ConnectionUrlFactory.getUrl()'s definition to catch Exception.
        // It shouldn't be a big problem as after numRetries, we anyway exit.
        LOG.info("Exception " + e + ". Will retry " + (numRetries - i)
            + " times.");
        // Introducing a random factor to the wait time before another retry.
        // The wait time is dependent on # of failures and a random factor.
        // At the first time of getting a SQLException, the wait time
        // is a random number between [0,300] msec. If the first retry
        // still fails, we will wait 300 msec grace period before the 2nd retry.
        // Also at the second retry, the waiting window is expanded to 600 msec
        // alleviating the request rate from the server. Similarly the 3rd retry
        // will wait 600 msec grace period before retry and the waiting window
        // is
        // expanded to 1200 msec.

        waitMS += waitMS;
        if (waitMS > retryMaxInternalSec * 1000) {
          waitMS = retryMaxInternalSec * 1000;
        }
        double waitTime = waitMS + waitMS * rand.nextDouble();
        Thread.sleep((long) waitTime);
        if (i + 1 == numRetries) {
          LOG.error("Still got Exception after " + numRetries + "  retries.",
              e);
          throw e;
        }
      }
    }
    return null;
  }

  /**
   * Populates inputToCS with a mapping from the input paths to their respective ContentSummary
   * objects.  If an input is in a subdirectory of another's location, or in the same location,
   * the input is not included in the total size of the inputs.  If it is not already present in
   * the mapping, it will not be added.
   *
   * @param inputs
   * @param inputToCS
   * @param conf
   * @throws IOException
   * @throws Exception
   */
  public static long getInputSize(Set<ReadEntity> inputs,
      Map<String, ContentSummary> inputToCS, HiveConf conf)
    throws IOException, Exception {

    URI defaultPathUri = new URI(conf.getVar(HiveConf.ConfVars.METASTOREWAREHOUSE));
    String defaultPath = defaultPathUri.getPath();
    String defaultPrefix =
      defaultPathUri.toString().substring(0, defaultPathUri.toString().lastIndexOf(defaultPath));

    // A mapping from the location as a String, formatted as a String for sorting, to the original
    // path of the object
    Map<String, Path> locationToPath = new HashMap<String, Path>();

    for (ReadEntity re : inputs) {
      Path p = null;
      switch (re.getType()) {
        case TABLE:
          Table table = re.getTable();

          if (table.isPartitioned()) {
            // If the input is a partitioned table, do not include its content summary, as data will
            // never be read from a partitioned table, only its partitions, so it must be a metadata
            // change to the table.
            continue;
          }
          if (table.isView()) {
            // If the input is a view, it does not have a content summary as it is only a logical
            // construct.
            continue;
          }

          p = table.getPath();
          break;
        case PARTITION:
          Partition partition = re.getPartition();

          if (partition.getTable().isView()) {
            // If the input is a partition of a view, it does not have a content summary as it is
            // only a logical construct.
            continue;
          }

          p = partition.getPartitionPath();
          break;
        default:
          continue;
      }

      String location = re.getLocation().toString();

      // If the location is something like /user/facebook/warehouse/ we want it to start with
      // hdfs://... to make ensure using prefixes we can identify subdirectories
      if (location.equals(defaultPath) ||
          location.startsWith(defaultPath.endsWith("/") ? defaultPath : defaultPath + "/")) {
        location = defaultPrefix + location;
      }

      // If the location does not end with / add it, this ensures /a/b/cd is not considered a
      // subdirectory of /a/b/c
      if (!location.endsWith("/")) {
        location += "/";
      }

      locationToPath.put(location, p);
    }

    String[] locations = new String[locationToPath.size()];
    locations = locationToPath.keySet().toArray(locations);
    Arrays.sort(locations);

    String lastLocation = null;
    long totalInputSize = 0;
    for (String formattedLocation : locations) {

      // Since the locations have been sorted, if this location is a subdirectory of another, that
      // directory must either be immediately before this location, or every location in between is
      // also a subdirectory
      if (lastLocation != null && formattedLocation.startsWith(lastLocation)) {
        continue;
      }

      Path p = locationToPath.get(formattedLocation);
      lastLocation = formattedLocation;

      String pathStr = p.toString();
      if(LOG.isDebugEnabled()) {
        LOG.debug("Finding from cache Content Summary for " + pathStr);
      }

      ContentSummary cs = (inputToCS == null) ? null : inputToCS.get(pathStr);
      if (cs == null) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("Fetch Content Summary for " + pathStr);
        }
        FileSystem fs = p.getFileSystem(conf);
        cs = fs.getContentSummary(p);
        inputToCS.put(pathStr, cs);
      }

      totalInputSize += cs.getLength();

      if (LOG.isDebugEnabled()) {
        LOG.debug("Length for file: " + pathStr + " = " + cs.getLength());
      }
    }

    return totalInputSize;
  }

  /**
   * Goes through the list of tasks, and populates a map from each path used
   * by a mapRedTask to the highest percentage to which it is sampled, or 100
   * if it is ever not sampled.
   *
   * Also, if a task is not a map reduce task or has a null or empty
   * NameToSplitSample map, it adds all of its inputs to a
   * set so they can be treated as unsampled.
   *
   * Calls itself recursively on each task's list of dependent tasks
   *
   * @return whether or not there is any sampling performed in the query
   */
  static public boolean checkForSamplingTasks(
      List<Task<? extends Serializable>> tasks,
      Map<String, Double> topPercentages,
      Set<ReadEntity> nonSampledInputs) {
    boolean isThereSampling = false;

    for (Task<? extends Serializable> task : tasks) {
      MapredWork work;

      // Only look for sampled inputs in MapRedTasks with non-null, non-empty
      // NameToSplitSample maps
      if (task.getWork() instanceof MapredWork &&
          (work = (MapredWork)task.getWork()).getNameToSplitSample() != null &&
          !work.getNameToSplitSample().isEmpty()) {

        isThereSampling = true;

        // If the task is a map reduce task, go through each of the paths
        // used by its work, if it is sampled check if it is the highest
        // sampling percentage yet seen for that path.  If it is not
        // sampled, set the highest percentage to 100.
        for (Map.Entry<String, ArrayList<String>> entry : work.getPathToAliases().entrySet()) {
          double percentage = 0;

          for (String alias : entry.getValue()) {
            if (work.getNameToSplitSample().containsKey(alias)) {
              if (work.getNameToSplitSample().get(alias).getPercent() > percentage) {
                percentage = work.getNameToSplitSample().get(alias).getPercent();
              }
            } else {
              percentage = 100;
              break;
            }
          }

          String path = entry.getKey();
          if (!topPercentages.containsKey(path) || percentage > topPercentages.get(path)) {
            topPercentages.put(path, percentage);
          }
        }
      } else if (task.getQueryPlan() != null) {
        nonSampledInputs.addAll(task.getQueryPlan().getInputs());
      }

      if (task.getDependentTasks() != null) {
        isThereSampling |= checkForSamplingTasks(task.getDependentTasks(),
            topPercentages,
            nonSampledInputs);
      }
    }

    return isThereSampling;
  }

  /**
   * Helper class used to pass from getObjectSize back to the caller.
   * This contains the total size of the objects passed in, as well as
   * the type, size and number of files for each object. For eg, if a query
   * references 2 partitions T1@p1 and T1@p2 of size 10 and 20, and 2 and 5
   * files respectively, the totalSize will be 30, and the object map will be
   * like:
   * T1@p1 -> <MANAGED_TABLE, 10, 2)
   * T1@p2 -> <MANAGED_TABLE, 20, 5)
   * Currently, this is logged in the job_stats_log table, and used for
   * downstream analysis.
   */
  public static class ObjectSize {
    long totalSize;
    Map<String, Triple<String, String, String>> objectTypeLengths;

    ObjectSize() {
    }

    ObjectSize(long totalSize,
               Map<String, Triple<String, String, String>> objectTypeLengths) {
      this.totalSize = totalSize;
      this.objectTypeLengths = objectTypeLengths;
    }

    long getTotalSize() {
      return totalSize;
    }

    void setTotalSize(long totalSize) {
      this.totalSize = totalSize;
    }

    Map<String, Triple<String, String, String>> getObjectTypeLengths() {
      return objectTypeLengths;
    }

    void setObjectTypeLengths(Map<String, Triple<String, String, String>> objectTypeLengths) {
      this.objectTypeLengths = objectTypeLengths;
    }
  }


  static public HookUtils.ObjectSize getObjectSize(HiveConf conf,
                                                   Set<Entity> objects,
                                                   boolean loadObjects)
    throws Exception {
    // The objects may need to be loaded again since StatsTask is executed after
    // the move task, and the object in the write entity may not have the size
    long totalSize = 0;
    Map<String, Triple<String, String, String>> objectLengths =
      new HashMap<String, Triple<String, String, String>>();
    Hive db = null;
    if (loadObjects) {
      db = Hive.get();
    }

    for (Entity object: objects) {
      // We are computing sizes only for tables and partitions
      Entity.Type objectType = object.getTyp();
      Table table = null;
      String size = null;
      String numFiles = null;
      Path path = null;

      switch (objectType) {
        case TABLE:
          table = object.getTable();

          if (table.isPartitioned() && !table.isView()) {
            // If the input is a partitioned table, do not include its content summary, as data will
            // never be read from a partitioned table, only its partitions, so it must be a metadata
            // change to the table.
            //
            // However, if the table is a view, a view's partitions are not included in the inputs,
            // so do not skip it so that we have some record of it.
            continue;
          }

          if (loadObjects) {
            table = db.getTable(table.getTableName());
          }

          if (table.isView()) {
            // Views are logical, so they have no size or files
            path = null;
            size = "0";
            numFiles = "0";
          } else {
            path = table.getPath();
            size = table.getProperty("totalSize");
            numFiles = table.getProperty("numFiles");
          }
          break;
        case PARTITION:
          Partition partition = object.getPartition();

          if (loadObjects) {
            partition =
              db.getPartition(partition.getTable(), partition.getSpec(), false);
          }
          table = partition.getTable();

          if (table.isView()) {
            // Views are logical, so they have no size or files
            // Currently view partitions are not included in the inputs, but this is included so
            // that if that changes in open source, it will not cause an NPE.  It should not cause
            // any double counting as the size of the view and its partitions are both 0.
            path = null;
            size = "0";
            numFiles = "0";
          } else {
            path = partition.getPartitionPath();
            size = partition.getParameters().get("totalSize");
            numFiles = partition.getParameters().get("numFiles");
          }
          break;
        default:
          // nothing to do
          break;
      }

      // Counting needed
      if (table != null) {
          if (size == null) {
            // If the size is not present in the metastore (old
            // legacy tables), get it from hdfs
            FileSystem fs = path.getFileSystem(conf);
            size = String.valueOf(fs.getContentSummary(path).getLength());
          }

          if (numFiles == null) {
            numFiles = String.valueOf(0);
          }

          Triple<String, String, String> triple =
            new Triple<String, String, String>(
              table.getTableType().toString(), size, numFiles);

          objectLengths.put(object.getName(), triple);

          // If the input/output is a external table or a view, dont add it to
          // the total size. The managed tables/partitions for those locations
          // should also be part of the object list passed in. It is true for
          // inputs, whereas outputs should not external tables or views in a
          // query. So, the totalSize may be less than the sum of all individual
          // sizes
          if ((table.getTableType() != TableType.EXTERNAL_TABLE) &&
              (table.getTableType() != TableType.VIRTUAL_VIEW)) {
            totalSize += Long.valueOf(size);
          }
      }
    }

    ObjectSize objectSize = new ObjectSize(totalSize, objectLengths);
    return objectSize;
  }

  /**
   * A helper class used to pass info from getInputInfo back to the caller.
   */
  public static class InputInfo {
    long size;
    long fileCount;
    long directoryCount;
    double estimatedNumSplits;

    InputInfo(long size, long fileCount, long directoryCount,
        double estimatedNumSplits) {
      this.size = size;
      this.fileCount = fileCount;
      this.directoryCount = directoryCount;
      this.estimatedNumSplits = estimatedNumSplits;
    }

    long getSize() {
      return size;
    }

    long getFileCount() {
      return fileCount;
    }

    long getDirectoryCount() {
      return directoryCount;
    }

    double getEstimatedNumSplits() {
      return estimatedNumSplits;
    }
  }

  /**
   * Returns the sizes of the inputs while taking sampling into account.
   *
   * @param inputs - entities used for the query input
   * @param inputToCS - already known mappings from paths to content summaries.
   * If a path is not in this mapping, it will be looked up
   * @param conf - hadoop conf for constructing filesystem
   * @param isThereSampling - whether the query includes sampling
   * @param pathToTopPercentage - a mapping from the path to the highest
   * sampled percentage. If not in the map, defaults to 100%
   * @param nonSampledInputs - entities that are not sampled
   * @param maxSplits - if the number of splits exceeds this number as the
   * splits are incrementally summed, return early
   * @param maxSize - if the size exceeds this number as the sizes are being
   * incrementally summed, return early
   * @return an InputInfo object about the net input
   * @throws IOException
   */

  static public InputInfo getInputInfo(Collection<ReadEntity> inputs,
      Map<String, ContentSummary> inputToCS, Configuration conf,
      boolean isThereSampling, Map<String, Double> pathToTopPercentage,
      Set<ReadEntity> nonSampledInputs,
      long maxSplits, long maxSize) throws IOException {

      double estimatedNumSplits = 0;
      long size = 0;
      long fileCount = 0;
      long directoryCount = 0;

    // Go over the input paths and calculate size
    for(ReadEntity re: inputs) {
      Path p = null;
      switch(re.getType()) {
      case TABLE:
        p = re.getTable().getPath();
        break;
      case PARTITION:
        p = re.getPartition().getPartitionPath();
        break;
      default:
        break;
      }

      if (p != null) {
        String pathStr = p.toString();
        LOG.debug("Finding from cache Content Summary for " + pathStr);
        ContentSummary cs = (inputToCS == null) ? null : inputToCS
            .get(pathStr);
        if (cs == null) {
          LOG.debug("Fetch Content Summary for " + pathStr);
          FileSystem fs = p.getFileSystem(conf);
          cs = fs.getContentSummary(p);
          inputToCS.put(pathStr, cs);
        }

        if (isThereSampling) {
          // If the input is used in a map reduce task get the highest
          // percentage to which it is sampled, otherwise, set the
          // sampling percentage to 100
          double samplePercentage = 100;
          if (pathToTopPercentage.containsKey(pathStr) &&
              !nonSampledInputs.contains(re)) {
            samplePercentage = pathToTopPercentage.get(pathStr);
          }
          size += (long)(cs.getLength() * samplePercentage / 100D);
          estimatedNumSplits += samplePercentage / 100;

          if (estimatedNumSplits > maxSplits) {
            break;
          }
        } else {
          size += cs.getLength();
          fileCount += cs.getFileCount();
          directoryCount += cs.getDirectoryCount();
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug ("Length for file: " + p.toString() + " = " + cs.getLength());
        }
      }

      if (size > maxSize){
        break;
      }
    }

    return new InputInfo(size, fileCount, directoryCount,
        estimatedNumSplits);
  }

  //Returns true approximately <percentageObj*100>% of the time
  public static boolean rollDice(float percentage) throws Exception {

    Random randGen = new Random();
    float randVal = randGen.nextFloat();

    if (percentage < 0 || percentage > 1) {
      throw new Exception("Percentages must be >=0% and <= 100%. Got " + percentage);
    }

    if (randVal < percentage) {
      return true;
    }

    return false;
  }

  public static <T> T getObject(Configuration conf, String className) {
    if ((className == null) || (className.isEmpty())) {
      return null;
    }

    T clazz = null;
    try {
      clazz = (T) ReflectionUtils.newInstance(conf.getClassByName(className), conf);
    } catch (ClassNotFoundException e) {
      return null;
    }
    return clazz;
  }
}
