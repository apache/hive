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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.MapRedStats;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.hooks.conf.FBHiveConf;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.HiveStatsMetricsPublisher;
import org.apache.hadoop.hive.ql.stats.HiveStatsMetricsPublisher.QueryTag;
import org.apache.hadoop.mapred.Counters;
import org.json.JSONObject;

/**
 * A hook which populates the job_stats_log MySQL table with
 * stats for each job which has run for this query, the query ID,
 * and whether or not the query succeeded.
 *
 * It also sets the query attributes in HiveStatsMetricsPublisher and logs
 * the stats through it as well.
 */
public class JobStatsHook implements ExecuteWithHookContext {

  public static final String HIVE_QUERY_SOURCE = "hive.query.source";

  public static ConnectionUrlFactory getJobStatsUrlFactory(HiveConf conf){
    return HookUtils.getUrlFactory(conf,
        FBHiveConf.CONNECTION_FACTORY,
        FBHiveConf.JOBSTATS_CONNECTION_FACTORY,
        FBHiveConf.JOBSTATS_MYSQL_TIER_VAR_NAME,
        FBHiveConf.JOBSTATS_HOST_DATABASE_VAR_NAME);
  }

  @Override
  public void run(HookContext hookContext) throws Exception {

    assert(hookContext.getHookType() == HookContext.HookType.POST_EXEC_HOOK ||
           hookContext.getHookType() == HookContext.HookType.ON_FAILURE_HOOK);

    String queryId = "";
    String querySrc = "";
    String queryTagsStr = "";
    String statsString = "";
    SessionState sess = SessionState.get();
    String queryFailed = hookContext.getHookType() == HookContext.HookType.ON_FAILURE_HOOK ? "1"
        : "0";
    HiveConf conf = sess.getConf();
    HiveStatsMetricsPublisher metricsPublisher =
      (HiveStatsMetricsPublisher)HookUtils.getObject(conf,
         conf.get(FBHiveConf.HIVE_METRICS_PUBLISHER));
    if (metricsPublisher == null) {
      return;
    }

    metricsPublisher.extractAndOverwriteQueryAttributes(hookContext);

    JSONObject jobStats = new JSONObject();

    ConnectionUrlFactory urlFactory = getJobStatsUrlFactory(conf);
    if (urlFactory == null) {
      throw new RuntimeException("DB parameters for audit_log not set!");
    }

    if (sess != null) {
      queryId = conf.getVar(HiveConf.ConfVars.HIVEQUERYID);
      querySrc = conf.get(HIVE_QUERY_SOURCE, "");

      List<TaskRunner> completedTasks = hookContext.getCompleteTaskList();
      Map<String, String> jobToStageMap = new HashMap<String, String>();

      if (completedTasks != null) {
        for (TaskRunner taskRunner : completedTasks) {
          Task<? extends Serializable> task = taskRunner.getTask();
          // If the Job ID is null, this indicates the task is not a map
          // reduce task, or it was run locally
          if (task.getJobID() != null) {
            String jobID = StringEscapeUtils.escapeJava(task.getJobID());
            String stageID = StringEscapeUtils.escapeJava(task.getId());
            jobToStageMap.put(jobID, stageID);
          }
        }
      }

      List<MapRedStats> listStats = sess.getLastMapRedStatsList();
      if (listStats != null && listStats.size() > 0) {
        Map[] perJobStats = new Map[listStats.size()];
        for (int i = 0; i < listStats.size(); i++) {
          MapRedStats mps = listStats.get(i);
          Counters ctrs = mps.getCounters();
          Map<String, String> counterList = new HashMap<String, String>();
          Map<String, Double> metrics = new HashMap<String,Double>();

          counterList.put("job_ID", mps.getJobId());

          if (jobToStageMap.containsKey(mps.getJobId())) {
            counterList.put("stage", jobToStageMap.get(mps.getJobId()));
          }

          addJobStat(counterList, metrics, "cpu_msec", "cpu_sec", mps.getCpuMSec(), 1000);
          addJobStat(counterList, metrics, "map", mps.getNumMap());
          addJobStat(counterList, metrics, "reduce", mps.getNumReduce());
          if (ctrs != null) {
            conditionalAddJobStat(counterList, metrics, "hdfs_read_bytes", "hdfs_read_mbytes",
                ctrs.findCounter("FileSystemCounters", "HDFS_BYTES_READ"), 1000000);
            conditionalAddJobStat(counterList, metrics, "hdfs_write_bytes", "hdfs_write_mbytes",
                ctrs.findCounter("FileSystemCounters", "HDFS_BYTES_WRITTEN"), 1000000);
            conditionalAddJobStat(counterList, metrics, "hdfs_local_read_bytes",
                "hdfs_read_local_mbytes", ctrs.findCounter("FileSystemCounters",
                "HDFS_BYTES_READ_LOCAL"), 1000000);
            conditionalAddJobStat(counterList, metrics, "hdfs_rack_read_bytes",
                "hdfs_rack_read_mbytes", ctrs.findCounter("FileSystemCounters",
                "HDFS_BYTES_READ_RACK"), 1000000);
            conditionalAddJobStat(counterList, metrics, "hdfs_read_exceptions",
                ctrs.findCounter("FileSystemCounters", "HDFS_READ_EXCEPTIONS"));
            conditionalAddJobStat(counterList, metrics,
                "hdfs_write_exceptions",
                ctrs.findCounter("FileSystemCounters", "HDFS_WRITE_EXCEPTIONS"));
            conditionalAddJobStat(counterList, metrics, "map_input_records",
                "map_input_million_records",
                ctrs.findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_RECORDS"),
                1000000);
            conditionalAddJobStat(counterList, metrics, "map_output_records",
                "map_output_million_records",
                ctrs.findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_RECORDS"),
                1000000);
            conditionalAddJobStat(counterList, metrics, "reduce_input_records",
                "reduce_input_million_records",
                ctrs.findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_INPUT_RECORDS"),
                1000000);
            conditionalAddJobStat(counterList, metrics, "reduce_output_records",
                "reduce_output_million_records",
                ctrs.findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS"),
                1000000);
            conditionalAddJobStat(counterList, metrics, "shuffle_bytes", "shuffle_mbytes",
                ctrs.findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_SHUFFLE_BYTES"),
                1000000);
            conditionalAddJobStat(counterList, metrics, "map_input_bytes", "map_input_mbytes",
                ctrs.findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_BYTES"),
                1000000);
            conditionalAddJobStat(counterList, metrics, "map_spill_cpu_msecs",
                "map_spill_cpu_secs",
                ctrs.findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_SPILL_CPU"),
                1000);
            conditionalAddJobStat(counterList, metrics, "map_spill_wallclock_msecs",
                "map_spill_walllclock_secs",
                ctrs.findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_SPILL_WALLCLOCK"),
                1000);
            conditionalAddJobStat(counterList, metrics, "map_spill_number", "map_spill_number",
                ctrs.findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_SPILL_NUMBER"), 1);
            conditionalAddJobStat(counterList, metrics, "map_spill_bytes", "map_spill_mbytes",
                ctrs.findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_SPILL_BYTES"),
                1000000);
            conditionalAddJobStat(counterList, metrics, "map_mem_sort_cpu_msecs",
                "map_mem_sort_cpu_secs",
                ctrs.findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_MEM_SORT_CPU"),
                1000);
            conditionalAddJobStat(counterList, metrics, "map_mem_sort_wallclock_msecs",
                "map_mem_sort_wallclock_secs",
                ctrs.findCounter("org.apache.hadoop.mapred.Task$Counter",
                                 "MAP_MEM_SORT_WALLCLOCK"),
                1000);
            conditionalAddJobStat(counterList, metrics, "map_merge_cpu_msecs",
                "map_merge_cpu_secs",
                ctrs.findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_MERGE_CPU"),
                1000);
            conditionalAddJobStat(counterList, metrics, "map_merge_wallclock_msecs",
                "map_merge_wallclock_secs",
                ctrs.findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_MERGE_WALLCLOCK"),
                1000);
            conditionalAddJobStat(counterList, metrics, "reduce_copy_cpu_msecs",
                "reduce_copy_cpu_secs",
                ctrs.findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_COPY_CPU"),
                1000);
            conditionalAddJobStat(counterList, metrics, "reduce_copy_wallclock_msecs",
                "reduce_copy_wallclock_secs",
                ctrs.findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_COPY_WALLCLOCK"),
                1000);
            conditionalAddJobStat(counterList, metrics, "reduce_sort_cpu_msecs",
                "reduce_sort_cpu_secs",
                ctrs.findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_SORT_CPU"),
                1000);
            conditionalAddJobStat(counterList, metrics, "redcue_sort_wallclock_msecs",
                "reduce_sort_wallclock_secs",
                ctrs.findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_SORT_WALLCLOCK"),
                1000);
            conditionalAddJobStat(counterList, metrics, "map_task_wallclock_msecs",
                "map_task_wallclock_secs",
                ctrs.findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_TASK_WALLCLOCK"),
            1000);
            conditionalAddJobStat(counterList, metrics, "reduce_task_wallclock_msecs",
                "reduce_task_wallclock_secs",
                ctrs.findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_TASK_WALLCLOCK"),
                1000);
            conditionalAddJobStat(counterList, metrics, "slots_millis_maps", "slots_secs_maps",
                ctrs.findCounter("org.apache.hadoop.mapred.JobInProgress$Counter",
                "SLOTS_MILLIS_MAPS"), 1000);
            conditionalAddJobStat(counterList, metrics, "slots_millis_reduces",
                "slots_secs_reduces", ctrs.findCounter(
                "org.apache.hadoop.mapred.JobInProgress$Counter", "SLOTS_MILLIS_REDUCES"),
                1000);
          }
          addJobStat(counterList, metrics, "success", mps.isSuccess() ? 1 : 0);
          perJobStats[i] = counterList;

          metricsPublisher.publishMetricsWithQueryTags(metrics);
        }
        jobStats.put("per_job_stats", perJobStats);
      }
    }

    HiveOperation op = sess.getHiveOperation();

    // If input was read, log the input and output size
    if ((op != null) &&
        ((op.equals(HiveOperation.CREATETABLE_AS_SELECT)) ||
         (op.equals(HiveOperation.LOAD)) ||
         (op.equals(HiveOperation.QUERY)))) {

      // We are depending on the stats to be present in the metastore.
      // If that is not true, we might end up calling getContentSummary for
      // all the inputs and outputs, which may create a problem for HDFS
      // Allow the user to manually turn it off.
      if (!conf.getBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
          if (SessionState.get().getOverriddenConfigurations().containsKey(
              HiveConf.ConfVars.HIVESTATSAUTOGATHER.varname)) {
            SessionState.getConsole().printInfo("WARNING: hive.stats.autogather is set to false." +
                "  Stats were not populated for any outputs of this query.  If any tables or " +
                "partitions were overwritten as part of this query, their stats may be incorrect");
          } else {
            throw new RuntimeException("hive.stats.autogather is set to false");
          }
      }

      // Log the total size and the individual sizes for each input and output
      HookUtils.ObjectSize inputSizes =
        HookUtils.getObjectSize(conf,
                                new HashSet<Entity>(hookContext.getInputs()),
                                false);
      jobStats.put("input_size", String.valueOf(inputSizes.getTotalSize()));
      if (!inputSizes.getObjectTypeLengths().isEmpty()) {
        jobStats.put("inputs", inputSizes.getObjectTypeLengths());
      }

      // Log the pool specified in the conf. May be overwritten by the conf
      // if we enable the feature on the JT to disallow non-standard pools.
      String specifiedPool = conf.get("mapred.fairscheduler.pool", "");
      if (specifiedPool.length() > 0) {
        jobStats.put("pool", conf.get("mapred.fairscheduler.pool"));
      }

      if (hookContext.getHookType() != HookContext.HookType.ON_FAILURE_HOOK) {
        // The object for the outputs was created before the statistics in that
        // object was populated. So, reload the outputs from the metastore to get
        // the size for outputs
        HookUtils.ObjectSize outputSizes =
          HookUtils.getObjectSize(conf,
                                  new HashSet<Entity>(hookContext.getOutputs()),
                                  true);
        jobStats.put("output_size", String.valueOf(outputSizes.getTotalSize()));
        if (!outputSizes.getObjectTypeLengths().isEmpty()) {
          jobStats.put("outputs", outputSizes.getObjectTypeLengths());
        }
      }
    }

    statsString = jobStats.toString();

    Set<QueryTag> queryTags = metricsPublisher.getQueryAttributes();
    queryTagsStr = StringUtils.join(queryTags, ',');

    List<Object> sqlParams = new ArrayList<Object>();
    sqlParams.add(StringEscapeUtils.escapeJava(queryId));
    sqlParams.add(StringEscapeUtils.escapeJava(querySrc));
    sqlParams.add(queryFailed);
    sqlParams.add(queryTagsStr);
    sqlParams.add(statsString);

    // Assertion at beginning of method guarantees this string will not remain empty
    String sql = "insert into job_stats_log set queryId = ?, query_src = ?, query_failed = ?, " +
                 "query_tags = ?, job_stats = ?";

    HookUtils.runInsert(conf, urlFactory, sql, sqlParams, HookUtils
        .getSqlNumRetry(conf));
  }

  private void conditionalAddJobStat(Map<String, String> counterList, Map<String, Double> metrics,
      String key, Counters.Counter cntr) {
      conditionalAddJobStat(counterList, metrics, key, key, cntr, 1);
  }

  private void conditionalAddJobStat(Map<String, String> counterList, Map<String, Double> metrics,
      String exactKey, String approximateKey, Counters.Counter cntr, int divisor) {
    if (cntr != null) {
      conditionalAddJobStat(counterList, metrics, exactKey,
                            approximateKey, cntr.getValue(), divisor);
    }
  }

  private void conditionalAddJobStat(Map<String, String> counterList, Map<String, Double> metrics,
      String exactKey, String approximateKey, long cntrValue, int divisor) {
    if (cntrValue >= 0) {
      addJobStat(counterList, metrics, exactKey, approximateKey, cntrValue, divisor);
    }
  }

  private void addJobStat(Map<String, String> counterList, Map<String, Double> metrics,
                          String key, long value) {
    addJobStat(counterList, metrics, key, key, value, 1);
  }

  // Method that adds a key value pair to a map, as well as to a list of OdsKeyValuePairs
  // with average aggregation
  private void addJobStat(Map<String, String> counterList, Map<String, Double> metrics,
      String exactKey,
      String approximatedKey, long value, int divisor) {
    counterList.put(exactKey, String.valueOf(value));
    metrics.put(approximatedKey, (double)value/divisor);
  }
}
