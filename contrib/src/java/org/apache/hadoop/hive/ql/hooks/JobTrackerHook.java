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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.DDLTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.HookUtils.InputInfo;
import org.apache.hadoop.hive.ql.hooks.conf.FBHiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Implementation of a pre execute hook that decides what
 * cluster to send a given query to based on the size of
 * query inputs
 *
 * TODO: this needs to be optimized once HIVE-1507 is in
 * place to reuse the patch->summary cache maintained in hive
 *
 * TODO: this encodes hadoop cluster info in code. very
 * undesirable. Need to figure this out better (SMC?)
 */
public class JobTrackerHook {

  static final private Log LOG = LogFactory.getLog(JobTrackerHook.class.getName());

  // store the prior location of the hadoop executable. switching this doesn't
  // matter unless we are using the 'submit via child' feature
  private static String preHadoopBin = null;

  private static String preJobTracker = null;

  private static Map<String, String> savedValues = null;

  public static class PreExec implements ExecuteWithHookContext {

    private final String dislike = "Not choosing Bronze/Corona because ";

    static final private String POOLS = "pools";

    /**
     * If the job is on an SLA pool, do not redirect this job.
     *
     * @return True if the pool matches an SLA pool, false otherwise
     */
    private boolean isOnSlaPool(HiveConf conf) {
      String pool = conf.get("mapred.fairscheduler.pool");

      // Nothing to be done if pool is not specified
      if ((pool == null) || (pool.isEmpty())) {
        return false;
      }

      // Make sure that SLA jobs are not redirected
      String[] slaPoolArray =
        conf.getStrings("mapred.jobtracker.hook.sla.pools");
      if ((slaPoolArray == null) || (slaPoolArray.length == 0)) {
        slaPoolArray = new String[]{"rootsla", "incrementalscraping"};
      }
      for (int i = 0; i < slaPoolArray.length; ++i) {
        if (slaPoolArray[i].equals(pool)) {
          LOG.debug("Pool " + pool + " is on an sla pool");
          return true;
        }
      }

      LOG.debug("Pool " + pool + " is not on an sla pool");
      return false;
    }

    /*
     * The user has specified a mapping table in hive.configs, which is
     * essentially of the form: pool -> <cluster, hadoopHome, jobTracker>
     * Since, cluster will be repeated a lot in these scenarios, the exact
     * mapping is: cluster -> <hadoopHome, jobTracker, array of pools>
     * Going forward, multiple clusters will be used in these mappings, once
     * silver is broken into silver and silver2. No code changes will be
     * required, only configuration change.
     * @return Whether to use the cluster from the smc
     */
    private boolean useClusterFromSmcConfig(HiveConf conf) {
      try {
        String pool = conf.get("mapred.fairscheduler.pool");

        // Nothing to be done if pool is not specified
        if ((pool == null) || (pool.isEmpty())) {
          return false;
        }

        ConnectionUrlFactory connectionUrlFactory =
          HookUtils.getUrlFactory(conf,
              FBHiveConf.CONNECTION_FACTORY,
              FBHiveConf.JOBTRACKER_CONNECTION_FACTORY,
              FBHiveConf.JOBTRACKER_MYSQL_TIER_VAR_NAME,
              FBHiveConf.JOBTRACKER_HOST_DATABASE_VAR_NAME);

        if (connectionUrlFactory == null) {
          return false;
        }

        String s = connectionUrlFactory.getValue(conf.get(FBHiveConf.HIVE_CONFIG_TIER), POOLS);
        if (s == null) {
          return false;
        }

        JSONObject poolsJSON = new JSONObject(s);

        Iterator<String> i = (Iterator<String>) poolsJSON.keys();
        while(i.hasNext()) {

          String clusterName = i.next();
          JSONObject jo = (JSONObject)poolsJSON.get(clusterName);

          String hadoopHome  = null;
          String jobTracker  = null;
          JSONArray poolsObj = null;

          boolean isCorona = false;
          if (jo.has("isCorona")) {
            isCorona = jo.getBoolean("isCorona");
          }

          if (!jo.has("hadoopHome") || !jo.has("pools")) {
            LOG.error("hadoopHome and pools need to be specified for " +
              clusterName);
            return false;
          } else {
            hadoopHome = jo.getString("hadoopHome");
            poolsObj   = (JSONArray)jo.get("pools");
          }
          if (!isCorona && !jo.has("jobTracker")) {
            LOG.error(
              "jobTracker needs to be specified for non-corona cluster " +
              clusterName);
            return false;
          } else {
            if (jo.has("jobTracker")) {
              jobTracker = jo.getString("jobTracker");
            }
          }

          // Do the pool match
          for (int idx = 0; idx < poolsObj.length(); idx++) {
            if (pool.equals(poolsObj.getString(idx))) {

              LOG.info ("Run it on " + clusterName + " due to pool " + pool);

              if (isCorona) {
                // Parameters are taken from configuration.
                runCorona(conf, hadoopHome);
              } else {
                // Run it on "clusterName"
                preHadoopBin = conf.getVar(HiveConf.ConfVars.HADOOPBIN);
                conf.setVar(HiveConf.ConfVars.HADOOPBIN,
                            hadoopHome + "/bin/hadoop");
                preJobTracker = conf.getVar(HiveConf.ConfVars.HADOOPJT);
                conf.setVar(HiveConf.ConfVars.HADOOPJT, jobTracker);
              }

              return true;
            }
          }
        }

        // Found nothing
        return false;
      } catch (TException e) {
        return false;
      } catch (JSONException e) {
        return false;
      } catch (Exception e) {
        return false;
      }
    }

    @Override
    public void run(HookContext hookContext) throws Exception {
      assert(hookContext.getHookType() == HookContext.HookType.PRE_EXEC_HOOK);
      SessionState sess = SessionState.get();
      Set<ReadEntity> inputs = hookContext.getInputs();
      Set<WriteEntity> outputs = hookContext.getOutputs();
      UserGroupInformation ugi = hookContext.getUgi();
      Map<String, ContentSummary> inputToCS = hookContext.getInputPathToContentSummary();

      QueryPlan queryPlan = hookContext.getQueryPlan();
      List<Task<? extends Serializable>> rootTasks = queryPlan.getRootTasks();

      // If it is a pure DDL task,
      if (rootTasks == null) {
        return;
      }
      if (rootTasks.size() == 1) {
        Task<? extends Serializable> tsk = rootTasks.get(0);
        if (tsk instanceof DDLTask) {
          return;
        }
      }

      HiveConf conf = sess.getConf();

      // In case posthook of the previous query was not triggered,
      // we revert job tracker to clean state first.
      if (preHadoopBin != null) {
        conf.setVar(HiveConf.ConfVars.HADOOPBIN, preHadoopBin);
        preHadoopBin = null;
      }

      if (preJobTracker != null) {
        conf.setVar(HiveConf.ConfVars.HADOOPJT, preJobTracker);
        preJobTracker = null;
      }

      // A map from a path to the highest percentage that it is sampled by a
      // map reduce task.  If any map reduce task which uses this path does not
      // sample, this percentage is 100.
      Map<String, Double> pathToTopPercentage = new HashMap<String, Double>();
      // A set of inputs we know were not sampled for some task, so we should
      // ignore any entries for them in pathToTopPercentage
      Set<ReadEntity> nonSampledInputs = new HashSet<ReadEntity>();
      boolean isThereSampling = false;
      if (!hookContext.getQueryPlan().getQueryStr().toUpperCase().
          contains(" JOIN ")) {
        isThereSampling = HookUtils.checkForSamplingTasks(
            hookContext.getQueryPlan().getRootTasks(),
            pathToTopPercentage,
            nonSampledInputs);
      }

      // if we are set on local mode execution (via user or auto) bail
      if ("local".equals(conf.getVar(HiveConf.ConfVars.HADOOPJT))) {
        return;
      }

      // The smc hive.configs contains a mapping of pools to the map-reduce
      // cluster. If the user has specified a pool, and the pool belongs to one
      // of the clusters for the smc, use that cluster
      if (useClusterFromSmcConfig(conf)) {
        return;
      }

      // If this is an SLA pool, bail
      if (isOnSlaPool(conf)) {
        return;
      }

      // check if we need to run at all
      if (! "true".equals(conf.get("fbhive.jobtracker.auto", ""))) {
        return;
      }

      int bronzePercentage = conf.getInt("fbhive.jobtracker.bronze.percentage",
                                         0);
      boolean isCoronaEnabled = conf.getBoolean("fbhive.jobtracker.corona.enabled", false);
      int coronaPercentage = 0;
      if (isCoronaEnabled) {
        coronaPercentage = conf.getInt("fbhive.jobtracker.corona.percentage",
                                         0);
      }

      int percents [] = {bronzePercentage, coronaPercentage};
      int roll = rollDice(percents);
      LOG.debug("Dice roll is " + roll);
      boolean tryBronze = false;
      boolean tryCorona = false;

      if (roll == -1) {
        // Don't run bronze/corona
        LOG.info(dislike + "because the coin toss said so");
        return;
      } else if (roll == 0) {
        tryBronze = true;
      } else if (roll == 1) {
        tryCorona = true;
      } else {
        throw new RuntimeException("Invalid roll! Roll was " + roll);
      }

      int maxGigaBytes = conf.getInt("fbhive.jobtracker.bronze.maxGigaBytes", 0);
      if (maxGigaBytes == 0) {
        LOG.info (dislike + "maxGigaBytes = 0");
        return;
      }
      long maxBytes = maxGigaBytes * 1024L * 1024 * 1024;

      if (maxGigaBytes < 0) {
        LOG.warn (dislike + "maxGigaBytes value of " + maxGigaBytes + "is invalid");
        return;
      }

      String bronzeHadoopHome = conf.get("fbhive.jobtracker.bronze.hadoopHome",
                                         "/mnt/vol/hive/sites/bronze/hadoop");

      String bronzeJobTracker = conf.get("fbhive.jobtracker.bronze.tracker",
                                         conf.get(FBHiveConf.FBHIVE_BRONZE_JOBTRACKER));

      // assuming we are using combinehiveinputformat - we know the # of splits will _at least_
      // be >= number of partitions/tables. by indicating the max input size - the
      // admin is also signalling the max # of splits (maxGig*1000/256MB). So we limit the number
      // of partitions to the max # of splits.

      int maxSplits = conf.getInt("fbhive.jobtracker.bronze.maxPartitions", maxGigaBytes * 4);

      if (!isThereSampling && inputs.size() > maxSplits) {
        LOG.info (dislike + "number of input tables/partitions: " + inputs.size() +
                  " exceeded max splits: " + maxSplits);
        return;
      }

      if (conf.getIntVar(HiveConf.ConfVars.HADOOPNUMREDUCERS) > maxSplits) {
        LOG.info (dislike + "number of reducers: "
                  + conf.getVar(HiveConf.ConfVars.HADOOPNUMREDUCERS)
                  + " exceeded max reducers: " + maxSplits);
        return;
      }

      InputInfo info = HookUtils.getInputInfo(inputs, inputToCS, conf,
          isThereSampling, pathToTopPercentage, nonSampledInputs, maxSplits,
          maxBytes);

      if (info.getEstimatedNumSplits() > maxSplits) {
        LOG.info (dislike + "the estimated number of input " +
            "tables/partitions exceeded max splits: " + maxSplits);
        return;
      }

      if (info.getSize() > maxBytes) {
        LOG.info (dislike + "input length of " + info.getSize() +
            " is more than " + maxBytes);
        return;
      }

      // we have met all the conditions to switch to bronze/corona cluster

      if (tryBronze) {
        // Run it on Bronze
        preHadoopBin = conf.getVar(HiveConf.ConfVars.HADOOPBIN);
        conf.setVar(HiveConf.ConfVars.HADOOPBIN, bronzeHadoopHome +
                    "/bin/hadoop");
        preJobTracker = conf.getVar(HiveConf.ConfVars.HADOOPJT);
        conf.setVar(HiveConf.ConfVars.HADOOPJT, bronzeJobTracker);
      } else if (tryCorona){
        String coronaHadoopHome = conf.get(
          "fbhive.jobtracker.corona.hadoopHome",
          "/mnt/vol/hive/sites/corona/hadoop");
        runCorona(conf, coronaHadoopHome);
      }
    }

    private void runCorona(HiveConf conf, String hadoopHome) {
      // Run it on Corona
      preHadoopBin = conf.getVar(HiveConf.ConfVars.HADOOPBIN);
      conf.setVar(HiveConf.ConfVars.HADOOPBIN, hadoopHome + "/bin/hadoop");
      // No need to set the JT as it's done through the conf
      Configuration coronaConf = new Configuration(false);
      // Read the configuration, save old values, replace with new ones
      coronaConf.addResource("mapred-site-corona.xml");
      savedValues = new HashMap<String, String>();
      for (Entry<String, String> e : coronaConf) {
        String key = e.getKey();
        String value = e.getValue();
        LOG.debug("Saving " + key + "(" + conf.get(key) + ")");
        savedValues.put(key, conf.get(key));
        LOG.debug("Setting " + key + "(" + key + ")");
        conf.set(key, value);
      }
    }
  }

  /**
   * Randomly picks an index with chance that is indicated by the value in
   * percentages. Returns -1 for the remaining percentage
   *
   * E.g. [60, 20] will return 0 (60% of the time) and 1 (20% of the time) and
   * -1 (20% of the time)
   * @param percentages
   * @return
   */
  private static int rollDice(int [] percentages) {

    Random randGen = new Random();
    int randVal = randGen.nextInt(100) + 1;

    // Make sure that percentages add up to <= 100%
    int sum = 0;
    for (int i=0; i < percentages.length; i++) {
      sum += percentages[i];
      if (percentages[i] < 0) {
        throw new RuntimeException("Percentages must be >=0. Got " +
                                   percentages[i]);
      }
    }
    if (sum > 100) {
      throw new RuntimeException("Percentages add up to > 100!");
    }

    for (int i=0; i < percentages.length; i++) {
      if (randVal <= percentages[i]) {
        return i;
      }
      randVal = randVal - percentages[i];
    }
    return -1;
  }

  public static class PostExec implements ExecuteWithHookContext {
    @Override
    public void run(HookContext hookContext) throws Exception {
      assert(hookContext.getHookType() == HookContext.HookType.POST_EXEC_HOOK);
      SessionState ss = SessionState.get();
      Set<ReadEntity> inputs = hookContext.getInputs();
      Set<WriteEntity> outputs = hookContext.getOutputs();
      LineageInfo linfo = hookContext.getLinfo();
      UserGroupInformation ugi = hookContext.getUgi();
      this.run(ss,inputs,outputs,linfo,ugi);
    }

    public void run(SessionState sess, Set<ReadEntity> inputs,
                    Set<WriteEntity> outputs, LineageInfo lInfo,
                    UserGroupInformation ugi) throws Exception {
      HiveConf conf = sess.getConf();

      if (preHadoopBin != null) {
        conf.setVar(HiveConf.ConfVars.HADOOPBIN, preHadoopBin);
        preHadoopBin = null;
      }

      if (preJobTracker != null) {
        conf.setVar(HiveConf.ConfVars.HADOOPJT, preJobTracker);
        preJobTracker = null;
      }

      // Restore values set for Corona
      if (savedValues != null) {
        for (Entry<String,String> e : savedValues.entrySet()) {
          String key = e.getKey();
          String value = e.getValue();
          LOG.debug("Restoring " + key + "(" + value + ")");
          if (value != null) {
            conf.set(key, value);
          } else  {
            conf.set(key, "");
          }
        }
      }
    }
  }
}
