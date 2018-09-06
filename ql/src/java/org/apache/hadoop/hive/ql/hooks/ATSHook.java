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
package org.apache.hadoop.hive.ql.hooks;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.ExplainTask;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration;
import org.apache.hadoop.hive.ql.plan.ExplainWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hive.common.util.ShutdownHookManager;
import org.apache.tez.dag.api.TezConfiguration;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * ATSHook sends query + plan info to Yarn App Timeline Server. To enable (hadoop 2.4 and up) set
 * hive.exec.pre.hooks/hive.exec.post.hooks/hive.exec.failure.hooks to include this class.
 */
public class ATSHook implements ExecuteWithHookContext {

  private static final Logger LOG = LoggerFactory.getLogger(ATSHook.class.getName());
  private static final Object LOCK = new Object();
  private static final int VERSION = 2;
  private static ExecutorService executor;
  private static ExecutorService senderExecutor;
  private static TimelineClient timelineClient;
  private enum EntityTypes { HIVE_QUERY_ID };
  private enum EventTypes { QUERY_SUBMITTED, QUERY_COMPLETED };
  private static final String ATS_DOMAIN_PREFIX = "hive_";
  private static boolean defaultATSDomainCreated = false;
  private static final String DEFAULT_ATS_DOMAIN = "hive_default_ats";

  @VisibleForTesting
  enum OtherInfoTypes {
    QUERY, STATUS, TEZ, MAPRED, INVOKER_INFO, SESSION_ID, THREAD_NAME, VERSION,
    CLIENT_IP_ADDRESS, HIVE_ADDRESS, HIVE_INSTANCE_TYPE, CONF, PERF, LLAP_APP_ID
  };
  private enum ExecutionMode {
    MR, TEZ, LLAP, SPARK, NONE
  };
  private enum PrimaryFilterTypes {
    user, requestuser, operationid, executionmode, tablesread, tableswritten, queue
  };

  private static final int WAIT_TIME = 3;

  private static final String[] PERF_KEYS = new String[] {
    PerfLogger.PARSE, PerfLogger.COMPILE, PerfLogger.ANALYZE, PerfLogger.OPTIMIZER,
    PerfLogger.GET_SPLITS, PerfLogger.RUN_TASKS,
  };

  private static void setupAtsExecutor(HiveConf conf) {
    synchronized(LOCK) {
      if (executor == null) {

        // The call to ATS appears to block indefinitely, blocking the ATS thread while
        // the hook continues to submit work to the ExecutorService with each query.
        // Over time the queued items can cause OOM as the HookContext seems to contain
        // some items which use a lot of memory.
        // Prevent this situation by creating executor with bounded capacity -
        // the event will not be sent to ATS if there are too many outstanding work submissions.
        int queueCapacity = conf.getIntVar(HiveConf.ConfVars.ATSHOOKQUEUECAPACITY);

        // Executor to create the ATS events.
        // This can use significant resources and should not be done on the main query thread.
        LOG.info("Creating ATS executor queue with capacity " + queueCapacity);
        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>(queueCapacity);
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ATS Logger %d").build();
        executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, queue, threadFactory);

        // Create a separate thread to send the events.
        // Keep separate from the creating events in case the send blocks.
        BlockingQueue<Runnable> senderQueue = new LinkedBlockingQueue<Runnable>(queueCapacity);
        senderExecutor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, senderQueue, threadFactory);

        YarnConfiguration yarnConf = new YarnConfiguration();
        timelineClient = TimelineClient.createTimelineClient();
        timelineClient.init(yarnConf);
        timelineClient.start();

        ShutdownHookManager.addShutdownHook(new Runnable() {
          @Override
          public void run() {
            try {
              executor.shutdown();
              executor.awaitTermination(WAIT_TIME, TimeUnit.SECONDS);
              executor = null;
            } catch(InterruptedException ie) { /* ignore */ }
            timelineClient.stop();
          }
        });
      }
    }
  }

  public ATSHook() {
    LOG.info("Created ATS Hook");
  }

  private void createTimelineDomain(String domainId, String readers, String writers) throws Exception {
    TimelineDomain timelineDomain = new TimelineDomain();
    timelineDomain.setId(domainId);
    timelineDomain.setReaders(readers);
    timelineDomain.setWriters(writers);
    timelineClient.putDomain(timelineDomain);
    LOG.info("ATS domain created:" + domainId + "(" + readers + "," + writers + ")");
  }

  private String createOrGetDomain(final HookContext hookContext) throws Exception {
    final String domainId;
    String domainReaders = null;
    String domainWriters = null;
    boolean create = false;
    if (SessionState.get() != null) {
      if (SessionState.get().getATSDomainId() == null) {
        domainId = ATS_DOMAIN_PREFIX + hookContext.getSessionId();
        // Create session domain if not present
        if (SessionState.get().getATSDomainId() == null) {
          String requestuser = hookContext.getUserName();
          if (hookContext.getUserName() == null ){
            requestuser = hookContext.getUgi().getShortUserName() ;
          }
          boolean addHs2User =
              HiveConf.getBoolVar(hookContext.getConf(), ConfVars.HIVETEZHS2USERACCESS);

          UserGroupInformation loginUserUgi = UserGroupInformation.getLoginUser();
          String loginUser =
              loginUserUgi == null ? null : loginUserUgi.getShortUserName();

          // In Tez, TEZ_AM_VIEW_ACLS/TEZ_AM_MODIFY_ACLS is used as the base for Tez ATS ACLS,
          // so if exists, honor it. So we get the same ACLS for Tez ATS entries and
          // Hive entries
          domainReaders = Utilities.getAclStringWithHiveModification(hookContext.getConf(),
              TezConfiguration.TEZ_AM_VIEW_ACLS, addHs2User, requestuser, loginUser);

          domainWriters = Utilities.getAclStringWithHiveModification(hookContext.getConf(),
              TezConfiguration.TEZ_AM_MODIFY_ACLS, addHs2User, requestuser, loginUser);
          SessionState.get().setATSDomainId(domainId);
          create = true;
        }
      } else {
        domainId = SessionState.get().getATSDomainId();
      }
    } else {
      // SessionState is null, this is unlikely to happen, just in case
      if (!defaultATSDomainCreated) {
        domainReaders = domainWriters = UserGroupInformation.getCurrentUser().getShortUserName();
        defaultATSDomainCreated = true;
        create = true;
      }
      domainId = DEFAULT_ATS_DOMAIN;
    }
    if (create) {
      final String readers = domainReaders;
      final String writers = domainWriters;
      // executor is single thread, so we can guarantee
      // domain created before any ATS entries
      executor.submit(new Runnable() {
        @Override
        public void run() {
          try {
            createTimelineDomain(domainId, readers, writers);
          } catch (Exception e) {
            LOG.warn("Failed to create ATS domain " + domainId, e);
          }
        }
      });
    }
    return domainId;
  }
  @Override
  public void run(final HookContext hookContext) throws Exception {
    final long currentTime = System.currentTimeMillis();

    final HiveConf conf = new HiveConf(hookContext.getConf());
    final QueryState queryState = hookContext.getQueryState();
    final String queryId = queryState.getQueryId();

    final Map<String, Long> durations = new HashMap<String, Long>();
    for (String key : hookContext.getPerfLogger().getEndTimes().keySet()) {
      durations.put(key, hookContext.getPerfLogger().getDuration(key));
    }

    try {
      setupAtsExecutor(conf);

      final String domainId = createOrGetDomain(hookContext);
      executor.submit(new Runnable() {
          @Override
          public void run() {
            try {
              QueryPlan plan = hookContext.getQueryPlan();
              if (plan == null) {
                return;
              }
              String queryId = plan.getQueryId();
              String opId = hookContext.getOperationId();
              long queryStartTime = plan.getQueryStartTime();
              String user = hookContext.getUgi().getShortUserName();
              String requestuser = hookContext.getUserName();
              if (hookContext.getUserName() == null ){
                requestuser = hookContext.getUgi().getUserName() ;
              }
              int numMrJobs = Utilities.getMRTasks(plan.getRootTasks()).size();
              int numTezJobs = Utilities.getTezTasks(plan.getRootTasks()).size();
              if (numMrJobs + numTezJobs <= 0) {
                return; // ignore client only queries
              }

              switch(hookContext.getHookType()) {
              case PRE_EXEC_HOOK:
              ExplainConfiguration config = new ExplainConfiguration();
              config.setFormatted(true);
              ExplainWork work = new ExplainWork(null,// resFile
                  null,// pCtx
                  plan.getRootTasks(),// RootTasks
                  plan.getFetchTask(),// FetchTask
                      null,
                  null,// analyzer
                  config, //explainConfig
                  null, // cboInfo
                  plan.getOptimizedQueryString() // optimizedSQL
              );
                @SuppressWarnings("unchecked")
                ExplainTask explain = (ExplainTask) TaskFactory.get(work);
                explain.initialize(queryState, plan, null, null);
                String query = plan.getQueryStr();
                JSONObject explainPlan = explain.getJSONPlan(null, work);
                String logID = conf.getLogIdVar(hookContext.getSessionId());
                List<String> tablesRead = getTablesFromEntitySet(hookContext.getInputs());
                List<String> tablesWritten = getTablesFromEntitySet(hookContext.getOutputs());
                String executionMode = getExecutionMode(plan).name();
                String hiveInstanceAddress = hookContext.getHiveInstanceAddress();
                if (hiveInstanceAddress == null) {
                  hiveInstanceAddress = InetAddress.getLocalHost().getHostAddress();
                }
                String hiveInstanceType = hookContext.isHiveServerQuery() ? "HS2" : "CLI";
                ApplicationId llapId = determineLlapId(conf, plan);
                fireAndForget(
                    createPreHookEvent(queryId, query, explainPlan, queryStartTime,
                        user, requestuser, numMrJobs, numTezJobs, opId,
                        hookContext.getIpAddress(), hiveInstanceAddress, hiveInstanceType,
                        hookContext.getSessionId(), logID, hookContext.getThreadId(), executionMode,
                        tablesRead, tablesWritten, conf, llapId, domainId));
                break;
              case POST_EXEC_HOOK:
                fireAndForget(createPostHookEvent(queryId, currentTime, user, requestuser, true, opId, durations, domainId));
                break;
              case ON_FAILURE_HOOK:
                fireAndForget(createPostHookEvent(queryId, currentTime, user, requestuser , false, opId, durations, domainId));
                break;
              default:
                //ignore
                break;
              }
            } catch (Exception e) {
              LOG.warn("Failed to submit plan to ATS for " + queryId, e);
            }
          }
        });
    } catch (Exception e) {
      LOG.warn("Failed to submit to ATS for " + queryId, e);
    }
  }

  protected List<String> getTablesFromEntitySet(Set<? extends Entity> entities) {
    List<String> tableNames = new ArrayList<String>();
    for (Entity entity : entities) {
      if (entity.getType() == Entity.Type.TABLE) {
        tableNames.add(entity.getTable().getFullyQualifiedName());
      }
    }
    return tableNames;
  }

  protected ExecutionMode getExecutionMode(QueryPlan plan) {
    int numMRJobs = Utilities.getMRTasks(plan.getRootTasks()).size();
    int numSparkJobs = Utilities.getSparkTasks(plan.getRootTasks()).size();
    int numTezJobs = Utilities.getTezTasks(plan.getRootTasks()).size();

    ExecutionMode mode = ExecutionMode.MR;
    if (0 == (numMRJobs + numSparkJobs + numTezJobs)) {
      mode = ExecutionMode.NONE;
    } else if (numSparkJobs > 0) {
      return ExecutionMode.SPARK;
    } else if (numTezJobs > 0) {
      mode = ExecutionMode.TEZ;
      // Need to go in and check if any of the tasks is running in LLAP mode.
      for (TezTask tezTask : Utilities.getTezTasks(plan.getRootTasks())) {
        if (tezTask.getWork().getLlapMode()) {
          mode = ExecutionMode.LLAP;
          break;
        }
      }
    }

    return mode;
  }

  TimelineEntity createPreHookEvent(String queryId, String query, JSONObject explainPlan,
      long startTime, String user, String requestuser, int numMrJobs, int numTezJobs, String opId,
      String clientIpAddress, String hiveInstanceAddress, String hiveInstanceType,
      String sessionID, String logID, String threadId, String executionMode,
      List<String> tablesRead, List<String> tablesWritten, HiveConf conf, ApplicationId llapAppId,
      String domainId)
          throws Exception {

    JSONObject queryObj = new JSONObject(new LinkedHashMap<>());
    queryObj.put("queryText", query);
    queryObj.put("queryPlan", explainPlan);

    LOG.info("Received pre-hook notification for :" + queryId);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Otherinfo: " + queryObj.toString());
      LOG.debug("Operation id: <" + opId + ">");
    }

    conf.stripHiddenConfigurations(conf);
    Map<String, String> confMap = new HashMap<String, String>();
    for (Map.Entry<String, String> setting : conf) {
      confMap.put(setting.getKey(), setting.getValue());
    }
    JSONObject confObj = new JSONObject((Map) confMap);

    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId(queryId);
    atsEntity.setEntityType(EntityTypes.HIVE_QUERY_ID.name());
    atsEntity.addPrimaryFilter(PrimaryFilterTypes.user.name(), user);
    atsEntity.addPrimaryFilter(PrimaryFilterTypes.requestuser.name(), requestuser);
    atsEntity.addPrimaryFilter(PrimaryFilterTypes.executionmode.name(), executionMode);
    atsEntity.addPrimaryFilter(PrimaryFilterTypes.queue.name(), conf.get("mapreduce.job.queuename"));

    if (opId != null) {
      atsEntity.addPrimaryFilter(PrimaryFilterTypes.operationid.name(), opId);
    }

    for (String tabName : tablesRead) {
      atsEntity.addPrimaryFilter(PrimaryFilterTypes.tablesread.name(), tabName);
    }
    for (String tabName : tablesWritten) {
      atsEntity.addPrimaryFilter(PrimaryFilterTypes.tableswritten.name(), tabName);
    }

    TimelineEvent startEvt = new TimelineEvent();
    startEvt.setEventType(EventTypes.QUERY_SUBMITTED.name());
    startEvt.setTimestamp(startTime);
    atsEntity.addEvent(startEvt);

    atsEntity.addOtherInfo(OtherInfoTypes.QUERY.name(), queryObj.toString());
    atsEntity.addOtherInfo(OtherInfoTypes.TEZ.name(), numTezJobs > 0);
    atsEntity.addOtherInfo(OtherInfoTypes.MAPRED.name(), numMrJobs > 0);
    atsEntity.addOtherInfo(OtherInfoTypes.SESSION_ID.name(), sessionID);
    atsEntity.addOtherInfo(OtherInfoTypes.INVOKER_INFO.name(), logID);
    atsEntity.addOtherInfo(OtherInfoTypes.THREAD_NAME.name(), threadId);
    atsEntity.addOtherInfo(OtherInfoTypes.VERSION.name(), VERSION);
    if (clientIpAddress != null) {
      atsEntity.addOtherInfo(OtherInfoTypes.CLIENT_IP_ADDRESS.name(), clientIpAddress);
    }
    atsEntity.addOtherInfo(OtherInfoTypes.HIVE_ADDRESS.name(), hiveInstanceAddress);
    atsEntity.addOtherInfo(OtherInfoTypes.HIVE_INSTANCE_TYPE.name(), hiveInstanceType);
    atsEntity.addOtherInfo(OtherInfoTypes.CONF.name(), confObj.toString());
    if (llapAppId != null) {
      atsEntity.addOtherInfo(OtherInfoTypes.LLAP_APP_ID.name(), llapAppId.toString());
    }
    atsEntity.setDomainId(domainId);

    return atsEntity;
  }

  TimelineEntity createPostHookEvent(String queryId, long stopTime, String user, String requestuser, boolean success,
      String opId, Map<String, Long> durations, String domainId) throws Exception {
    LOG.info("Received post-hook notification for :" + queryId);

    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId(queryId);
    atsEntity.setEntityType(EntityTypes.HIVE_QUERY_ID.name());
    atsEntity.addPrimaryFilter(PrimaryFilterTypes.user.name(), user);
    atsEntity.addPrimaryFilter(PrimaryFilterTypes.requestuser.name(), requestuser);
    if (opId != null) {
      atsEntity.addPrimaryFilter(PrimaryFilterTypes.operationid.name(), opId);
    }

    TimelineEvent stopEvt = new TimelineEvent();
    stopEvt.setEventType(EventTypes.QUERY_COMPLETED.name());
    stopEvt.setTimestamp(stopTime);
    atsEntity.addEvent(stopEvt);

    atsEntity.addOtherInfo(OtherInfoTypes.STATUS.name(), success);

    // Perf times
    JSONObject perfObj = new JSONObject(new LinkedHashMap<>());
    for (Map.Entry<String, Long> entry : durations.entrySet()) {
      perfObj.put(entry.getKey(), entry.getValue());
    }
    atsEntity.addOtherInfo(OtherInfoTypes.PERF.name(), perfObj.toString());
    atsEntity.setDomainId(domainId);

    return atsEntity;
  }

  void fireAndForget(final TimelineEntity entity) throws Exception {
    senderExecutor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          timelineClient.putEntities(entity);
        } catch (Exception err) {
          LOG.warn("Failed to send event to ATS", err);
        }
      }
    });
  }

  private ApplicationId determineLlapId(final HiveConf conf, QueryPlan plan) throws IOException {
    // Note: for now, LLAP is only supported in Tez tasks. Will never come to MR; others may
    //       be added here, although this is only necessary to have extra debug information.
    for (TezTask tezTask : Utilities.getTezTasks(plan.getRootTasks())) {
      if (!tezTask.getWork().getLlapMode()) continue;
      // In HS2, the client should have been cached already for the common case.
      // Otherwise, this may actually introduce delay to compilation for the first query.
      String hosts = HiveConf.getVar(conf, HiveConf.ConfVars.LLAP_DAEMON_SERVICE_HOSTS);
      if (hosts != null && !hosts.isEmpty()) {
        ApplicationId llapId = LlapRegistryService.getClient(conf).getApplicationId();
        LOG.info("The query will use LLAP instance " + llapId + " (" + hosts + ")");
        return llapId;
      } else {
        LOG.info("Cannot determine LLAP instance on client - service hosts are not set");
        return null;
      }
    }
    return null;
  }
}
