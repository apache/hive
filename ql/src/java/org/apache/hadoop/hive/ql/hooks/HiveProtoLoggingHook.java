/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.hooks;

import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERDATABASE;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERDATABASE_OWNER;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERPARTITION_BUCKETNUM;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERPARTITION_FILEFORMAT;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERPARTITION_LOCATION;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERPARTITION_MERGEFILES;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERPARTITION_SERDEPROPERTIES;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERPARTITION_SERIALIZER;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERTABLE_ADDCOLS;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERTABLE_ADDCONSTRAINT;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERTABLE_ADDPARTS;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERTABLE_ARCHIVE;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERTABLE_BUCKETNUM;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERTABLE_CLUSTER_SORT;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERTABLE_COMPACT;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERTABLE_DROPCONSTRAINT;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERTABLE_DROPPARTS;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERTABLE_EXCHANGEPARTITION;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERTABLE_FILEFORMAT;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERTABLE_LOCATION;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERTABLE_MERGEFILES;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERTABLE_PARTCOLTYPE;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERTABLE_PROPERTIES;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERTABLE_RENAME;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERTABLE_RENAMECOL;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERTABLE_RENAMEPART;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERTABLE_REPLACECOLS;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERTABLE_SERDEPROPERTIES;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERTABLE_SERIALIZER;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERTABLE_SKEWED;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERTABLE_TOUCH;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERTABLE_UNARCHIVE;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERTABLE_UPDATEPARTSTATS;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERTABLE_UPDATETABLESTATS;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERTBLPART_SKEWED_LOCATION;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERVIEW_AS;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERVIEW_PROPERTIES;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ALTERVIEW_RENAME;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.ANALYZE_TABLE;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.CACHE_METADATA;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.CREATEDATABASE;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.CREATEFUNCTION;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.CREATEMACRO;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.CREATEROLE;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.CREATETABLE;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.CREATETABLE_AS_SELECT;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.CREATEVIEW;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.DROPDATABASE;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.DROPFUNCTION;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.DROPMACRO;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.DROPROLE;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.DROPTABLE;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.DROPVIEW;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.DROPVIEW_PROPERTIES;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.EXPORT;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.IMPORT;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.KILL_QUERY;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.LOAD;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.LOCKTABLE;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.MSCK;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.QUERY;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.RELOADFUNCTION;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.TRUNCATETABLE;
import static org.apache.hadoop.hive.ql.plan.HiveOperation.UNLOCKTABLE;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.ExplainTask;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecDriver;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.hooks.proto.HiveHookEvents.HiveHookEventProto;
import org.apache.hadoop.hive.ql.hooks.proto.HiveHookEvents.MapFieldEntry;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration;
import org.apache.hadoop.hive.ql.plan.ExplainWork;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hive.common.util.ShutdownHookManager;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.history.logging.proto.DatePartitionedLogger;
import org.apache.tez.dag.history.logging.proto.ProtoMessageWriter;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Log events from hive hook using protobuf serialized format, partitioned by date.
 */
public class HiveProtoLoggingHook implements ExecuteWithHookContext {
  private static final Logger LOG = LoggerFactory.getLogger(HiveProtoLoggingHook.class.getName());
  private static final Set<String> includedOperationSet;
  private static final int VERSION = 1;

  static {
    // List of operation for which we log.
    includedOperationSet = Arrays.stream(new HiveOperation[] { LOAD, EXPORT, IMPORT,
        CREATEDATABASE, DROPDATABASE, DROPTABLE, MSCK, ALTERTABLE_ADDCOLS, ALTERTABLE_REPLACECOLS,
        ALTERTABLE_RENAMECOL, ALTERTABLE_RENAMEPART, ALTERTABLE_UPDATEPARTSTATS,
        ALTERTABLE_UPDATETABLESTATS, ALTERTABLE_RENAME, ALTERTABLE_DROPPARTS, ALTERTABLE_ADDPARTS,
        ALTERTABLE_TOUCH, ALTERTABLE_ARCHIVE, ALTERTABLE_UNARCHIVE, ALTERTABLE_PROPERTIES,
        ALTERTABLE_SERIALIZER, ALTERPARTITION_SERIALIZER, ALTERTABLE_SERDEPROPERTIES,
        ALTERPARTITION_SERDEPROPERTIES, ALTERTABLE_CLUSTER_SORT, ANALYZE_TABLE, CACHE_METADATA,
        ALTERTABLE_BUCKETNUM, ALTERPARTITION_BUCKETNUM, CREATEFUNCTION, DROPFUNCTION,
        RELOADFUNCTION, CREATEMACRO, DROPMACRO, CREATEVIEW, DROPVIEW, ALTERVIEW_PROPERTIES,
        DROPVIEW_PROPERTIES, LOCKTABLE, UNLOCKTABLE, CREATEROLE, DROPROLE, ALTERTABLE_FILEFORMAT,
        ALTERPARTITION_FILEFORMAT, ALTERTABLE_LOCATION, ALTERPARTITION_LOCATION, CREATETABLE,
        TRUNCATETABLE, CREATETABLE_AS_SELECT, QUERY, ALTERDATABASE, ALTERDATABASE_OWNER,
        ALTERTABLE_MERGEFILES, ALTERPARTITION_MERGEFILES, ALTERTABLE_SKEWED,
        ALTERTBLPART_SKEWED_LOCATION, ALTERTABLE_PARTCOLTYPE, ALTERTABLE_EXCHANGEPARTITION,
        ALTERTABLE_DROPCONSTRAINT, ALTERTABLE_ADDCONSTRAINT, ALTERVIEW_RENAME, ALTERVIEW_AS,
        ALTERTABLE_COMPACT, KILL_QUERY })
            .map(HiveOperation::getOperationName)
            .collect(Collectors.toSet());
  }

  private static final int HIVE_HOOK_PROTO_QUEUE_CAPACITY_DEFAULT = 64;
  private static final int WAIT_TIME = 5;

  public enum EventType {
    QUERY_SUBMITTED, QUERY_COMPLETED
  }

  public enum OtherInfoType {
    QUERY, STATUS, TEZ, MAPRED, INVOKER_INFO, SESSION_ID, THREAD_NAME, VERSION, CLIENT_IP_ADDRESS,
    HIVE_ADDRESS, HIVE_INSTANCE_TYPE, CONF, PERF, LLAP_APP_ID
  }

  public enum ExecutionMode {
    MR, TEZ, LLAP, SPARK, NONE
  }

  static class EventLogger {
    private final Clock clock;
    private final String logFileName;
    private final DatePartitionedLogger<HiveHookEventProto> logger;
    private final ExecutorService logWriter;
    private int logFileCount = 0;
    private ProtoMessageWriter<HiveHookEventProto> writer;
    private LocalDate writerDate;
    private boolean eventPerFile;

    EventLogger(HiveConf conf, Clock clock) {
      this.clock = clock;
      // randomUUID is slow, since its cryptographically secure, only first query will take time.
      this.logFileName = "hive_" + UUID.randomUUID().toString();
      String baseDir = conf.getVar(ConfVars.HIVE_PROTO_EVENTS_BASE_PATH);
      if (StringUtils.isBlank(baseDir)) {
        baseDir = null;
        LOG.error(ConfVars.HIVE_PROTO_EVENTS_BASE_PATH.varname + " is not set, logging disabled.");
      }

      eventPerFile = conf.getBoolVar(ConfVars.HIVE_PROTO_FILE_PER_EVENT);
      LOG.info("Event per file enabled: {}", eventPerFile);
      DatePartitionedLogger<HiveHookEventProto> tmpLogger = null;
      try {
        if (baseDir != null) {
          tmpLogger = new DatePartitionedLogger<>(HiveHookEventProto.PARSER, new Path(baseDir),
              conf, clock);
        }
      } catch (IOException e) {
        LOG.error("Unable to intialize logger, logging disabled.", e);
      }
      this.logger = tmpLogger;
      if (logger == null) {
        logWriter = null;
        return;
      }

      int queueCapacity = conf.getInt(ConfVars.HIVE_PROTO_EVENTS_QUEUE_CAPACITY.varname,
          HIVE_HOOK_PROTO_QUEUE_CAPACITY_DEFAULT);

      ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true)
          .setNameFormat("Hive Hook Proto Log Writer %d").build();
      logWriter = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue<Runnable>(queueCapacity), threadFactory);
    }

    void shutdown() {
      if (logWriter != null) {
        logWriter.shutdown();
        try {
          logWriter.awaitTermination(WAIT_TIME, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          LOG.warn("Got interrupted exception while waiting for events to be flushed", e);
        }
      }
      IOUtils.closeQuietly(writer);
    }

    void handle(HookContext hookContext) {
      if (logger == null) {
        return;
      }
      // Note: same hookContext object is used for all the events for a given query, if we try to
      // do it async we have concurrency issues and when query cache is enabled, post event comes
      // before we start the pre hook processing and causes inconsistent events publishing.
      QueryPlan plan = hookContext.getQueryPlan();
      if (plan == null) {
        LOG.debug("Received null query plan.");
        return;
      }
      if (!includedOperationSet.contains(plan.getOperationName())) {
        LOG.debug("Not logging events of operation type : {}", plan.getOperationName());
        return;
      }
      HiveHookEventProto event;
      switch (hookContext.getHookType()) {
      case PRE_EXEC_HOOK:
        event = getPreHookEvent(hookContext);
        break;
      case POST_EXEC_HOOK:
        event = getPostHookEvent(hookContext, true);
        break;
      case ON_FAILURE_HOOK:
        event = getPostHookEvent(hookContext, false);
        break;
      default:
        LOG.warn("Ignoring event of type: {}", hookContext.getHookType());
        event = null;
      }
      if (event != null) {
        try {
          logWriter.execute(() -> writeEvent(event));
        } catch (RejectedExecutionException e) {
          LOG.warn("Writer queue full ignoring event {} for query {}",
              hookContext.getHookType(), plan.getQueryId());
        }
      }
    }

    private static final int MAX_RETRIES = 2;
    private void writeEvent(HiveHookEventProto event) {
      for (int retryCount = 0; retryCount <= MAX_RETRIES; ++retryCount) {
        try {
          if (writer == null || !logger.getNow().toLocalDate().equals(writerDate)) {
            if (writer != null) {
              // Day change over case, reset the logFileCount.
              logFileCount = 0;
              IOUtils.closeQuietly(writer);
            }
            // increment log file count, if creating a new writer.
            writer = logger.getWriter(logFileName + "_" + ++logFileCount);
            writerDate = logger.getDateFromDir(writer.getPath().getParent().getName());
          }
          writer.writeProto(event);
          if (eventPerFile) {
            if (writer != null) {
              LOG.debug("Event per file enabled. Closing proto event file: {}", writer.getPath());
              IOUtils.closeQuietly(writer);
            }
            // rollover to next file
            writer = logger.getWriter(logFileName + "_" + ++logFileCount);
          } else {
            writer.hflush();
          }
          return;
        } catch (IOException e) {
          // Something wrong with writer, lets close and reopen.
          IOUtils.closeQuietly(writer);
          writer = null;
          if (retryCount < MAX_RETRIES) {
            LOG.warn("Error writing proto message for query {}, eventType: {}, retryCount: {}," +
                " error: {} ", event.getHiveQueryId(), event.getEventType(), retryCount,
                e.getMessage());
          } else {
            LOG.error("Error writing proto message for query {}, eventType: {}: ",
                event.getHiveQueryId(), event.getEventType(), e);
          }
          try {
            // 0 seconds, for first retry assuming fs object was closed and open will fix it.
            Thread.sleep(1000 * retryCount * retryCount);
          } catch (InterruptedException e1) {
            LOG.warn("Got interrupted in retry sleep.", e1);
          }
        }
      }
    }

    private HiveHookEventProto getPreHookEvent(HookContext hookContext) {
      QueryPlan plan = hookContext.getQueryPlan();
      LOG.info("Received pre-hook notification for: " + plan.getQueryId());

      // Make a copy so that we do not modify hookContext conf.
      HiveConf conf = new HiveConf(hookContext.getConf());
      List<ExecDriver> mrTasks = Utilities.getMRTasks(plan.getRootTasks());
      List<TezTask> tezTasks = Utilities.getTezTasks(plan.getRootTasks());
      ExecutionMode executionMode = getExecutionMode(plan, mrTasks, tezTasks);

      HiveHookEventProto.Builder builder = HiveHookEventProto.newBuilder();
      builder.setEventType(EventType.QUERY_SUBMITTED.name());
      builder.setTimestamp(plan.getQueryStartTime());
      builder.setHiveQueryId(plan.getQueryId());
      builder.setUser(getUser(hookContext));
      builder.setRequestUser(getRequestUser(hookContext));
      String queueName = getQueueName(executionMode, conf);
      if (queueName != null) {
        builder.setQueue(queueName);
      }
      builder.setExecutionMode(executionMode.name());
      builder.addAllTablesRead(getTablesFromEntitySet(hookContext.getInputs()));
      builder.addAllTablesWritten(getTablesFromEntitySet(hookContext.getOutputs()));
      if (hookContext.getOperationId() != null) {
        builder.setOperationId(hookContext.getOperationId());
      }

      try {
        JSONObject queryObj = new JSONObject();
        queryObj.put("queryText", plan.getQueryStr());
        queryObj.put("queryPlan", getExplainPlan(plan, conf, hookContext));
        addMapEntry(builder, OtherInfoType.QUERY, queryObj.toString());
      } catch (Exception e) {
        LOG.error("Unexpected exception while serializing json.", e);
      }

      addMapEntry(builder, OtherInfoType.TEZ, Boolean.toString(tezTasks.size() > 0));
      addMapEntry(builder, OtherInfoType.MAPRED, Boolean.toString(mrTasks.size() > 0));
      addMapEntry(builder, OtherInfoType.SESSION_ID, hookContext.getSessionId());
      String logID = conf.getLogIdVar(hookContext.getSessionId());
      addMapEntry(builder, OtherInfoType.INVOKER_INFO, logID);
      addMapEntry(builder, OtherInfoType.THREAD_NAME, hookContext.getThreadId());
      addMapEntry(builder, OtherInfoType.VERSION, Integer.toString(VERSION));
      addMapEntry(builder, OtherInfoType.CLIENT_IP_ADDRESS, hookContext.getIpAddress());

      String hiveInstanceAddress = hookContext.getHiveInstanceAddress();
      if (hiveInstanceAddress == null) {
        try {
          hiveInstanceAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
          LOG.error("Error tyring to get localhost address: ", e);
        }
      }
      addMapEntry(builder, OtherInfoType.HIVE_ADDRESS, hiveInstanceAddress);

      String hiveInstanceType = hookContext.isHiveServerQuery() ? "HS2" : "CLI";
      addMapEntry(builder, OtherInfoType.HIVE_INSTANCE_TYPE, hiveInstanceType);

      ApplicationId llapId = determineLlapId(conf, executionMode);
      if (llapId != null) {
        addMapEntry(builder, OtherInfoType.LLAP_APP_ID, llapId.toString());
      }

      conf.stripHiddenConfigurations(conf);
      JSONObject confObj = new JSONObject();
      for (Map.Entry<String, String> setting : conf) {
        confObj.put(setting.getKey(), setting.getValue());
      }
      addMapEntry(builder, OtherInfoType.CONF, confObj.toString());
      return builder.build();
    }

    private HiveHookEventProto getPostHookEvent(HookContext hookContext, boolean success) {
      QueryPlan plan = hookContext.getQueryPlan();
      LOG.info("Received post-hook notification for: " + plan.getQueryId());

      HiveHookEventProto.Builder builder = HiveHookEventProto.newBuilder();
      builder.setEventType(EventType.QUERY_COMPLETED.name());
      builder.setTimestamp(clock.getTime());
      builder.setHiveQueryId(plan.getQueryId());
      builder.setUser(getUser(hookContext));
      builder.setRequestUser(getRequestUser(hookContext));
      if (hookContext.getOperationId() != null) {
        builder.setOperationId(hookContext.getOperationId());
      }
      addMapEntry(builder, OtherInfoType.STATUS, Boolean.toString(success));
      JSONObject perfObj = new JSONObject();
      for (String key : hookContext.getPerfLogger().getEndTimes().keySet()) {
        perfObj.put(key, hookContext.getPerfLogger().getDuration(key));
      }
      addMapEntry(builder, OtherInfoType.PERF, perfObj.toString());

      return builder.build();
    }

    private void addMapEntry(HiveHookEventProto.Builder builder, OtherInfoType key, String value) {
      if (value != null) {
        builder.addOtherInfo(
            MapFieldEntry.newBuilder().setKey(key.name()).setValue(value).build());
      }
    }

    private String getUser(HookContext hookContext) {
      return hookContext.getUgi().getShortUserName();
    }

    private String getRequestUser(HookContext hookContext) {
      String requestuser = hookContext.getUserName();
      if (requestuser == null) {
        requestuser = hookContext.getUgi().getUserName();
      }
      return requestuser;
    }

    private String getQueueName(ExecutionMode mode, HiveConf conf) {
      switch (mode) {
      case LLAP:
        return conf.get(HiveConf.ConfVars.LLAP_DAEMON_QUEUE_NAME.varname);
      case MR:
        return conf.get(MRJobConfig.QUEUE_NAME);
      case TEZ:
        return conf.get(TezConfiguration.TEZ_QUEUE_NAME);
      case SPARK:
      case NONE:
      default:
        return null;
      }
    }

    private List<String> getTablesFromEntitySet(Set<? extends Entity> entities) {
      List<String> tableNames = new ArrayList<>();
      for (Entity entity : entities) {
        if (entity.getType() == Entity.Type.TABLE) {
          tableNames.add(entity.getTable().getDbName() + "." + entity.getTable().getTableName());
        }
      }
      return tableNames;
    }

    private ExecutionMode getExecutionMode(QueryPlan plan, List<ExecDriver> mrTasks,
        List<TezTask> tezTasks) {
      if (tezTasks.size() > 0) {
        // Need to go in and check if any of the tasks is running in LLAP mode.
        for (TezTask tezTask : tezTasks) {
          if (tezTask.getWork().getLlapMode()) {
            return ExecutionMode.LLAP;
          }
        }
        return ExecutionMode.TEZ;
      } else if (mrTasks.size() > 0) {
        return ExecutionMode.MR;
      } else if (Utilities.getSparkTasks(plan.getRootTasks()).size() > 0) {
        return ExecutionMode.SPARK;
      } else {
        return ExecutionMode.NONE;
      }
    }

    private JSONObject getExplainPlan(QueryPlan plan, HiveConf conf, HookContext hookContext)
        throws Exception {
      // Get explain plan for the query.
      ExplainConfiguration config = new ExplainConfiguration();
      config.setFormatted(true);
      ExplainWork work = new ExplainWork(null, // resFile
          null, // pCtx
          plan.getRootTasks(), // RootTasks
          plan.getFetchTask(), // FetchTask
          null, null, // analyzer
          config, // explainConfig
          plan.getCboInfo(), // cboInfo,
          plan.getOptimizedQueryString(),
          null
      );
      ExplainTask explain = (ExplainTask) TaskFactory.get(work, conf);
      explain.initialize(hookContext.getQueryState(), plan, null, null);
      return explain.getJSONPlan(null, work);
    }

    private ApplicationId determineLlapId(HiveConf conf, ExecutionMode mode) {
      // Note: for now, LLAP is only supported in Tez tasks. Will never come to MR; others may
      // be added here, although this is only necessary to have extra debug information.
      if (mode == ExecutionMode.LLAP) {
        // In HS2, the client should have been cached already for the common case.
        // Otherwise, this may actually introduce delay to compilation for the first query.
        String hosts = HiveConf.getVar(conf, HiveConf.ConfVars.LLAP_DAEMON_SERVICE_HOSTS);
        if (hosts != null && !hosts.isEmpty()) {
          try {
            return LlapRegistryService.getClient(conf).getApplicationId();
          } catch (IOException e) {
            LOG.error("Error trying to get llap instance", e);
          }
        } else {
          LOG.info("Cannot determine LLAP instance on client - service hosts are not set");
          return null;
        }
      }
      return null;
    }

    // Singleton using DCL.
    private static volatile EventLogger instance;
    static EventLogger getInstance(HiveConf conf) {
      if (instance == null) {
        synchronized (EventLogger.class) {
          if (instance == null) {
            instance = new EventLogger(conf, SystemClock.getInstance());
            ShutdownHookManager.addShutdownHook(instance::shutdown);
          }
        }
      }
      return instance;
    }
  }

  @Override
  public void run(HookContext hookContext) throws Exception {
    try {
      EventLogger logger = EventLogger.getInstance(hookContext.getConf());
      logger.handle(hookContext);
    } catch (Exception e) {
      LOG.error("Got exceptoin while processing event: ", e);
    }
  }
}
