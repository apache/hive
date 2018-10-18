/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap.daemon.impl;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.llap.LlapNodeId;
import org.apache.hadoop.hive.llap.log.Log4jQueryCompleteMarker;
import org.apache.hadoop.hive.llap.log.LogHelpers;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.logging.slf4j.Log4jMarker;
import org.apache.tez.common.CallableWithNdc;

import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.daemon.impl.LlapTokenChecker.LlapTokenInfo;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SignableVertexSpec;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateProto;
import org.apache.hadoop.hive.llap.shufflehandler.ShuffleHandler;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MDC;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * Tracks queries running within a daemon
 */
public class QueryTracker extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(QueryTracker.class);
  private static final Marker QUERY_COMPLETE_MARKER = new Log4jMarker(new Log4jQueryCompleteMarker());

  private final ScheduledExecutorService executorService;

  private final ConcurrentHashMap<QueryIdentifier, QueryInfo> queryInfoMap = new ConcurrentHashMap<>();

  private final String[] localDirsBase;
  private final FileSystem localFs;
  private final String clusterId;
  private final long defaultDeleteDelaySeconds;
  private final boolean routeBasedLoggingEnabled;

  // TODO At the moment there's no way of knowing whether a query is running or not.
  // A race is possible between dagComplete and registerFragment - where the registerFragment
  // is processed after a dagCompletes.
  // May need to keep track of completed dags for a certain time duration to avoid this.
  // Alternately - send in an explicit dag start message before any other message is processed.
  // Multiple threads communicating from a single AM gets in the way of this.

  // Keeps track of completed DAGS. QueryIdentifiers need to be unique across applications.
  private final Set<QueryIdentifier> completedDagMap =
      Collections.newSetFromMap(new ConcurrentHashMap<QueryIdentifier, Boolean>());


  private final Lock lock = new ReentrantLock();
  private final ConcurrentMap<QueryIdentifier, ReentrantReadWriteLock> dagSpecificLocks = new ConcurrentHashMap<>();

  // Tracks various maps for dagCompletions. This is setup here since stateChange messages
  // may be processed by a thread which ends up executing before a task.
  private final ConcurrentMap<QueryIdentifier, ConcurrentMap<String, SourceStateProto>>
      sourceCompletionMap = new ConcurrentHashMap<>();

  // Tracks HiveQueryId by QueryIdentifier. This can only be set when config is parsed in TezProcessor.
  // all the other existing code passes queryId equal to 0 everywhere.
  // If we switch the runtime and move to parsing the payload in the AM - the actual hive queryId could
  // be sent over the wire from the AM, and will take the place of AppId+dagId in QueryIdentifier.
  private final ConcurrentHashMap<QueryIdentifier, String> queryIdentifierToHiveQueryId =
      new ConcurrentHashMap<>();

  public QueryTracker(Configuration conf, String[] localDirsBase, String clusterId) {
    super("QueryTracker");
    this.localDirsBase = localDirsBase;
    this.clusterId = clusterId;
    try {
      localFs = FileSystem.getLocal(conf);
    } catch (IOException e) {
      throw new RuntimeException("Failed to setup local filesystem instance", e);
    }

    this.defaultDeleteDelaySeconds = HiveConf.getTimeVar(
        conf, ConfVars.LLAP_FILE_CLEANUP_DELAY_SECONDS, TimeUnit.SECONDS);

    int numCleanerThreads = HiveConf.getIntVar(
        conf, ConfVars.LLAP_DAEMON_NUM_FILE_CLEANER_THREADS);
    this.executorService = Executors.newScheduledThreadPool(numCleanerThreads,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("QueryCompletionThread %d").build());

    String logger = HiveConf.getVar(conf, ConfVars.LLAP_DAEMON_LOGGER);
    if (logger != null && (logger.equalsIgnoreCase(LogHelpers.LLAP_LOGGER_NAME_QUERY_ROUTING))) {
      routeBasedLoggingEnabled = true;
    } else {
      routeBasedLoggingEnabled = false;
    }
    LOG.info(
        "QueryTracker setup with numCleanerThreads={}, defaultCleanupDelay(s)={}, routeBasedLogging={}",
        numCleanerThreads, defaultDeleteDelaySeconds, routeBasedLoggingEnabled);
  }

  /**
   * Register a new fragment for a specific query
   */
  QueryFragmentInfo registerFragment(QueryIdentifier queryIdentifier, String appIdString, String dagIdString,
    String dagName, String hiveQueryIdString, int dagIdentifier, String vertexName, int fragmentNumber,
    int attemptNumber,
    String user, SignableVertexSpec vertex, Token<JobTokenIdentifier> appToken,
    String fragmentIdString, LlapTokenInfo tokenInfo, final LlapNodeId amNodeId) throws IOException {

    ReadWriteLock dagLock = getDagLock(queryIdentifier);
    // Note: This is a readLock to prevent a race with queryComplete. Operations
    // and mutations within this lock need to be on concurrent structures.
    dagLock.readLock().lock();
    try {
      if (completedDagMap.contains(queryIdentifier)) {
        // Cleanup the dag lock here, since it may have been created after the query completed
        dagSpecificLocks.remove(queryIdentifier);
        String message = "Dag " + dagName + " already complete. Rejecting fragment ["
            + vertexName + ", " + fragmentNumber + ", " + attemptNumber + "]";
        LOG.info(message);
        throw new RuntimeException(message);
      }
      // TODO: for now, we get the secure username out of UGI... after signing, we can take it
      //       out of the request provided that it's signed.
      if (tokenInfo == null) {
        tokenInfo = LlapTokenChecker.getTokenInfo(clusterId);
      }
      boolean isExistingQueryInfo = true;
      QueryInfo queryInfo = queryInfoMap.get(queryIdentifier);
      if (queryInfo == null) {
        if (UserGroupInformation.isSecurityEnabled()) {
          Preconditions.checkNotNull(tokenInfo.userName);
        }
        queryInfo =
            new QueryInfo(queryIdentifier, appIdString, dagIdString, dagName, hiveQueryIdString,
                dagIdentifier, user,
                getSourceCompletionMap(queryIdentifier), localDirsBase, localFs,
                tokenInfo.userName, tokenInfo.appId, amNodeId, vertex.getTokenIdentifier(), appToken,
                vertex.getIsExternalSubmission());
        QueryInfo old = queryInfoMap.putIfAbsent(queryIdentifier, queryInfo);
        if (old != null) {
          queryInfo = old;
        } else {
          isExistingQueryInfo = false;
        }
      }
      if (isExistingQueryInfo) {
        // We already retrieved the incoming info, check without UGI.
        LlapTokenChecker.checkPermissions(tokenInfo, queryInfo.getTokenUserName(),
            queryInfo.getTokenAppId(), queryInfo.getQueryIdentifier());
      }

      queryIdentifierToHiveQueryId.putIfAbsent(queryIdentifier, hiveQueryIdString);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Registering request for {} with the ShuffleHandler", queryIdentifier);
      }
      if (!vertex.getIsExternalSubmission()) {
        ShuffleHandler.get()
            .registerDag(appIdString, dagIdentifier, appToken,
                user, queryInfo.getLocalDirs());
      }

      return queryInfo.registerFragment(
          vertexName, fragmentNumber, attemptNumber, vertex, fragmentIdString);
    } finally {
      dagLock.readLock().unlock();
    }
  }

  /**
   * Indicate to the tracker that a fragment is complete. This is from internal execution within the daemon
   * @param fragmentInfo
   */
  void fragmentComplete(QueryFragmentInfo fragmentInfo) {
    QueryIdentifier qId = fragmentInfo.getQueryInfo().getQueryIdentifier();
    QueryInfo queryInfo = queryInfoMap.get(qId);
    if (queryInfo == null) {
      // Possible because a queryComplete message from the AM can come in first - KILL / SUCCESSFUL,
      // before the fragmentComplete is reported
      LOG.info("Ignoring fragmentComplete message for unknown query: {}", qId);
    } else {
      queryInfo.unregisterFragment(fragmentInfo);

      // Try marking the query as complete if this is an external submission
      handleFragmentCompleteExternalQuery(queryInfo);
    }
  }

  List<QueryFragmentInfo> getRegisteredFragments(QueryIdentifier queryIdentifier) {
    ReadWriteLock dagLock = getDagLock(queryIdentifier);
    dagLock.readLock().lock();
    try {
      QueryInfo queryInfo = queryInfoMap.get(queryIdentifier);
      if (queryInfo == null) {
        // Race with queryComplete
        LOG.warn("Unknown query: Returning an empty list of fragments");
        return Collections.emptyList();
      }
      return queryInfo.getRegisteredFragments();
    } finally {
      dagLock.readLock().unlock();
    }
  }

  /**
   * Register completion for a query
   * @param queryIdentifier
   * @param deleteDelay
   */
  QueryInfo queryComplete(QueryIdentifier queryIdentifier, long deleteDelay,
      boolean isExternalQuery) throws IOException {
    if (deleteDelay == -1) {
      deleteDelay = defaultDeleteDelaySeconds;
    }
    ReadWriteLock dagLock = getDagLock(queryIdentifier);
    dagLock.writeLock().lock();
    try {
      // If isExternalQuery -> the call is from within hte daemon, so no permission check required
      // to get access to the queryInfo instance.
      QueryInfo queryInfo = isExternalQuery
          ? queryInfoMap.get(queryIdentifier) : checkPermissionsAndGetQuery(queryIdentifier);
      if (queryInfo == null) {
        // Should not happen.
        LOG.warn("Ignoring query complete for unknown dag: {}", queryIdentifier);
        return null;
      }

      LOG.info(
          "Processing queryComplete for queryIdentifier={}, isExternalQuery={}, with deleteDelay={} seconds",
          queryIdentifier, isExternalQuery,
          deleteDelay);

      queryInfoMap.remove(queryIdentifier);
      if (!isExternalQuery) {
        rememberCompletedDag(queryIdentifier);
        cleanupLocalDirs(queryInfo, deleteDelay);
        handleLogOnQueryCompletion(queryInfo.getHiveQueryIdString(), queryInfo.getDagIdString());
      } else {
        // If there's no pending fragments, queue some of the cleanup for a later point - locks, log rolling.
        if (queryInfo.getRegisteredFragments().size() == 0) {
          LOG.debug("Queueing future cleanup for external queryId: {}", queryInfo.getHiveQueryIdString());
          executorService.schedule(new ExternalQueryCleanerCallable(queryInfo.getHiveQueryIdString(),
                  queryInfo.getDagIdString(), queryInfo.getQueryIdentifier()), 1, TimeUnit.MINUTES);
        } else {
          if (LOG.isTraceEnabled()) {
            LOG.trace(
                "NumRegisterFragments={}, Not queuing cleanup for external queryId={}",
                queryInfo.getRegisteredFragments().size(), queryInfo.getHiveQueryIdString());
          }
        }
      }

      // Clearing this before sending a kill is OK, since canFinish will change to false.
      // Ideally this should be a state machine where kills are issued to the executor,
      // and the structures are cleaned up once all tasks complete. New requests, however,
      // should not be allowed after a query complete is received.
      sourceCompletionMap.remove(queryIdentifier);
      String savedQueryId = queryIdentifierToHiveQueryId.remove(queryIdentifier);
      if (!isExternalQuery) {
        removeQuerySpecificLock(queryIdentifier);
      }
      if (savedQueryId != null) {
        ObjectCacheFactory.removeLlapQueryCache(savedQueryId);
      }
      return queryInfo;
    } finally {
      dagLock.writeLock().unlock();
    }
  }


  private void cleanupLocalDirs(QueryInfo queryInfo, long deleteDelay) {
    String[] localDirs = queryInfo.getLocalDirsNoCreate();
    if (localDirs != null) {
      for (String localDir : localDirs) {
        cleanupDir(localDir, deleteDelay);
        ShuffleHandler.get().unregisterDag(localDir, queryInfo.getAppIdString(), queryInfo.getDagIdentifier());
      }
    }
  }

  private void handleLogOnQueryCompletion(String queryIdString, String dagIdString) {
    if (routeBasedLoggingEnabled) {
      // Inform the routing purgePolicy.
      // Send out a fake log message at the ERROR level with the MDC for this query setup. With an
      // LLAP custom appender this message will not be logged.
      MDC.put("dagId", dagIdString);
      MDC.put("queryId", queryIdString);
      try {
        LOG.error(QUERY_COMPLETE_MARKER, "Ignore this. Log line to interact with logger." +
            " Query complete: " + queryIdString + ", " +
            dagIdString);
      } finally {
        MDC.clear();
      }
    }
  }

  private void removeQuerySpecificLock(QueryIdentifier queryIdentifier) {
    dagSpecificLocks.remove(queryIdentifier);
  }


  public void rememberCompletedDag(QueryIdentifier queryIdentifier) {
    if (completedDagMap.add(queryIdentifier)) {
      // We will remember completed DAG for an hour to avoid execution out-of-order fragments.
      executorService.schedule(new DagMapCleanerCallable(queryIdentifier), 1, TimeUnit.HOURS);
    } else {
      LOG.warn("Couldn't add {} to completed dag set", queryIdentifier);
    }
  }

  /**
   * Register an update to a source within an executing dag
   * @param queryIdentifier
   * @param sourceName
   * @param sourceState
   */
  void registerSourceStateChange(QueryIdentifier queryIdentifier, String sourceName,
      SourceStateProto sourceState) throws IOException {
    getSourceCompletionMap(queryIdentifier).put(sourceName, sourceState);
    QueryInfo queryInfo = checkPermissionsAndGetQuery(queryIdentifier);
    if (queryInfo != null) {
      queryInfo.sourceStateUpdated(sourceName);
    } else {
      // Could be null if there's a race between the threads processing requests, with a
      // dag finish processed earlier.
    }
  }

  private ReentrantReadWriteLock getDagLockNoCreate(QueryIdentifier queryIdentifier) {
    return dagSpecificLocks.get(queryIdentifier);
  }

  private ReentrantReadWriteLock getDagLock(QueryIdentifier queryIdentifier) {
    lock.lock();
    try {
      ReentrantReadWriteLock dagLock = dagSpecificLocks.get(queryIdentifier);
      if (dagLock == null) {
        dagLock = new ReentrantReadWriteLock();
        dagSpecificLocks.put(queryIdentifier, dagLock);
      }
      return dagLock;
    } finally {
      lock.unlock();
    }
  }

  private ConcurrentMap<String, SourceStateProto> getSourceCompletionMap(QueryIdentifier queryIdentifier) {
    ConcurrentMap<String, SourceStateProto> dagMap = sourceCompletionMap.get(queryIdentifier);
    if (dagMap == null) {
      dagMap = new ConcurrentHashMap<>();
      ConcurrentMap<String, SourceStateProto> old =
          sourceCompletionMap.putIfAbsent(queryIdentifier, dagMap);
      dagMap = (old != null) ? old : dagMap;
    }
    return dagMap;
  }

  @Override
  public void serviceStart() {
    LOG.info(getName() + " started");
  }

  @Override
  public void serviceStop() {
    executorService.shutdownNow();
    LOG.info(getName() + " stopped");
  }

  private void cleanupDir(String dir, long deleteDelay) {
    LOG.info("Scheduling deletion of {} after {} seconds", dir, deleteDelay);
    executorService.schedule(new FileCleanerCallable(dir), deleteDelay, TimeUnit.SECONDS);
  }

  private class FileCleanerCallable extends CallableWithNdc<Void> {
    private final String dirToDelete;

    private FileCleanerCallable(String dirToDelete) {
      this.dirToDelete = dirToDelete;
    }

    @Override
    protected Void callInternal() {
      Path pathToDelete = new Path(dirToDelete);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Deleting path: " + pathToDelete);
      }
      try {
        localFs.delete(new Path(dirToDelete), true);
      } catch (IOException e) {
        LOG.warn("Ignoring exception while cleaning up path: " + pathToDelete, e);
      }
      return null;
    }
  }

  private class DagMapCleanerCallable extends CallableWithNdc<Void> {
    private final QueryIdentifier queryIdentifier;

    private DagMapCleanerCallable(QueryIdentifier queryIdentifier) {
      this.queryIdentifier = queryIdentifier;
    }

    @Override
    protected Void callInternal() {
      completedDagMap.remove(queryIdentifier);
      return null;
    }
  }

  private class ExternalQueryCleanerCallable extends CallableWithNdc<Void> {

    private final String queryIdString;
    private final String dagIdString;
    private final QueryIdentifier queryIdentifier;

    public ExternalQueryCleanerCallable(String queryIdString, String dagIdString,
                                        QueryIdentifier queryIdentifier) {
      this.queryIdString = queryIdString;
      this.dagIdString = dagIdString;
      this.queryIdentifier = queryIdentifier;
    }

    @Override
    protected Void callInternal() {
      LOG.info("External cleanup callable for {}", queryIdentifier);
      ReentrantReadWriteLock dagLock = getDagLockNoCreate(queryIdentifier);
      if (dagLock == null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("null dagLock. No cleanup required at the moment for {}", queryIdString);
        }
        return null;
      }
      boolean locked = dagLock.writeLock().tryLock();
      if (!locked) {
        // Something else holds the lock at the moment. Don't bother cleaning up.
        if (LOG.isTraceEnabled()) {
          LOG.trace("Lock not obtained. Skipping cleanup for {}", queryIdString);
        }
        return null;
      }
      try {
        // See if there are additional knownFragments. If there are, more fragments came in
        // after this cleanup was scheduled, and there's nothing to be done.
        QueryInfo queryInfo = queryInfoMap.get(queryIdentifier);
        if (queryInfo != null) {
          // QueryInfo will only exist if more work came in, after this was scheduled.
          if (LOG.isTraceEnabled()) {
            LOG.info("QueryInfo found for {}. Expecting future cleanup", queryIdString);
          }
          return null;
        }
        LOG.info("Processing cleanup for {}", queryIdString);
        handleLogOnQueryCompletion(queryIdString, dagIdString);
        removeQuerySpecificLock(queryIdentifier);
      } finally {
        dagLock.writeLock().unlock();
      }
      return null;
    }
  }

  private QueryInfo checkPermissionsAndGetQuery(QueryIdentifier queryId) throws IOException {
    QueryInfo queryInfo = queryInfoMap.get(queryId);
    if (queryInfo == null) return null;
    LlapTokenChecker.checkPermissions(clusterId, queryInfo.getTokenUserName(),
        queryInfo.getTokenAppId(), queryInfo.getQueryIdentifier());
    return queryInfo;
  }

  public boolean checkPermissionsForQuery(QueryIdentifier queryId) throws IOException {
    return checkPermissionsAndGetQuery(queryId) != null;
  }


  private void handleFragmentCompleteExternalQuery(QueryInfo queryInfo) {
    if (queryInfo.isExternalQuery()) {
      ReentrantReadWriteLock dagLock = getDagLock(queryInfo.getQueryIdentifier());
      if (dagLock == null) {
        LOG.warn("Ignoring fragment completion for unknown query: {}",
            queryInfo.getQueryIdentifier());
      }
      boolean locked = dagLock.writeLock().tryLock();
      if (!locked) {
        // Some other operation in progress using the same lock.
        // A subsequent fragmentComplete is expected to come in.
        return;
      }
      try {
         if (queryInfo.getRegisteredFragments().size() == 0) {
           queryComplete(queryInfo.getQueryIdentifier(), -1, true);
         } else {
           if (LOG.isTraceEnabled()) {
             LOG.trace(
                 "Not invoking queryComplete on fragmentComplete for {}, since there are known fragments. count={}",
                 queryInfo.getHiveQueryIdString(), queryInfo.getRegisteredFragments().size());
           }
         }
      } catch (IOException e) {
        LOG.error("Failed to process query complete for external submission: {}",
            queryInfo.getQueryIdentifier());
      } finally {
        dagLock.writeLock().unlock();
      }
    }
  }
}
