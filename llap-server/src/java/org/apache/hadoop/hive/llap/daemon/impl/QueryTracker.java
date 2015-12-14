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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.fs.Path;
import org.apache.tez.common.CallableWithNdc;

import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.configuration.LlapConfiguration;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.FragmentSpecProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateProto;
import org.apache.hadoop.hive.llap.shufflehandler.ShuffleHandler;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * Tracks queries running within a daemon
 */
public class QueryTracker extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(QueryTracker.class);

  private final ScheduledExecutorService executorService;

  // TODO Make use if the query id for cachin when this is available.
  private final ConcurrentHashMap<String, QueryInfo> queryInfoMap = new ConcurrentHashMap<>();

  private final String[] localDirsBase;
  private final FileSystem localFs;
  private final long defaultDeleteDelaySeconds;

  // TODO At the moment there's no way of knowing whether a query is running or not.
  // A race is possible between dagComplete and registerFragment - where the registerFragment
  // is processed after a dagCompletes.
  // May need to keep track of completed dags for a certain time duration to avoid this.
  // Alternately - send in an explicit dag start message before any other message is processed.
  // Multiple threads communicating from a single AM gets in the way of this.

  // Keeps track of completed dags. Assumes dag names are unique across AMs.
  private final Set<String> completedDagMap = Collections.newSetFromMap(
      new ConcurrentHashMap<String, Boolean>());


  private final Lock lock = new ReentrantLock();
  private final ConcurrentMap<String, ReadWriteLock> dagSpecificLocks = new ConcurrentHashMap<>();

  // Tracks various maps for dagCompletions. This is setup here since stateChange messages
  // may be processed by a thread which ends up executing before a task.
  private final ConcurrentMap<String, ConcurrentMap<String, SourceStateProto>>
    sourceCompletionMap = new ConcurrentHashMap<>();

  // Tracks queryId by dagName. This can only be set when config is parsed in TezProcessor,
  // all the other existing code passes queryId equal to 0 everywhere.
  private final ConcurrentHashMap<String, String> dagNameToQueryId = new ConcurrentHashMap<>();

  public QueryTracker(Configuration conf, String[] localDirsBase) {
    super("QueryTracker");
    this.localDirsBase = localDirsBase;
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
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("QueryFileCleaner %d").build());
  }

  /**
   * Register a new fragment for a specific query
   * @param queryId
   * @param appIdString
   * @param dagName
   * @param dagIdentifier
   * @param vertexName
   * @param fragmentNumber
   * @param attemptNumber
   * @param user
   * @throws IOException
   */
  QueryFragmentInfo registerFragment(String queryId, String appIdString, String dagName,
      int dagIdentifier, String vertexName, int fragmentNumber, int attemptNumber, String user,
      FragmentSpecProto fragmentSpec) throws IOException {
    ReadWriteLock dagLock = getDagLock(dagName);
    dagLock.readLock().lock();
    try {
      if (!completedDagMap.contains(dagName)) {
        QueryInfo queryInfo = queryInfoMap.get(dagName);
        if (queryInfo == null) {
          queryInfo = new QueryInfo(queryId, appIdString, dagName, dagIdentifier, user,
              getSourceCompletionMap(dagName), localDirsBase, localFs);
          queryInfoMap.putIfAbsent(dagName, queryInfo);
        }
        return queryInfo.registerFragment(vertexName, fragmentNumber, attemptNumber, fragmentSpec);
      } else {
        // Cleanup the dag lock here, since it may have been created after the query completed
        dagSpecificLocks.remove(dagName);
        throw new RuntimeException(
            "Dag " + dagName + " already complete. Rejecting fragment ["
                + vertexName + ", " + fragmentNumber + ", " + attemptNumber + "]");
      }
    } finally {
      dagLock.readLock().unlock();
    }
  }

  /**
   * Indicate to the tracker that a fragment is complete. This is from internal execution within the daemon
   * @param fragmentInfo
   */
  void fragmentComplete(QueryFragmentInfo fragmentInfo) {
    String dagName = fragmentInfo.getQueryInfo().getDagName();
    QueryInfo queryInfo = queryInfoMap.get(dagName);
    if (queryInfo == null) {
      // Possible because a queryComplete message from the AM can come in first - KILL / SUCCESSFUL,
      // before the fragmentComplete is reported
      LOG.info("Ignoring fragmentComplete message for unknown query");
    } else {
      queryInfo.unregisterFragment(fragmentInfo);
    }
  }

  /**
   * Register completion for a query
   * @param queryId
   * @param dagName
   * @param deleteDelay
   */
  List<QueryFragmentInfo> queryComplete(String queryId, String dagName, long deleteDelay) {
    if (deleteDelay == -1) {
      deleteDelay = defaultDeleteDelaySeconds;
    }
    ReadWriteLock dagLock = getDagLock(dagName);
    dagLock.writeLock().lock();
    try {
      rememberCompletedDag(dagName);
      LOG.info("Processing queryComplete for dagName={} with deleteDelay={} seconds",
          dagName, deleteDelay);
      QueryInfo queryInfo = queryInfoMap.remove(dagName);
      if (queryInfo == null) {
        LOG.warn("Ignoring query complete for unknown dag: {}", dagName);
        return Collections.emptyList();
      }
      String[] localDirs = queryInfo.getLocalDirsNoCreate();
      if (localDirs != null) {
        for (String localDir : localDirs) {
          cleanupDir(localDir, deleteDelay);
          ShuffleHandler.get().unregisterDag(localDir, dagName, queryInfo.getDagIdentifier());
        }
      }
      // Clearing this before sending a kill is OK, since canFinish will change to false.
      // Ideally this should be a state machine where kills are issued to the executor,
      // and the structures are cleaned up once all tasks complete. New requests, however,
      // should not be allowed after a query complete is received.
      sourceCompletionMap.remove(dagName);
      String savedQueryId = dagNameToQueryId.remove(dagName);
      queryId = queryId == null ? savedQueryId : queryId;
      dagSpecificLocks.remove(dagName);
      if (queryId != null) {
        ObjectCacheFactory.removeLlapQueryCache(queryId);
      }
      return queryInfo.getRegisteredFragments();
    } finally {
      dagLock.writeLock().unlock();
    }
  }



  public void rememberCompletedDag(String dagName) {
    if (completedDagMap.add(dagName)) {
      // We will remember completed DAG for an hour to avoid execution out-of-order fragments.
      executorService.schedule(new DagMapCleanerCallable(dagName), 1, TimeUnit.HOURS);
    } else {
      LOG.warn("Couldn't add {} to completed dag set", dagName);
    }
  }

  /**
   * Register an update to a source within an executing dag
   * @param dagName
   * @param sourceName
   * @param sourceState
   */
  void registerSourceStateChange(String dagName, String sourceName, SourceStateProto sourceState) {
    getSourceCompletionMap(dagName).put(sourceName, sourceState);
    QueryInfo queryInfo = queryInfoMap.get(dagName);
    if (queryInfo != null) {
      queryInfo.sourceStateUpdated(sourceName);
    } else {
      // Could be null if there's a race between the threads processing requests, with a
      // dag finish processed earlier.
    }
  }


  private ReadWriteLock getDagLock(String dagName) {
    lock.lock();
    try {
      ReadWriteLock dagLock = dagSpecificLocks.get(dagName);
      if (dagLock == null) {
        dagLock = new ReentrantReadWriteLock();
        dagSpecificLocks.put(dagName, dagLock);
      }
      return dagLock;
    } finally {
      lock.unlock();
    }
  }

  private ConcurrentMap<String, SourceStateProto> getSourceCompletionMap(String dagName) {
    ConcurrentMap<String, SourceStateProto> dagMap = sourceCompletionMap.get(dagName);
    if (dagMap == null) {
      dagMap = new ConcurrentHashMap<>();
      ConcurrentMap<String, SourceStateProto> old =
          sourceCompletionMap.putIfAbsent(dagName, dagMap);
      dagMap = (old != null) ? old : dagMap;
    }
    return dagMap;
  }

  public void registerDagQueryId(String dagName, String queryId) {
    if (queryId == null) return;
    dagNameToQueryId.putIfAbsent(dagName, queryId);
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
    private final String dagName;

    private DagMapCleanerCallable(String dagName) {
      this.dagName = dagName;
    }

    @Override
    protected Void callInternal() {
      completedDagMap.remove(dagName);
      return null;
    }
  }
}
