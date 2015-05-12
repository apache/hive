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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.llap.shufflehandler.ShuffleHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks queries running within a daemon
 */
public class QueryTracker {

  private static final Logger LOG = LoggerFactory.getLogger(QueryTracker.class);
  private final QueryFileCleaner queryFileCleaner;

  // TODO Make use if the query id for cachin when this is available.
  private final ConcurrentHashMap<String, QueryInfo> queryInfoMap = new ConcurrentHashMap<>();

  private final String[] localDirsBase;
  private final FileSystem localFs;

  public QueryTracker(Configuration conf, String[] localDirsBase) {
    this.localDirsBase = localDirsBase;
    try {
      localFs = FileSystem.getLocal(conf);
    } catch (IOException e) {
      throw new RuntimeException("Failed to setup local filesystem instance", e);
    }
    queryFileCleaner = new QueryFileCleaner(conf, localFs);
    queryFileCleaner.init(conf);
    queryFileCleaner.start();
  }


  void registerFragment(String queryId, String appIdString, String dagName, int dagIdentifier,
                        String vertexName, int fragmentNumber, int attemptNumber,
                        String user) throws
      IOException {
    QueryInfo queryInfo = queryInfoMap.get(dagName);
    if (queryInfo == null) {
      queryInfo = new QueryInfo(queryId, appIdString, dagName, dagIdentifier, user);
      queryInfoMap.putIfAbsent(dagName, queryInfo);
    }
    // TODO Start tracking individual fragments, so that taskKilled etc messages
    // can be routed through this layer to simplify the interfaces.
  }

  String[] getLocalDirs(String queryId, String dagName, String user) throws IOException {
    QueryInfo queryInfo = queryInfoMap.get(dagName);
    return queryInfo.getLocalDirs();
  }

  void queryComplete(String queryId, String dagName, long deleteDelay) {
    LOG.info("Processing queryComplete for dagName={} with deleteDelay={} seconds", dagName, deleteDelay);
    QueryInfo queryInfo = queryInfoMap.remove(dagName);
    if (queryInfo == null) {
      LOG.warn("Ignoring query complete for unknown dag: {}", dagName);
    }
    String []localDirs = queryInfo.getLocalDirsNoCreate();
    if (localDirs != null) {
      for (String localDir : localDirs) {
        queryFileCleaner.cleanupDir(localDir, deleteDelay);
        ShuffleHandler.get().unregisterDag(localDir, dagName, queryInfo.dagIdentifier);
      }
    }
    // TODO HIVE-10535 Cleanup map join cache
  }

  void shutdown() {
    queryFileCleaner.stop();
  }


  private class QueryInfo {

    private final String queryId;
    private final String appIdString;
    private final String dagName;
    private final int dagIdentifier;
    private final String user;
    private String[] localDirs;

    public QueryInfo(String queryId, String appIdString, String dagName, int dagIdentifier,
                     String user) {
      this.queryId = queryId;
      this.appIdString = appIdString;
      this.dagName = dagName;
      this.dagIdentifier = dagIdentifier;
      this.user = user;
    }




    private synchronized void createLocalDirs() throws IOException {
      if (localDirs == null) {
        localDirs = new String[localDirsBase.length];
        for (int i = 0; i < localDirsBase.length; i++) {
          localDirs[i] = createAppSpecificLocalDir(localDirsBase[i], appIdString, user, dagIdentifier);
          localFs.mkdirs(new Path(localDirs[i]));
        }
      }
    }

    private synchronized String[] getLocalDirs() throws IOException {
      if (localDirs == null) {
        createLocalDirs();
      }
      return localDirs;
    }

    private synchronized String[] getLocalDirsNoCreate() {
      return this.localDirs;
    }
  }

  private static String createAppSpecificLocalDir(String baseDir, String applicationIdString,
                                                  String user, int dagIdentifier) {
    // TODO This is broken for secure clusters. The app will not have permission to create these directories.
    // May work via Slider - since the directory would already exist. Otherwise may need a custom shuffle handler.
    // TODO This should be the process user - and not the user on behalf of whom the query is being submitted.
    return baseDir + File.separator + "usercache" + File.separator + user + File.separator +
        "appcache" + File.separator + applicationIdString + File.separator + dagIdentifier;
  }
}
