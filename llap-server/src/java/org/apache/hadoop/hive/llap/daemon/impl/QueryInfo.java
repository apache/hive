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

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.FragmentSpecProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateProto;

public class QueryInfo {
  private final String queryId;
  private final String appIdString;
  private final String dagName;
  private final int dagIdentifier;
  private final String user;
  private final String[] localDirsBase;
  private final FileSystem localFs;
  private String[] localDirs;
  // Map of states for different vertices.

  private final Set<QueryFragmentInfo> knownFragments =
      Collections.newSetFromMap(new ConcurrentHashMap<QueryFragmentInfo, Boolean>());

  private final ConcurrentMap<String, SourceStateProto> sourceStateMap;


  public QueryInfo(String queryId, String appIdString, String dagName, int dagIdentifier,
                   String user, ConcurrentMap<String, SourceStateProto> sourceStateMap,
                   String[] localDirsBase, FileSystem localFs) {
    this.queryId = queryId;
    this.appIdString = appIdString;
    this.dagName = dagName;
    this.dagIdentifier = dagIdentifier;
    this.sourceStateMap = sourceStateMap;
    this.user = user;
    this.localDirsBase = localDirsBase;
    this.localFs = localFs;
  }

  public String getQueryId() {
    return queryId;
  }

  public String getAppIdString() {
    return appIdString;
  }

  public String getDagName() {
    return dagName;
  }

  public int getDagIdentifier() {
    return dagIdentifier;
  }

  public String getUser() {
    return user;
  }

  public ConcurrentMap<String, SourceStateProto> getSourceStateMap() {
    return sourceStateMap;
  }

  public QueryFragmentInfo registerFragment(String vertexName, int fragmentNumber, int attemptNumber, FragmentSpecProto fragmentSpec) {
    QueryFragmentInfo fragmentInfo = new QueryFragmentInfo(this, vertexName, fragmentNumber, attemptNumber,
        fragmentSpec);
    knownFragments.add(fragmentInfo);
    return fragmentInfo;
  }

  public void unregisterFragment(QueryFragmentInfo fragmentInfo) {
    knownFragments.remove(fragmentInfo);
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

  /**
   * Get, and create if required, local-dirs for a query
   * @return
   * @throws IOException
   */
  public synchronized String[] getLocalDirs() throws IOException {
    if (localDirs == null) {
      createLocalDirs();
    }
    return localDirs;
  }

  public synchronized String[] getLocalDirsNoCreate() {
    return this.localDirs;
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
