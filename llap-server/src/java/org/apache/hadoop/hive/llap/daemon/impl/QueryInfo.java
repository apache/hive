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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.llap.daemon.FinishableStateUpdateHandler;
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

  private final FinishableStateTracker finishableStateTracker = new FinishableStateTracker();

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

  public List<QueryFragmentInfo> getRegisteredFragments() {
    return Lists.newArrayList(knownFragments);
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

  /**
   *
   * @param handler
   * @param sources
   * @param fragmentInfo
   * @param lastFinishableState
   * @return true if the current state is the same as the lastFinishableState. false if the state has already changed.
   */
  boolean registerForFinishableStateUpdates(FinishableStateUpdateHandler handler,
                                         List<String> sources, QueryFragmentInfo fragmentInfo,
                                         boolean lastFinishableState) {
    return finishableStateTracker
        .registerForUpdates(handler, sources, fragmentInfo, lastFinishableState);
  }

  void unregisterFinishableStateUpdate(FinishableStateUpdateHandler handler) {
    finishableStateTracker.unregisterForUpdates(handler);
  }

  void sourceStateUpdated(String sourceName) {
    finishableStateTracker.sourceStateUpdated(sourceName);
  }


  private static class FinishableStateTracker {

    private final Map<FinishableStateUpdateHandler, EntityInfo> trackedEntities = new HashMap<>();
    private final Multimap<String, EntityInfo> sourceToEntity = HashMultimap.create();

    synchronized boolean registerForUpdates(FinishableStateUpdateHandler handler,
                                         List<String> sources, QueryFragmentInfo fragmentInfo,
                                         boolean lastFinishableState) {
      EntityInfo entityInfo =
          new EntityInfo(handler, sources, fragmentInfo, lastFinishableState);
      if (trackedEntities.put(handler, entityInfo) != null) {
        throw new IllegalStateException(
            "Only a single registration allowed per entity. Duplicate for " + handler.toString());
      }
      for (String source : sources) {
        sourceToEntity.put(source, entityInfo);
      }

      if (lastFinishableState != fragmentInfo.canFinish()) {
        entityInfo.setLastFinishableState(fragmentInfo.canFinish());
        return false;
      } else {
        return true;
      }
    }

    synchronized void unregisterForUpdates(FinishableStateUpdateHandler handler) {
      EntityInfo info = trackedEntities.remove(handler);
      Preconditions.checkState(info != null, "Cannot invoke unregister on an entity which has not been registered");
      for (String source : info.getSources()) {
        sourceToEntity.remove(source, info);
      }
    }

    synchronized void sourceStateUpdated(String sourceName) {
      Collection<EntityInfo> interestedEntityInfos = sourceToEntity.get(sourceName);
      if (interestedEntityInfos != null) {
        for (EntityInfo entityInfo : interestedEntityInfos) {
          boolean newFinishState = entityInfo.getFragmentInfo().canFinish();
          if (newFinishState != entityInfo.getLastFinishableState()) {
            // State changed. Callback
            entityInfo.setLastFinishableState(newFinishState);
            entityInfo.getHandler().finishableStateUpdated(newFinishState);
          }
        }
      }
    }


  }

  private static class EntityInfo {
    final FinishableStateUpdateHandler handler;
    final List<String> sources;
    final QueryFragmentInfo fragmentInfo;
    boolean lastFinishableState;

    public EntityInfo(FinishableStateUpdateHandler handler, List<String> sources, QueryFragmentInfo fragmentInfo, boolean lastFinishableState) {
      this.handler = handler;
      this.sources = sources;
      this.fragmentInfo = fragmentInfo;
      this.lastFinishableState = lastFinishableState;
    }

    public FinishableStateUpdateHandler getHandler() {
      return handler;
    }

    public QueryFragmentInfo getFragmentInfo() {
      return fragmentInfo;
    }

    public boolean getLastFinishableState() {
      return lastFinishableState;
    }

    public List<String> getSources() {
      return sources;
    }

    public void setLastFinishableState(boolean lastFinishableState) {
      this.lastFinishableState = lastFinishableState;
    }
  }
}
