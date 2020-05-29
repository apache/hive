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
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.llap.LlapNodeId;
import org.apache.hadoop.hive.llap.daemon.FinishableStateUpdateHandler;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SignableVertexSpec;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateProto;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.tez.common.security.JobTokenIdentifier;

public class QueryInfo {
  private final QueryIdentifier queryIdentifier;
  private final String appIdString;
  private final String dagIdString;
  private final String dagName;
  private final String hiveQueryIdString;
  private final int dagIdentifier;
  private final String user;
  private final String[] localDirsBase;
  private final FileSystem localFs;
  private String[] localDirs;
  private final LlapNodeId amNodeId;
  private final String appTokenIdentifier;
  private final Token<JobTokenIdentifier> appToken;
  private final boolean isExternalQuery;
  // Map of states for different vertices.

  private final Set<QueryFragmentInfo> knownFragments =
      Collections.newSetFromMap(new ConcurrentHashMap<QueryFragmentInfo, Boolean>());

  private final ConcurrentMap<String, SourceStateProto> sourceStateMap;

  private final FinishableStateTracker finishableStateTracker = new FinishableStateTracker();
  private final String tokenUserName, appId;
  private final ContainerRunnerImpl.UgiPool ugiPool;

  public QueryInfo(QueryIdentifier queryIdentifier, String appIdString, String dagIdString,
    String dagName, String hiveQueryIdString,
    int dagIdentifier, String user,
    ConcurrentMap<String, SourceStateProto> sourceStateMap,
    String[] localDirsBase, FileSystem localFs, String tokenUserName,
    String tokenAppId, final LlapNodeId amNodeId,
    String tokenIdentifier,
    Token<JobTokenIdentifier> appToken,
    boolean isExternalQuery, ContainerRunnerImpl.UgiPool ugiPool) {
    this.queryIdentifier = queryIdentifier;
    this.appIdString = appIdString;
    this.dagIdString = dagIdString;
    this.dagName = dagName;
    this.hiveQueryIdString = hiveQueryIdString;
    this.dagIdentifier = dagIdentifier;
    this.sourceStateMap = sourceStateMap;
    this.user = user;
    this.localDirsBase = localDirsBase;
    this.localFs = localFs;
    this.tokenUserName = tokenUserName;
    this.appId = tokenAppId;
    this.amNodeId = amNodeId;
    this.appTokenIdentifier = tokenIdentifier;
    this.appToken = appToken;
    this.isExternalQuery = isExternalQuery;
    this.ugiPool = ugiPool;
    final InetSocketAddress address =
        NetUtils.createSocketAddrForHost(amNodeId.getHostname(), amNodeId.getPort());
    SecurityUtil.setTokenService(appToken, address);
    // TODO Caching this and re-using across submissions breaks AM recovery, since the
    // new AM may run on a different host/port.
  }

  public QueryIdentifier getQueryIdentifier() {
    return queryIdentifier;
  }

  public String getAppIdString() {
    return appIdString;
  }

  public String getDagIdString() {
    return dagIdString;
  }

  public String getHiveQueryIdString() {
    return hiveQueryIdString;
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

  public LlapNodeId getAmNodeId() {
    return amNodeId;
  }

  public QueryFragmentInfo registerFragment(String vertexName, int fragmentNumber,
      int attemptNumber, SignableVertexSpec vertexSpec, String fragmentIdString) {
    QueryFragmentInfo fragmentInfo = new QueryFragmentInfo(
        this, vertexName, fragmentNumber, attemptNumber, vertexSpec, fragmentIdString);
    boolean wasUniqueFragment = knownFragments.add(fragmentInfo);
    if (!wasUniqueFragment) {
      // The same query fragment (including attempt number) has already been registered.
      // This could potentially occur for external clients that are trying to submit the
      // exact same fragment more than once (for example speculative execution of a query fragment).
      // This should not occur for a non-external query fragment.
      // Either way, registering the same fragment twice should be disallowed as the LLAP structures
      // it will only ever have a single submission of a fragment.
      String message = "Fragment " + fragmentIdString + "(isExternal=" + isExternalQuery()
              + ") has already been registered.";
      throw new IllegalArgumentException(message);
    }
    return fragmentInfo;
  }

  public void unregisterFragment(QueryFragmentInfo fragmentInfo) {
    knownFragments.remove(fragmentInfo);
  }

  public List<QueryFragmentInfo> getRegisteredFragments() {
    return Lists.newArrayList(knownFragments);
  }

  public boolean isExternalQuery() {
    return isExternalQuery;
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
    // May work via YARN Service - since the directory would already exist. Otherwise may need a custom shuffle handler.
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

    private final ReentrantLock lock = new ReentrantLock();

    boolean registerForUpdates(FinishableStateUpdateHandler handler,
                                         List<String> sources, QueryFragmentInfo fragmentInfo,
                                         boolean lastFinishableState) {
      lock.lock();
      try {
        EntityInfo entityInfo =
            new EntityInfo(handler, sources, fragmentInfo, lastFinishableState);
        if (trackedEntities.put(handler, entityInfo) != null) {
          throw new IllegalStateException(
              "Only a single registration allowed per entity. Duplicate for " + handler.toString());
        }
        for (String source : sources) {
          sourceToEntity.put(source, entityInfo);
        }

        boolean canFinish = QueryFragmentInfo.canFinish(fragmentInfo);
        if (lastFinishableState == canFinish) {
          // State has not changed.
          return true;
        } else {
          entityInfo.setLastFinishableState(canFinish);
          return false;
        }
      } finally {
        lock.unlock();
      }
    }

    void unregisterForUpdates(FinishableStateUpdateHandler handler) {
      lock.lock();
      try {
        EntityInfo info = trackedEntities.remove(handler);
        Preconditions.checkState(info != null,
            "Cannot invoke unregister on an entity which has not been registered");
        for (String source : info.getSources()) {
          sourceToEntity.remove(source, info);
        }
      } finally {
        lock.unlock();
      }
    }

    void sourceStateUpdated(String sourceName) {
      List<EntityInfo> interestedEntityInfos = null;
      lock.lock();
      try {
        Collection<EntityInfo> entities = sourceToEntity.get(sourceName);
        if (entities != null) {
          // Create a copy since the underlying list can be changed elsewhere.
          interestedEntityInfos = new LinkedList<>(entities);
        }
      } finally {
        lock.unlock();
      }
      if (interestedEntityInfos != null) {
        for (EntityInfo entityInfo : interestedEntityInfos) {
          boolean newFinishState = QueryFragmentInfo.canFinish(entityInfo.getFragmentInfo());
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

  public String getTokenUserName() {
    return tokenUserName;
  }

  public String getTokenAppId() {
    return appId;
  }

  UserGroupInformation getUmbilicalUgi() throws ExecutionException {
    return ugiPool.getUmbilicalUgi(appTokenIdentifier, appToken);
  }

  void returnUmbilicalUgi(UserGroupInformation ugi) {
    ugiPool.returnUmbilicalUgi(appTokenIdentifier, ugi);
  }
}
