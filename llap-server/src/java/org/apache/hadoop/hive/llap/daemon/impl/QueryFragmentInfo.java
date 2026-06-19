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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.llap.daemon.FinishableStateUpdateHandler;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.IOSpecProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SignableVertexSpec;
import org.apache.hadoop.hive.llap.tezplugins.LlapTezUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryFragmentInfo {

  private static final Logger LOG = LoggerFactory.getLogger(QueryFragmentInfo.class);

  private final QueryInfo queryInfo;
  private final String vertexName;
  private final int fragmentNumber;
  private final int attemptNumber;
  private final SignableVertexSpec vertexSpec;
  private final String fragmentIdString;
  private boolean canFinishForPriority;

  public QueryFragmentInfo(QueryInfo queryInfo, String vertexName, int fragmentNumber,
      int attemptNumber, SignableVertexSpec vertexSpec, String fragmentIdString) {
    Preconditions.checkNotNull(queryInfo);
    Preconditions.checkNotNull(vertexName);
    Preconditions.checkNotNull(vertexSpec);
    this.queryInfo = queryInfo;
    this.vertexName = vertexName;
    this.fragmentNumber = fragmentNumber;
    this.attemptNumber = attemptNumber;
    this.vertexSpec = vertexSpec;
    this.fragmentIdString = fragmentIdString;
    this.canFinishForPriority = false; // Updated when we add this to the queue.
  }

  // Only meant for use by the QueryTracker
  QueryInfo getQueryInfo() {
    return this.queryInfo;
  }

  public SignableVertexSpec getVertexSpec() {
    return vertexSpec;
  }

  public String getVertexName() {
    return vertexName;
  }

  public int getFragmentNumber() {
    return fragmentNumber;
  }

  public int getAttemptNumber() {
    return attemptNumber;
  }

  public String getFragmentIdentifierString() {
    return fragmentIdString;
  }

  /**
   * Unlike canFinish, this CANNOT be derived dynamically; a change without a reinsert will
   * cause the queue order to become incorrect.
   */
  public boolean canFinishForPriority() {
    return canFinishForPriority;
  }

  /**
   * This MUST be called when the fragment is NOT in wait queue.
   */
  public void setCanFinishForPriority(boolean value) {
    canFinishForPriority = value;
  }

  /**
   * Check whether a task can run to completion or may end up blocking on it's sources.
   * This currently happens via looking up source state.
   * TODO: Eventually, this should lookup the Hive Processor to figure out whether
   * it's reached a state where it can finish - especially in cases of failures
   * after data has been fetched.
   *
   * @return true if the task can finish, false otherwise
   */
  public static boolean canFinish(QueryFragmentInfo fragment) {
    return fragment.canFinish();
  }

  // Hide this so it doesn't look like a simple property.
  private boolean canFinish() {
    List<IOSpecProto> inputSpecList = vertexSpec.getInputSpecsList();
    boolean canFinish = true;
    if (inputSpecList != null && !inputSpecList.isEmpty()) {
      for (IOSpecProto inputSpec : inputSpecList) {
        if (LlapTezUtils.isSourceOfInterest(inputSpec.getIoDescriptor().getClassName())) {
          // Lookup the state in the map.
          LlapDaemonProtocolProtos.SourceStateProto state = queryInfo.getSourceStateMap()
              .get(inputSpec.getConnectedVertexName());
          if (state != null && state == LlapDaemonProtocolProtos.SourceStateProto.S_SUCCEEDED) {
            continue;
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Cannot finish due to source: " + inputSpec.getConnectedVertexName());
            }
            canFinish = false;
            break;
          }
        }
      }
    }
    return canFinish;
  }

  /**
   * Get, and create if required, local-dirs for a fragment
   * @return
   * @throws IOException
   */
  public String[] getLocalDirs() throws IOException {
    return queryInfo.getLocalDirs();
  }

  /**
   *
   * @param handler
   * @param lastFinishableState
   * @return true if the current state is the same as the lastFinishableState. false if the state has already changed.
   */
  public boolean registerForFinishableStateUpdates(FinishableStateUpdateHandler handler,
                                                boolean lastFinishableState) {
    List<String> sourcesOfInterest = new LinkedList<>();
    List<IOSpecProto> inputSpecList = vertexSpec.getInputSpecsList();
    if (inputSpecList != null && !inputSpecList.isEmpty()) {
      for (IOSpecProto inputSpec : inputSpecList) {
        if (LlapTezUtils.isSourceOfInterest(inputSpec.getIoDescriptor().getClassName())) {
          sourcesOfInterest.add(inputSpec.getConnectedVertexName());
        }
      }
    }
    return queryInfo.registerForFinishableStateUpdates(handler, sourcesOfInterest, this,
        lastFinishableState);
  }


  public void unregisterForFinishableStateUpdates(FinishableStateUpdateHandler handler) {
    queryInfo.unregisterFinishableStateUpdate(handler);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    QueryFragmentInfo that = (QueryFragmentInfo) o;

    if (fragmentNumber != that.fragmentNumber) {
      return false;
    }
    if (attemptNumber != that.attemptNumber) {
      return false;
    }
    return vertexName.equals(that.vertexName);

  }

  @Override
  public int hashCode() {
    int result = vertexName.hashCode();
    result = 31 * result + fragmentNumber;
    result = 31 * result + attemptNumber;
    return result;
  }
}
