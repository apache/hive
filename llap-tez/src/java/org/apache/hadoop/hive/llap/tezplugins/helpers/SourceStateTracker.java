/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.llap.tezplugins.helpers;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.QueryIdentifierProto;
import org.apache.hadoop.hive.llap.tezplugins.LlapTezUtils;
import org.apache.tez.serviceplugins.api.TaskCommunicatorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.llap.LlapNodeId;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.FragmentRuntimeInfo;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateUpdatedRequestProto;
import org.apache.hadoop.hive.llap.tez.Converters;
import org.apache.hadoop.hive.llap.tezplugins.LlapTaskCommunicator;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.runtime.api.impl.InputSpec;

public class SourceStateTracker {

  private static final Logger LOG = LoggerFactory.getLogger(SourceStateTracker.class);

  private final TaskCommunicatorContext taskCommunicatorContext;
  private final LlapTaskCommunicator taskCommunicator;

  // Tracks vertices for which notifications have been registered
  private final Set<String> notificationRegisteredVertices = new HashSet<>();

  private final Map<String, SourceInfo> sourceInfoMap = new HashMap<>();
  private final Map<LlapNodeId, NodeInfo> nodeInfoMap = new HashMap<>();

  private volatile QueryIdentifierProto currentQueryIdentifier;

  public SourceStateTracker(TaskCommunicatorContext taskCommunicatorContext,
                            LlapTaskCommunicator taskCommunicator) {
    this.taskCommunicatorContext = taskCommunicatorContext;
    this.taskCommunicator = taskCommunicator;
  }

  /**
   * To be invoked after each DAG completes.
   */
  public synchronized void resetState(QueryIdentifierProto currentQueryIdentifierProto) {
    sourceInfoMap.clear();
    nodeInfoMap.clear();
    notificationRegisteredVertices.clear();
    this.currentQueryIdentifier = currentQueryIdentifierProto;
  }

  /**
   * Used to register a task for state updates. Effectively registers for state updates to go to the specific node.
   * @param host
   * @param port
   * @param inputSpecList
   */
  public synchronized void registerTaskForStateUpdates(String host, int port,
                                                       List<InputSpec> inputSpecList) {

    // Add tracking information. Check if source state already known and send out an update if it is.

    List<String> sourcesOfInterest = getSourceInterestList(inputSpecList);
    if (sourcesOfInterest != null && !sourcesOfInterest.isEmpty()) {
      LlapNodeId nodeId = LlapNodeId.getInstance(host, port);
      NodeInfo nodeInfo = getNodeInfo(nodeId);

      // Set up the data structures, before any notifications come in.
      for (String src : sourcesOfInterest) {
        VertexState oldStateForNode = nodeInfo.getLastKnownStateForSource(src);
        if (oldStateForNode == null) {
          // Not registered for this node.
          // Register and send state if it is successful.
          SourceInfo srcInfo = getSourceInfo(src);
          srcInfo.addNode(nodeId);

          nodeInfo.addSource(src, srcInfo.lastKnownState);
          if (srcInfo.lastKnownState == VertexState.SUCCEEDED) {
            sendStateUpdateToNode(nodeId, src, srcInfo.lastKnownState);
          }

        } else {
          // Already registered to send updates to this node for the specific source.
          // Nothing to do for now, unless tracking tasks at a later point.
        }

        // Setup for actual notifications, if not already done for a previous task.
        maybeRegisterForVertexUpdates(src);
      }
    } else {
      // Don't need to track anything for this task. No new notifications, etc.
    }
  }

  /**
   * Handled notifications on state updates for sources
   * @param sourceName
   * @param sourceState
   */
  public synchronized void sourceStateUpdated(String sourceName, VertexState sourceState) {
    SourceInfo sourceInfo = getSourceInfo(sourceName);
    // Update source info if the state is SUCCEEDED
    if (sourceState == VertexState.SUCCEEDED) {
      sourceInfo.numCompletedTasks = getVertexCompletedTaskCount(sourceName);
      sourceInfo.numTasks = getVertexTotalTaskCount(sourceName);
    }
    sourceInfo.lastKnownState = sourceState;
    // Checking state per node for future failure handling scenarios, where an update
    // to a single node may fail.
    for (LlapNodeId nodeId : sourceInfo.getInterestedNodes()) {
      NodeInfo nodeInfo = nodeInfoMap.get(nodeId);
      VertexState lastStateForNode = nodeInfo.getLastKnownStateForSource(sourceName);
      // Send only if the state has changed.
      if (lastStateForNode != sourceState) {
        nodeInfo.setLastKnownStateForSource(sourceName, sourceState);
        sendStateUpdateToNode(nodeId, sourceName, sourceState);
      }
    }
  }


  // Assumes serialized DAGs within an AM, and a reset of structures after each DAG completes.
  /**
   * Constructs FragmentRuntimeInfo for scheduling within LLAP daemons.
   * Also caches state based on state updates.
   * @param vertexName
   * @param fragmentNumber
   * @param priority
   * @return
   */
  public synchronized FragmentRuntimeInfo getFragmentRuntimeInfo(String vertexName, int fragmentNumber,
                                                                 int priority) {
    FragmentRuntimeInfo.Builder builder = FragmentRuntimeInfo.newBuilder();
    maybeRegisterForVertexUpdates(vertexName);

    MutableInt totalTaskCount = new MutableInt(0);
    MutableInt completedTaskCount = new MutableInt(0);
    computeUpstreamTaskCounts(completedTaskCount, totalTaskCount, vertexName);

    builder.setNumSelfAndUpstreamCompletedTasks(completedTaskCount.intValue());
    builder.setNumSelfAndUpstreamTasks(totalTaskCount.intValue());
    builder.setDagStartTime(taskCommunicatorContext.getDagStartTime());
    builder.setWithinDagPriority(priority);
    builder.setFirstAttemptStartTime(taskCommunicatorContext.getFirstAttemptStartTime(vertexName, fragmentNumber));
    builder.setCurrentAttemptStartTime(System.currentTimeMillis());
    return builder.build();
  }

  private void computeUpstreamTaskCounts(MutableInt completedTaskCount, MutableInt totalTaskCount, String sourceName) {
    SourceInfo sourceInfo = getSourceInfo(sourceName);
    if (sourceInfo.lastKnownState == VertexState.SUCCEEDED) {
      // Some of the information in the source is complete. Don't need to fetch it from the context.
      completedTaskCount.add(sourceInfo.numCompletedTasks);
      totalTaskCount.add(sourceInfo.numTasks);
    } else {
      completedTaskCount.add(getVertexCompletedTaskCount(sourceName));
      int totalCount = getVertexTotalTaskCount(sourceName);

      // Uninitialized vertices will report count as 0.
      totalCount = totalCount == -1 ? 0 : totalCount;
      totalTaskCount.add(totalCount);
    }

    // Walk through all the source vertices
    for (String up : taskCommunicatorContext.getInputVertexNames(sourceName)) {
      computeUpstreamTaskCounts(completedTaskCount, totalTaskCount, up);
    }
  }

  private static class SourceInfo {

    // Always start in the running state. Requests for state updates will be sent out after registration.
    private VertexState lastKnownState = VertexState.RUNNING;

    // Used for sending notifications about a vertex completed. For canFinish
    // Can be converted to a Tez event, if this is sufficient to decide on pre-emption
    private final List<LlapNodeId> interestedNodes = new LinkedList<>();

    // Used for sending information for scheduling priority.
    private int numTasks;
    private int numCompletedTasks;

    void addNode(LlapNodeId nodeId) {
      interestedNodes.add(nodeId);
    }

    List<LlapNodeId> getInterestedNodes() {
      return this.interestedNodes;
    }



  }

  private synchronized SourceInfo getSourceInfo(String srcName) {
    SourceInfo sourceInfo = sourceInfoMap.get(srcName);
    if (sourceInfo == null) {
      sourceInfo = new SourceInfo();
      sourceInfoMap.put(srcName, sourceInfo);
    }
    return sourceInfo;
  }


  private static class NodeInfo {
    private final Map<String, VertexState> sourcesOfInterest = new HashMap<>();

    void addSource(String srcName, VertexState sourceState) {
      sourcesOfInterest.put(srcName, sourceState);
    }

    VertexState getLastKnownStateForSource(String src) {
      return sourcesOfInterest.get(src);
    }

    void setLastKnownStateForSource(String src, VertexState state) {
      sourcesOfInterest.put(src, state);
    }
  }

  private synchronized NodeInfo getNodeInfo(LlapNodeId llapNodeId) {
    NodeInfo nodeInfo = nodeInfoMap.get(llapNodeId);
    if (nodeInfo == null) {
      nodeInfo = new NodeInfo();
      nodeInfoMap.put(llapNodeId, nodeInfo);
    }
    return nodeInfo;
  }


  private List<String> getSourceInterestList(List<InputSpec> inputSpecList) {
    List<String> sourcesOfInterest = Collections.emptyList();
    if (inputSpecList != null) {
      boolean alreadyFound = false;
      for (InputSpec inputSpec : inputSpecList) {
        if (LlapTezUtils.isSourceOfInterest(inputSpec.getInputDescriptor().getClassName())) {
          if (!alreadyFound) {
            alreadyFound = true;
            sourcesOfInterest = new LinkedList<>();
          }
          sourcesOfInterest.add(inputSpec.getSourceVertexName());
        }
      }
    }
    return sourcesOfInterest;
  }


  private void maybeRegisterForVertexUpdates(String sourceName) {
    if (!notificationRegisteredVertices.contains(sourceName)) {
      notificationRegisteredVertices.add(sourceName);
      taskCommunicatorContext.registerForVertexStateUpdates(sourceName, EnumSet.of(
          VertexState.RUNNING, VertexState.SUCCEEDED));
    }
  }

  private int getVertexCompletedTaskCount(String vname) {
    int completedTaskCount;
    try {
      completedTaskCount =
          taskCommunicatorContext.getVertexCompletedTaskCount(vname);
      return completedTaskCount;
    } catch (Exception e) {
      LOG.error("Failed to get vertex completed task count for sourceName={}",
          vname);
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  private int getVertexTotalTaskCount(String vname) {
    int totalCount;
    try {
      totalCount =
          taskCommunicatorContext.getVertexTotalTaskCount(vname);
      return totalCount;
    } catch (Exception e) {
      LOG.error("Failed to get total task count for sourceName={}", vname);
      if (e instanceof RuntimeException) {
        throw (RuntimeException)e;
      } else {
        throw new RuntimeException(e);
      }
    }
  }





  void sendStateUpdateToNode(LlapNodeId nodeId, String sourceName, VertexState state) {
    taskCommunicator.sendStateUpdate(nodeId,
        SourceStateUpdatedRequestProto.newBuilder().setQueryIdentifier(currentQueryIdentifier)
            .setSrcName(sourceName).setState(Converters.fromVertexState(state)).build());
  }


}
