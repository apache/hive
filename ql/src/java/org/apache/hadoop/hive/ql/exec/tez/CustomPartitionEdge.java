/**
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

package org.apache.hadoop.hive.ql.exec.tez;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tez.dag.api.EdgeManagerPlugin;
import org.apache.tez.dag.api.EdgeManagerPluginContext;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;

public class CustomPartitionEdge extends EdgeManagerPlugin {

  private static final Log LOG = LogFactory.getLog(CustomPartitionEdge.class.getName());

  CustomEdgeConfiguration conf = null;
  final EdgeManagerPluginContext context;

  // used by the framework at runtime. initialize is the real initializer at runtime
  public CustomPartitionEdge(EdgeManagerPluginContext context) {
    super(context);
    this.context = context;
  }


  @Override
  public int getNumDestinationTaskPhysicalInputs(int destinationTaskIndex) {
    return context.getSourceVertexNumTasks();
  }

  @Override
  public int getNumSourceTaskPhysicalOutputs(int sourceTaskIndex) {
    return conf.getNumBuckets();
  }

  @Override
  public int getNumDestinationConsumerTasks(int sourceTaskIndex) {
    return context.getDestinationVertexNumTasks();
  }

  // called at runtime to initialize the custom edge.
  @Override
  public void initialize() {
    ByteBuffer payload = context.getUserPayload().getPayload();
    LOG.info("Initializing the edge, payload: " + payload);
    if (payload == null) {
      throw new RuntimeException("Invalid payload");
    }
    // De-serialization code
    DataInputByteBuffer dibb = new DataInputByteBuffer();
    dibb.reset(payload);
    conf = new CustomEdgeConfiguration();
    try {
      conf.readFields(dibb);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    LOG.info("Routing table: " + conf.getRoutingTable() + " num Buckets: " + conf.getNumBuckets());
  }

  @Override
  public void routeDataMovementEventToDestination(DataMovementEvent event,
      int sourceTaskIndex, int sourceOutputIndex, Map<Integer, List<Integer>> mapDestTaskIndices) {
    List<Integer> outputIndices = Collections.singletonList(sourceTaskIndex);
    for (Integer destIndex : conf.getRoutingTable().get(sourceOutputIndex)) {
      mapDestTaskIndices.put(destIndex, outputIndices);
    }
  }

  @Override
  public void routeInputSourceTaskFailedEventToDestination(int sourceTaskIndex,
      Map<Integer, List<Integer>> mapDestTaskIndices) {
    List<Integer> outputIndices = Collections.singletonList(sourceTaskIndex);
    for (int i = 0; i < context.getDestinationVertexNumTasks(); i++) {
      mapDestTaskIndices.put(i, outputIndices);
    }
  }

  @Override
  public int routeInputErrorEventToSource(InputReadErrorEvent event,
      int destinationTaskIndex, int destinationFailedInputIndex) {
    return event.getIndex();
  }
}
