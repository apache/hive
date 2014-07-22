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
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.tez.dag.api.EdgeManager;
import org.apache.tez.dag.api.EdgeManagerContext;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;

import com.google.common.collect.Multimap;

public class CustomPartitionEdge implements EdgeManager {

  private static final Log LOG = LogFactory.getLog(CustomPartitionEdge.class.getName());

  CustomEdgeConfiguration conf = null;

  // used by the framework at runtime. initialize is the real initializer at runtime
  public CustomPartitionEdge() {  
  }

  @Override
  public int getNumDestinationTaskPhysicalInputs(int numSourceTasks, 
      int destinationTaskIndex) {
    return numSourceTasks;
  }

  @Override
  public int getNumSourceTaskPhysicalOutputs(int numDestinationTasks, 
      int sourceTaskIndex) {
    return conf.getNumBuckets();
  }

  @Override
  public int getNumDestinationConsumerTasks(int sourceTaskIndex, int numDestinationTasks) {
    return numDestinationTasks;
  }

  // called at runtime to initialize the custom edge.
  @Override
  public void initialize(EdgeManagerContext context) {
    byte[] payload = context.getUserPayload();
    LOG.info("Initializing the edge, payload: " + payload);
    if (payload == null) {
      throw new RuntimeException("Invalid payload");
    }
    // De-serialization code
    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(payload, payload.length);
    conf = new CustomEdgeConfiguration();
    try {
      conf.readFields(dib);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    LOG.info("Routing table: " + conf.getRoutingTable() + " num Buckets: " + conf.getNumBuckets());
  }

  @Override
  public void routeDataMovementEventToDestination(DataMovementEvent event,
      int sourceTaskIndex, int numDestinationTasks, Map<Integer, List<Integer>> mapDestTaskIndices) {
    int srcIndex = event.getSourceIndex();
    List<Integer> destTaskIndices = new ArrayList<Integer>();
    destTaskIndices.addAll(conf.getRoutingTable().get(srcIndex));
    mapDestTaskIndices.put(new Integer(sourceTaskIndex), destTaskIndices);
  }

  @Override
  public void routeInputSourceTaskFailedEventToDestination(int sourceTaskIndex, 
      int numDestinationTasks, Map<Integer, List<Integer>> mapDestTaskIndices) {
    List<Integer> destTaskIndices = new ArrayList<Integer>();
    addAllDestinationTaskIndices(numDestinationTasks, destTaskIndices);
    mapDestTaskIndices.put(new Integer(sourceTaskIndex), destTaskIndices);
  }

  @Override
  public int routeInputErrorEventToSource(InputReadErrorEvent event, 
      int destinationTaskIndex) {
    return event.getIndex();
  }

  void addAllDestinationTaskIndices(int numDestinationTasks, List<Integer> taskIndices) {
    for(int i=0; i<numDestinationTasks; ++i) {
      taskIndices.add(new Integer(i));
    }
  }
}
