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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.split.TezGroupedSplitsInputFormat;
import org.apache.hadoop.mapred.split.TezMapredSplitsGrouper;
import org.apache.tez.dag.api.EdgeManagerDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.VertexLocationHint.TaskLocationHint;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRInputUserPayloadProto;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitProto;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.RootInputConfigureVertexTasksEvent;
import org.apache.tez.runtime.api.events.RootInputDataInformationEvent;
import org.apache.tez.runtime.api.events.RootInputUpdatePayloadEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;


/*
 * Only works with old mapred API
 * Will only work with a single MRInput for now.
 */
public class CustomPartitionVertex implements VertexManagerPlugin {

  private static final Log LOG = LogFactory.getLog(CustomPartitionVertex.class.getName());


  VertexManagerPluginContext context;

  private Multimap<Integer, Integer> bucketToTaskMap = HashMultimap.<Integer, Integer>create();
  private Multimap<Integer, InputSplit> bucketToInitialSplitMap = 
      ArrayListMultimap.<Integer, InputSplit>create();

  private RootInputConfigureVertexTasksEvent configureVertexTaskEvent;
  private List<RootInputDataInformationEvent> dataInformationEvents;
  private Map<Path, List<FileSplit>> pathFileSplitsMap = new TreeMap<Path, List<FileSplit>>();
  private int numBuckets = -1;
  private Configuration conf = null;
  private boolean rootVertexInitialized = false;
  Multimap<Integer, InputSplit> bucketToGroupedSplitMap;


  private Map<Integer, Integer> bucketToNumTaskMap = new HashMap<Integer, Integer>();

  public CustomPartitionVertex() {
  }

  @Override
  public void initialize(VertexManagerPluginContext context) {
    this.context = context; 
    ByteBuffer byteBuf = ByteBuffer.wrap(context.getUserPayload());
    this.numBuckets = byteBuf.getInt();
  }

  @Override
  public void onVertexStarted(Map<String, List<Integer>> completions) {
    int numTasks = context.getVertexNumTasks(context.getVertexName());
    List<Integer> scheduledTasks = new ArrayList<Integer>(numTasks);
    for (int i=0; i<numTasks; ++i) {
      scheduledTasks.add(new Integer(i));
    }
    context.scheduleVertexTasks(scheduledTasks);
  }

  @Override
  public void onSourceTaskCompleted(String srcVertexName, Integer attemptId) {
  }

  @Override
  public void onVertexManagerEventReceived(VertexManagerEvent vmEvent) {
  }

  // One call per root Input - and for now only one is handled.
  @Override
  public void onRootVertexInitialized(String inputName, InputDescriptor inputDescriptor,
      List<Event> events) {

    // Ideally, since there's only 1 Input expected at the moment -
    // ensure this method is called only once. Tez will call it once per Root Input.
    Preconditions.checkState(rootVertexInitialized == false);
    rootVertexInitialized = true;
    try {
      // This is using the payload from the RootVertexInitializer corresponding
      // to InputName. Ideally it should be using it's own configuration class - but that
      // means serializing another instance.
      MRInputUserPayloadProto protoPayload = 
          MRHelpers.parseMRInputPayload(inputDescriptor.getUserPayload());
      this.conf = MRHelpers.createConfFromByteString(protoPayload.getConfigurationBytes());

      /*
       * Currently in tez, the flow of events is thus: "Generate Splits -> Initialize Vertex"
       * (with parallelism info obtained from the generate splits phase). The generate splits
       * phase groups splits using the TezGroupedSplitsInputFormat. However, for bucket map joins
       * the grouping done by this input format results in incorrect results as the grouper has no
       * knowledge of buckets. So, we initially set the input format to be HiveInputFormat
       * (in DagUtils) for the case of bucket map joins so as to obtain un-grouped splits.
       * We then group the splits corresponding to buckets using the tez grouper which returns
       * TezGroupedSplits.
       */

      // This assumes that Grouping will always be used. 
      // Changing the InputFormat - so that the correct one is initialized in MRInput.
      this.conf.set("mapred.input.format.class", TezGroupedSplitsInputFormat.class.getName());
      MRInputUserPayloadProto updatedPayload = MRInputUserPayloadProto
          .newBuilder(protoPayload)
          .setConfigurationBytes(MRHelpers.createByteStringFromConf(conf))
          .build();
      inputDescriptor.setUserPayload(updatedPayload.toByteArray());
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    boolean dataInformationEventSeen = false;
    for (Event event : events) {
      if (event instanceof RootInputConfigureVertexTasksEvent) {
        // No tasks should have been started yet. Checked by initial state check.
        Preconditions.checkState(dataInformationEventSeen == false);
        Preconditions
        .checkState(
            context.getVertexNumTasks(context.getVertexName()) == -1,
            "Parallelism for the vertex should be set to -1 if the InputInitializer is setting parallelism");
        RootInputConfigureVertexTasksEvent cEvent = (RootInputConfigureVertexTasksEvent) event;

        // The vertex cannot be configured until all DataEvents are seen - to build the routing table.
        configureVertexTaskEvent = cEvent;
        dataInformationEvents = Lists.newArrayListWithCapacity(configureVertexTaskEvent.getNumTasks());
      }
      if (event instanceof RootInputUpdatePayloadEvent) {
        // this event can never occur. If it does, fail.
        Preconditions.checkState(false);
      } else if (event instanceof RootInputDataInformationEvent) {
        dataInformationEventSeen = true;
        RootInputDataInformationEvent diEvent = (RootInputDataInformationEvent) event;
        dataInformationEvents.add(diEvent);
        FileSplit fileSplit;
        try {
          fileSplit = getFileSplitFromEvent(diEvent);
        } catch (IOException e) {
          throw new RuntimeException("Failed to get file split for event: " + diEvent);
        }
        List<FileSplit> fsList = pathFileSplitsMap.get(fileSplit.getPath()); 
        if (fsList == null) {
          fsList = new ArrayList<FileSplit>();
          pathFileSplitsMap.put(fileSplit.getPath(), fsList);
        }
        fsList.add(fileSplit);
      }
    }

    setBucketNumForPath(pathFileSplitsMap);
    try {
      groupSplits();
      processAllEvents(inputName);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void processAllEvents(String inputName) throws IOException {

    List<InputSplit> finalSplits = Lists.newLinkedList();
    int taskCount = 0;
    for (Entry<Integer, Collection<InputSplit>> entry : bucketToGroupedSplitMap.asMap().entrySet()) {
      int bucketNum = entry.getKey();
      Collection<InputSplit> initialSplits = entry.getValue();
      finalSplits.addAll(initialSplits);
      for (int i = 0; i < initialSplits.size(); i++) {
        bucketToTaskMap.put(bucketNum, taskCount);
        taskCount++;
      }
    }

    // Construct the EdgeManager descriptor to be used by all edges which need the routing table.
    EdgeManagerDescriptor hiveEdgeManagerDesc = new EdgeManagerDescriptor(
        CustomPartitionEdge.class.getName());    
    byte[] payload = getBytePayload(bucketToTaskMap);
    hiveEdgeManagerDesc.setUserPayload(payload);

    Map<String, EdgeManagerDescriptor> emMap = Maps.newHashMap();

    // Replace the edge manager for all vertices which have routing type custom.
    for (Entry<String, EdgeProperty> edgeEntry : context.getInputVertexEdgeProperties().entrySet()) {
      if (edgeEntry.getValue().getDataMovementType() == DataMovementType.CUSTOM
          && edgeEntry.getValue().getEdgeManagerDescriptor().getClassName()
              .equals(CustomPartitionEdge.class.getName())) {
        emMap.put(edgeEntry.getKey(), hiveEdgeManagerDesc);
      }
    }

    LOG.info("Task count is " + taskCount);

    List<RootInputDataInformationEvent> taskEvents = Lists.newArrayListWithCapacity(finalSplits.size());
    // Re-serialize the splits after grouping.
    int count = 0;
    for (InputSplit inputSplit : finalSplits) {
      MRSplitProto serializedSplit = MRHelpers.createSplitProto(inputSplit);
      RootInputDataInformationEvent diEvent = new RootInputDataInformationEvent(
          count, serializedSplit.toByteArray());
      diEvent.setTargetIndex(count);
      count++;
      taskEvents.add(diEvent);
    }

    // Replace the Edge Managers
    context.setVertexParallelism(
        taskCount,
        new VertexLocationHint(createTaskLocationHintsFromSplits(finalSplits
            .toArray(new InputSplit[finalSplits.size()]))), emMap);

    // Set the actual events for the tasks.
    context.addRootInputEvents(inputName, taskEvents);
  }

  private byte[] getBytePayload(Multimap<Integer, Integer> routingTable) throws IOException {
    CustomEdgeConfiguration edgeConf = 
        new CustomEdgeConfiguration(routingTable.keySet().size(), routingTable);
    DataOutputBuffer dob = new DataOutputBuffer();
    edgeConf.write(dob);
    byte[] serialized = dob.getData();

    return serialized;
  }

  private FileSplit getFileSplitFromEvent(RootInputDataInformationEvent event)
      throws IOException {
    InputSplit inputSplit = null;
    if (event.getDeserializedUserPayload() != null) {
      inputSplit = (InputSplit) event.getDeserializedUserPayload();
    } else {
      MRSplitProto splitProto = MRSplitProto.parseFrom(event.getUserPayload());
      SerializationFactory serializationFactory = new SerializationFactory(
          new Configuration());
      inputSplit = MRHelpers.createOldFormatSplitFromUserPayload(splitProto,
          serializationFactory);
    }

    if (!(inputSplit instanceof FileSplit)) {
      throw new UnsupportedOperationException(
          "Cannot handle splits other than FileSplit for the moment");
    }
    return (FileSplit) inputSplit;
  }

  /*
   * This method generates the map of bucket to file splits.
   */
  private void setBucketNumForPath(Map<Path, List<FileSplit>> pathFileSplitsMap) {
    int bucketNum = 0;
    int fsCount = 0;
    for (Map.Entry<Path, List<FileSplit>> entry : pathFileSplitsMap.entrySet()) {
      int bucketId = bucketNum % numBuckets;
      for (FileSplit fsplit : entry.getValue()) {
        fsCount++;
        bucketToInitialSplitMap.put(bucketId, fsplit);
      }
      bucketNum++;
    }

    LOG.info("Total number of splits counted: " + fsCount + " and total files encountered: " 
        + pathFileSplitsMap.size());
  }

  private void groupSplits () throws IOException {
    estimateBucketSizes();
    bucketToGroupedSplitMap = 
        ArrayListMultimap.<Integer, InputSplit>create(bucketToInitialSplitMap);
    
    Map<Integer, Collection<InputSplit>> bucketSplitMap = bucketToInitialSplitMap.asMap();
    for (int bucketId : bucketSplitMap.keySet()) {
      Collection<InputSplit>inputSplitCollection = bucketSplitMap.get(bucketId);
      TezMapredSplitsGrouper grouper = new TezMapredSplitsGrouper();

      InputSplit[] groupedSplits = grouper.getGroupedSplits(conf, 
          inputSplitCollection.toArray(new InputSplit[0]), bucketToNumTaskMap.get(bucketId),
          HiveInputFormat.class.getName());
      LOG.info("Original split size is " + 
          inputSplitCollection.toArray(new InputSplit[0]).length + 
          " grouped split size is " + groupedSplits.length);
      bucketToGroupedSplitMap.removeAll(bucketId);
      for (InputSplit inSplit : groupedSplits) {
        bucketToGroupedSplitMap.put(bucketId, inSplit);
      }
    }
  }

  private void estimateBucketSizes() {
    Map<Integer, Long>bucketSizeMap = new HashMap<Integer, Long>();
    Map<Integer, Collection<InputSplit>> bucketSplitMap = bucketToInitialSplitMap.asMap();
    long totalSize = 0;
    for (int bucketId : bucketSplitMap.keySet()) {
      Long size = 0L;
      Collection<InputSplit>inputSplitCollection = bucketSplitMap.get(bucketId);
      Iterator<InputSplit> iter = inputSplitCollection.iterator();
      while (iter.hasNext()) {
        FileSplit fsplit = (FileSplit)iter.next();
        size += fsplit.getLength();
        totalSize += fsplit.getLength();
      }
      bucketSizeMap.put(bucketId, size);
    }

    int totalResource = context.getTotalAVailableResource().getMemory();
    int taskResource = context.getVertexTaskResource().getMemory();
    float waves = conf.getFloat(
        TezConfiguration.TEZ_AM_GROUPING_SPLIT_WAVES,
        TezConfiguration.TEZ_AM_GROUPING_SPLIT_WAVES_DEFAULT);

    int numTasks = (int)((totalResource*waves)/taskResource);
    LOG.info("Total resource: " + totalResource + " Task Resource: " + taskResource
        + " waves: " + waves + " total size of splits: " + totalSize + 
        " total number of tasks: " + numTasks);

    for (int bucketId : bucketSizeMap.keySet()) {
      int numEstimatedTasks = 0;
      if (totalSize != 0) {
        numEstimatedTasks = (int)(numTasks * bucketSizeMap.get(bucketId) / totalSize);
      }
      LOG.info("Estimated number of tasks: " + numEstimatedTasks + " for bucket " + bucketId);
      if (numEstimatedTasks == 0) {
        numEstimatedTasks = 1;
      }
      bucketToNumTaskMap.put(bucketId, numEstimatedTasks);
    }
  }

  private static List<TaskLocationHint> createTaskLocationHintsFromSplits(
      org.apache.hadoop.mapred.InputSplit[] oldFormatSplits) {
    Iterable<TaskLocationHint> iterable = Iterables.transform(Arrays.asList(oldFormatSplits),
        new Function<org.apache.hadoop.mapred.InputSplit, TaskLocationHint>() {
      @Override
      public TaskLocationHint apply(org.apache.hadoop.mapred.InputSplit input) {
        try {
          if (input.getLocations() != null) {
            return new TaskLocationHint(new HashSet<String>(Arrays.asList(input.getLocations())),
                null);
          } else {
            LOG.info("NULL Location: returning an empty location hint");
            return new TaskLocationHint(null,null);
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });

    return Lists.newArrayList(iterable);
  }
}
