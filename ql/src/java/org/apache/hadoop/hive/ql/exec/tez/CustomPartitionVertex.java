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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.plan.TezWork.VertexType;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.split.TezMapReduceSplitsGrouper;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.mapreduce.hadoop.MRInputHelpers;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRInputUserPayloadProto;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitProto;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputSpecUpdate;
import org.apache.tez.runtime.api.events.InputConfigureVertexTasksEvent;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.events.InputUpdatePayloadEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.protobuf.ByteString;

/*
 * Only works with old mapred API
 * Will only work with a single MRInput for now.
 */
public class CustomPartitionVertex extends VertexManagerPlugin {

  private static final Log LOG = LogFactory.getLog(CustomPartitionVertex.class.getName());

  VertexManagerPluginContext context;

  private InputConfigureVertexTasksEvent configureVertexTaskEvent;
  private List<InputDataInformationEvent> dataInformationEvents;
  private int numBuckets = -1;
  private Configuration conf = null;
  private final SplitGrouper grouper = new SplitGrouper();
  private int taskCount = 0;
  private VertexType vertexType;
  private String mainWorkName;
  private final Multimap<Integer, Integer> bucketToTaskMap = HashMultimap.<Integer, Integer> create();

  private final Map<String, Multimap<Integer, InputSplit>> inputToGroupedSplitMap =
      new HashMap<String, Multimap<Integer, InputSplit>>();

  public CustomPartitionVertex(VertexManagerPluginContext context) {
    super(context);
  }

  @Override
  public void initialize() {
    this.context = getContext();
    ByteBuffer payload = context.getUserPayload().getPayload();
    CustomVertexConfiguration vertexConf = new CustomVertexConfiguration();
    DataInputByteBuffer dibb = new DataInputByteBuffer();
    dibb.reset(payload);
    try {
      vertexConf.readFields(dibb);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.numBuckets = vertexConf.getNumBuckets();
    this.mainWorkName = vertexConf.getInputName();
    this.vertexType = vertexConf.getVertexType();
  }

  @Override
  public void onVertexStarted(Map<String, List<Integer>> completions) {
    int numTasks = context.getVertexNumTasks(context.getVertexName());
    List<VertexManagerPluginContext.TaskWithLocationHint> scheduledTasks = 
      new ArrayList<VertexManagerPluginContext.TaskWithLocationHint>(numTasks);
    for (int i = 0; i < numTasks; ++i) {
      scheduledTasks.add(new VertexManagerPluginContext.TaskWithLocationHint(new Integer(i), null));
    }
    context.scheduleVertexTasks(scheduledTasks);
  }

  @Override
  public void onSourceTaskCompleted(String srcVertexName, Integer attemptId) {
  }

  @Override
  public void onVertexManagerEventReceived(VertexManagerEvent vmEvent) {
  }

  // One call per root Input
  @Override
  public void onRootVertexInitialized(String inputName, InputDescriptor inputDescriptor,
      List<Event> events) {
    LOG.info("On root vertex initialized " + inputName);

    try {
      // This is using the payload from the RootVertexInitializer corresponding
      // to InputName. Ideally it should be using it's own configuration class -
      // but that
      // means serializing another instance.
      MRInputUserPayloadProto protoPayload =
          MRInputHelpers.parseMRInputPayload(inputDescriptor.getUserPayload());
      this.conf = TezUtils.createConfFromByteString(protoPayload.getConfigurationBytes());

      /*
       * Currently in tez, the flow of events is thus:
       * "Generate Splits -> Initialize Vertex" (with parallelism info obtained
       * from the generate splits phase). The generate splits phase groups
       * splits using the TezGroupedSplitsInputFormat. However, for bucket map
       * joins the grouping done by this input format results in incorrect
       * results as the grouper has no knowledge of buckets. So, we initially
       * set the input format to be HiveInputFormat (in DagUtils) for the case
       * of bucket map joins so as to obtain un-grouped splits. We then group
       * the splits corresponding to buckets using the tez grouper which returns
       * TezGroupedSplits.
       */

      // This assumes that Grouping will always be used.
      // Enabling grouping on the payload.
      MRInputUserPayloadProto updatedPayload =
          MRInputUserPayloadProto.newBuilder(protoPayload).setGroupingEnabled(true).build();
      inputDescriptor.setUserPayload(UserPayload.create(updatedPayload.toByteString().asReadOnlyByteBuffer()));
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    boolean dataInformationEventSeen = false;
    Map<String, List<FileSplit>> pathFileSplitsMap = new TreeMap<String, List<FileSplit>>();

    for (Event event : events) {
      if (event instanceof InputConfigureVertexTasksEvent) {
        // No tasks should have been started yet. Checked by initial state
        // check.
        Preconditions.checkState(dataInformationEventSeen == false);
        InputConfigureVertexTasksEvent cEvent = (InputConfigureVertexTasksEvent) event;

        // The vertex cannot be configured until all DataEvents are seen - to
        // build the routing table.
        configureVertexTaskEvent = cEvent;
        dataInformationEvents =
            Lists.newArrayListWithCapacity(configureVertexTaskEvent.getNumTasks());
      }
      if (event instanceof InputUpdatePayloadEvent) {
        // this event can never occur. If it does, fail.
        Preconditions.checkState(false);
      } else if (event instanceof InputDataInformationEvent) {
        dataInformationEventSeen = true;
        InputDataInformationEvent diEvent = (InputDataInformationEvent) event;
        dataInformationEvents.add(diEvent);
        FileSplit fileSplit;
        try {
          fileSplit = getFileSplitFromEvent(diEvent);
        } catch (IOException e) {
          throw new RuntimeException("Failed to get file split for event: " + diEvent);
        }
        List<FileSplit> fsList = pathFileSplitsMap.get(fileSplit.getPath().getName());
        if (fsList == null) {
          fsList = new ArrayList<FileSplit>();
          pathFileSplitsMap.put(fileSplit.getPath().getName(), fsList);
        }
        fsList.add(fileSplit);
      }
    }

    Multimap<Integer, InputSplit> bucketToInitialSplitMap =
        getBucketSplitMapForPath(pathFileSplitsMap);

    try {
      int totalResource = context.getTotalAvailableResource().getMemory();
      int taskResource = context.getVertexTaskResource().getMemory();
      float waves =
          conf.getFloat(TezMapReduceSplitsGrouper.TEZ_GROUPING_SPLIT_WAVES,
              TezMapReduceSplitsGrouper.TEZ_GROUPING_SPLIT_WAVES_DEFAULT);

      int availableSlots = totalResource / taskResource;

      LOG.info("Grouping splits. " + availableSlots + " available slots, " + waves + " waves.");
      JobConf jobConf = new JobConf(conf);
      ShimLoader.getHadoopShims().getMergedCredentials(jobConf);

      Multimap<Integer, InputSplit> bucketToGroupedSplitMap =
          HashMultimap.<Integer, InputSplit> create();
      for (Integer key : bucketToInitialSplitMap.keySet()) {
        InputSplit[] inputSplitArray =
            (bucketToInitialSplitMap.get(key).toArray(new InputSplit[0]));
        Multimap<Integer, InputSplit> groupedSplit =
            HiveSplitGenerator.generateGroupedSplits(jobConf, conf, inputSplitArray, waves,
                availableSlots, inputName);
        bucketToGroupedSplitMap.putAll(key, groupedSplit.values());
      }

      LOG.info("We have grouped the splits into " + bucketToGroupedSplitMap);
      if ((mainWorkName.isEmpty() == false) && (mainWorkName.compareTo(inputName) != 0)) {
        /*
         * this is the small table side. In case of SMB join, we may need to send each split to the
         * corresponding bucket-based task on the other side. In case a split needs to go to
         * multiple downstream tasks, we need to clone the event and send it to the right
         * destination.
         */
        processAllSideEvents(inputName, bucketToGroupedSplitMap);
      } else {
        processAllEvents(inputName, bucketToGroupedSplitMap);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void processAllSideEvents(String inputName,
      Multimap<Integer, InputSplit> bucketToGroupedSplitMap) throws IOException {
    // the bucket to task map should have been setup by the big table.
    if (bucketToTaskMap.isEmpty()) {
      inputToGroupedSplitMap.put(inputName, bucketToGroupedSplitMap);
      return;
    }
    List<InputDataInformationEvent> taskEvents = new ArrayList<InputDataInformationEvent>();
    for (Entry<Integer, Collection<InputSplit>> entry : bucketToGroupedSplitMap.asMap().entrySet()) {
      Collection<Integer> destTasks = bucketToTaskMap.get(entry.getKey());
      for (Integer task : destTasks) {
        for (InputSplit split : entry.getValue()) {
          MRSplitProto serializedSplit = MRInputHelpers.createSplitProto(split);
          InputDataInformationEvent diEvent =
              InputDataInformationEvent.createWithSerializedPayload(task, serializedSplit
                  .toByteString().asReadOnlyByteBuffer());
          diEvent.setTargetIndex(task);
          taskEvents.add(diEvent);
        }
      }
    }

    context.addRootInputEvents(inputName, taskEvents);
  }

  private void processAllEvents(String inputName,
      Multimap<Integer, InputSplit> bucketToGroupedSplitMap) throws IOException {

    List<InputSplit> finalSplits = Lists.newLinkedList();
    for (Entry<Integer, Collection<InputSplit>> entry : bucketToGroupedSplitMap.asMap().entrySet()) {
      int bucketNum = entry.getKey();
      Collection<InputSplit> initialSplits = entry.getValue();
      finalSplits.addAll(initialSplits);
      for (int i = 0; i < initialSplits.size(); i++) {
        bucketToTaskMap.put(bucketNum, taskCount);
        taskCount++;
      }
    }

    // Construct the EdgeManager descriptor to be used by all edges which need
    // the routing table.
    EdgeManagerPluginDescriptor hiveEdgeManagerDesc = null;
    if ((vertexType == VertexType.MULTI_INPUT_INITIALIZED_EDGES)
        || (vertexType == VertexType.INITIALIZED_EDGES)) {
      hiveEdgeManagerDesc = EdgeManagerPluginDescriptor.create(CustomPartitionEdge.class.getName());
      UserPayload payload = getBytePayload(bucketToTaskMap);
      hiveEdgeManagerDesc.setUserPayload(payload);
    }
    Map<String, EdgeManagerPluginDescriptor> emMap = Maps.newHashMap();

    // Replace the edge manager for all vertices which have routing type custom.
    for (Entry<String, EdgeProperty> edgeEntry : context.getInputVertexEdgeProperties().entrySet()) {
      if (edgeEntry.getValue().getDataMovementType() == DataMovementType.CUSTOM
          && edgeEntry.getValue().getEdgeManagerDescriptor().getClassName()
              .equals(CustomPartitionEdge.class.getName())) {
        emMap.put(edgeEntry.getKey(), hiveEdgeManagerDesc);
      }
    }

    LOG.info("Task count is " + taskCount);

    List<InputDataInformationEvent> taskEvents =
        Lists.newArrayListWithCapacity(finalSplits.size());
    // Re-serialize the splits after grouping.
    int count = 0;
    for (InputSplit inputSplit : finalSplits) {
      MRSplitProto serializedSplit = MRInputHelpers.createSplitProto(inputSplit);
      InputDataInformationEvent diEvent = InputDataInformationEvent.createWithSerializedPayload(
          count, serializedSplit.toByteString().asReadOnlyByteBuffer());
      diEvent.setTargetIndex(count);
      count++;
      taskEvents.add(diEvent);
    }

    // Replace the Edge Managers
    Map<String, InputSpecUpdate> rootInputSpecUpdate =
      new HashMap<String, InputSpecUpdate>();
    rootInputSpecUpdate.put(
        inputName,
        InputSpecUpdate.getDefaultSinglePhysicalInputSpecUpdate());
    if ((mainWorkName.compareTo(inputName) == 0) || (mainWorkName.isEmpty())) {
      context.setVertexParallelism(
          taskCount,
          VertexLocationHint.create(grouper.createTaskLocationHints(finalSplits
              .toArray(new InputSplit[finalSplits.size()]))), emMap, rootInputSpecUpdate);
    }

    // Set the actual events for the tasks.
    context.addRootInputEvents(inputName, taskEvents);
    if (inputToGroupedSplitMap.isEmpty() == false) {
      for (Entry<String, Multimap<Integer, InputSplit>> entry : inputToGroupedSplitMap.entrySet()) {
        processAllSideEvents(entry.getKey(), entry.getValue());
      }
      inputToGroupedSplitMap.clear();
    }
  }

  UserPayload getBytePayload(Multimap<Integer, Integer> routingTable) throws IOException {
    CustomEdgeConfiguration edgeConf =
        new CustomEdgeConfiguration(routingTable.keySet().size(), routingTable);
    DataOutputBuffer dob = new DataOutputBuffer();
    edgeConf.write(dob);
    byte[] serialized = dob.getData();
    return UserPayload.create(ByteBuffer.wrap(serialized));
  }

  private FileSplit getFileSplitFromEvent(InputDataInformationEvent event) throws IOException {
    InputSplit inputSplit = null;
    if (event.getDeserializedUserPayload() != null) {
      inputSplit = (InputSplit) event.getDeserializedUserPayload();
    } else {
      MRSplitProto splitProto = MRSplitProto.parseFrom(ByteString.copyFrom(event.getUserPayload()));
      SerializationFactory serializationFactory = new SerializationFactory(new Configuration());
      inputSplit = MRInputHelpers.createOldFormatSplitFromUserPayload(splitProto, serializationFactory);
    }

    if (!(inputSplit instanceof FileSplit)) {
      throw new UnsupportedOperationException(
          "Cannot handle splits other than FileSplit for the moment. Current input split type: "
              + inputSplit.getClass().getSimpleName());
    }
    return (FileSplit) inputSplit;
  }

  /*
   * This method generates the map of bucket to file splits.
   */
  private Multimap<Integer, InputSplit> getBucketSplitMapForPath(
      Map<String, List<FileSplit>> pathFileSplitsMap) {

    int bucketNum = 0;

    Multimap<Integer, InputSplit> bucketToInitialSplitMap =
        ArrayListMultimap.<Integer, InputSplit> create();

    for (Map.Entry<String, List<FileSplit>> entry : pathFileSplitsMap.entrySet()) {
      int bucketId = bucketNum % numBuckets;
      for (FileSplit fsplit : entry.getValue()) {
        bucketToInitialSplitMap.put(bucketId, fsplit);
      }
      bucketNum++;
    }

    if (bucketNum < numBuckets) {
      int loopedBucketId = 0;
      for (; bucketNum < numBuckets; bucketNum++) {
        for (InputSplit fsplit : bucketToInitialSplitMap.get(loopedBucketId)) {
          bucketToInitialSplitMap.put(bucketNum, fsplit);
        }
        loopedBucketId++;
      }
    }

    return bucketToInitialSplitMap;
  }
}
