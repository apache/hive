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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.VertexLocationHint.TaskLocationHint;
import org.apache.tez.mapreduce.hadoop.InputSplitInfoMem;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRInputUserPayloadProto;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitProto;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitsProto;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.RootInputSpecUpdate;
import org.apache.tez.runtime.api.events.RootInputConfigureVertexTasksEvent;
import org.apache.tez.runtime.api.events.RootInputDataInformationEvent;
import org.apache.tez.runtime.api.events.RootInputInitializerEvent;
import org.apache.tez.runtime.api.RootInputSpecUpdate;
import org.apache.tez.runtime.api.TezRootInputInitializer;
import org.apache.tez.runtime.api.TezRootInputInitializerContext;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

/**
 * This class is used to generate splits inside the AM on the cluster. It
 * optionally groups together splits based on available head room as well as
 * making sure that splits from different partitions are only grouped if they
 * are of the same schema, format and serde
 */
public class HiveSplitGenerator extends TezRootInputInitializer {

  private static final Log LOG = LogFactory.getLog(HiveSplitGenerator.class);

  private static final SplitGrouper grouper = new SplitGrouper();

  public HiveSplitGenerator(TezRootInputInitializerContext initializerContext) {
    super(initializerContext);
  }

  @Override
  public List<Event> initialize() throws Exception {
    TezRootInputInitializerContext rootInputContext = getContext();

    MRInputUserPayloadProto userPayloadProto =
        MRHelpers.parseMRInputPayload(rootInputContext.getUserPayload());

    Configuration conf =
        MRHelpers.createConfFromByteString(userPayloadProto.getConfigurationBytes());

    boolean sendSerializedEvents =
        conf.getBoolean("mapreduce.tez.input.initializer.serialize.event.payload", true);

    // Read all credentials into the credentials instance stored in JobConf.
    JobConf jobConf = new JobConf(conf);
    ShimLoader.getHadoopShims().getMergedCredentials(jobConf);

    InputSplitInfoMem inputSplitInfo = null;
    String realInputFormatName = conf.get("mapred.input.format.class");
    if (realInputFormatName != null && !realInputFormatName.isEmpty()) {
      // Need to instantiate the realInputFormat
      InputFormat<?, ?> inputFormat =
          (InputFormat<?, ?>) ReflectionUtils.newInstance(Class.forName(realInputFormatName),
              jobConf);

      int totalResource = rootInputContext.getTotalAvailableResource().getMemory();
      int taskResource = rootInputContext.getVertexTaskResource().getMemory();
      int availableSlots = totalResource / taskResource;

      // Create the un-grouped splits
      float waves =
          conf.getFloat(TezConfiguration.TEZ_AM_GROUPING_SPLIT_WAVES,
              TezConfiguration.TEZ_AM_GROUPING_SPLIT_WAVES_DEFAULT);

      InputSplit[] splits = inputFormat.getSplits(jobConf, (int) (availableSlots * waves));
      LOG.info("Number of input splits: " + splits.length + ". " + availableSlots
          + " available slots, " + waves + " waves. Input format is: " + realInputFormatName);

      Multimap<Integer, InputSplit> groupedSplits =
          generateGroupedSplits(jobConf, conf, splits, waves, availableSlots);
      // And finally return them in a flat array
      InputSplit[] flatSplits = groupedSplits.values().toArray(new InputSplit[0]);
      LOG.info("Number of grouped splits: " + flatSplits.length);

      List<TaskLocationHint> locationHints = grouper.createTaskLocationHints(flatSplits);

      Utilities.clearWork(jobConf);

      inputSplitInfo =
          new InputSplitInfoMem(flatSplits, locationHints, flatSplits.length, null, jobConf);
    } else {
      // no need for grouping and the target #of tasks.
      inputSplitInfo = MRHelpers.generateInputSplitsToMem(jobConf, false, 0);
    }

    return createEventList(sendSerializedEvents, inputSplitInfo);
  }

  public static Multimap<Integer, InputSplit> generateGroupedSplits(JobConf jobConf,
      Configuration conf, InputSplit[] splits, float waves, int availableSlots)
      throws Exception {

    MapWork work = Utilities.getMapWork(jobConf);

    Multimap<Integer, InputSplit> bucketSplitMultiMap =
        ArrayListMultimap.<Integer, InputSplit> create();

    Class<?> previousInputFormatClass = null;
    String previousDeserializerClass = null;
    Map<Map<String, PartitionDesc>, Map<String, PartitionDesc>> cache =
        new HashMap<Map<String, PartitionDesc>, Map<String, PartitionDesc>>();

    int i = 0;

    for (InputSplit s : splits) {
      // this is the bit where we make sure we don't group across partition
      // schema boundaries

      Path path = ((FileSplit) s).getPath();

      PartitionDesc pd =
          HiveFileFormatUtils.getPartitionDescFromPathRecursively(work.getPathToPartitionInfo(),
              path, cache);

      String currentDeserializerClass = pd.getDeserializerClassName();
      Class<?> currentInputFormatClass = pd.getInputFileFormatClass();

      if ((currentInputFormatClass != previousInputFormatClass)
          || (!currentDeserializerClass.equals(previousDeserializerClass))) {
        ++i;
      }

      previousInputFormatClass = currentInputFormatClass;
      previousDeserializerClass = currentDeserializerClass;

      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding split " + path + " to src group " + i);
      }
      bucketSplitMultiMap.put(i, s);
    }
    LOG.info("# Src groups for split generation: " + (i + 1));

    // group them into the chunks we want
    Multimap<Integer, InputSplit> groupedSplits =
        grouper.group(jobConf, bucketSplitMultiMap, availableSlots, waves);

    return groupedSplits;
  }

  public void handleInputInitializerEvent(List<RootInputInitializerEvent> events) throws Exception {
  }

  private List<Event> createEventList(boolean sendSerializedEvents, InputSplitInfoMem inputSplitInfo) {

    List<Event> events = Lists.newArrayListWithCapacity(inputSplitInfo.getNumTasks() + 1);

    RootInputConfigureVertexTasksEvent configureVertexEvent = new
        RootInputConfigureVertexTasksEvent(inputSplitInfo.getNumTasks(),
        inputSplitInfo.getTaskLocationHints(),
        RootInputSpecUpdate.getDefaultSinglePhysicalInputSpecUpdate());
    events.add(configureVertexEvent);

    if (sendSerializedEvents) {
      MRSplitsProto splitsProto = inputSplitInfo.getSplitsProto();
      int count = 0;
      for (MRSplitProto mrSplit : splitsProto.getSplitsList()) {
        RootInputDataInformationEvent diEvent =
            new RootInputDataInformationEvent(count++, mrSplit.toByteArray());
        events.add(diEvent);
      }
    } else {
      int count = 0;
      for (org.apache.hadoop.mapred.InputSplit split : inputSplitInfo.getOldFormatSplits()) {
        RootInputDataInformationEvent diEvent = new RootInputDataInformationEvent(count++, split);
        events.add(diEvent);
      }
    }
    return events;
  }
}
