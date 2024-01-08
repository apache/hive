/*
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.google.common.base.Preconditions;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hive.common.util.ReflectionUtil;
import org.apache.tez.common.counters.TezCounters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.split.SplitLocationProvider;
import org.apache.hadoop.mapreduce.split.TezMapReduceSplitsGrouper;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.mapreduce.hadoop.InputSplitInfoMem;
import org.apache.tez.mapreduce.hadoop.MRInputHelpers;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRInputUserPayloadProto;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitProto;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitsProto;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputInitializer;
import org.apache.tez.runtime.api.InputInitializerContext;
import org.apache.tez.runtime.api.InputSpecUpdate;
import org.apache.tez.runtime.api.events.InputConfigureVertexTasksEvent;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.events.InputInitializerEvent;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

/**
 * This class is used to generate splits inside the AM on the cluster. It
 * optionally groups together splits based on available head room as well as
 * making sure that splits from different partitions are only grouped if they
 * are of the same schema, format and serde
 */
public class HiveSplitGenerator extends InputInitializer {

  private static final Logger LOG = LoggerFactory.getLogger(HiveSplitGenerator.class);

  private final DynamicPartitionPruner pruner;
  private Configuration conf;
  private JobConf jobConf;
  private MRInputUserPayloadProto userPayloadProto;
  private MapWork work;
  private final SplitGrouper splitGrouper = new SplitGrouper();
  private SplitLocationProvider splitLocationProvider;
  private Optional<Integer> numSplits;

  private boolean generateSingleSplit;

  public HiveSplitGenerator(Configuration conf, MapWork work, final boolean generateSingleSplit) throws IOException {
    this(conf, work, generateSingleSplit, null);
  }

  public HiveSplitGenerator(Configuration conf, MapWork work, final boolean generateSingleSplit, Integer numSplits)
      throws IOException {
    super(null);

    this.conf = conf;
    this.work = work;
    this.jobConf = new JobConf(conf);
    this.generateSingleSplit = generateSingleSplit;

    // Assuming grouping enabled always.
    userPayloadProto = MRInputUserPayloadProto.newBuilder().setGroupingEnabled(true).build();

    this.splitLocationProvider =
        Utils.getSplitLocationProvider(conf, work.getCacheAffinity(), LOG);
    LOG.info("SplitLocationProvider: " + splitLocationProvider);

    // Read all credentials into the credentials instance stored in JobConf.
    ShimLoader.getHadoopShims().getMergedCredentials(jobConf);

    // Events can start coming in the moment the InputInitializer is created. The pruner
    // must be setup and initialized here so that it sets up it's structures to start accepting events.
    // Setting it up in initialize leads to a window where events may come in before the pruner is
    // initialized, which may cause it to drop events.
    // No dynamic partition pruning
    pruner = null;
    this.numSplits = Optional.ofNullable(numSplits);
  }

  public HiveSplitGenerator(InputInitializerContext initializerContext) {
    super(initializerContext);
    Preconditions.checkNotNull(initializerContext);
    pruner = new DynamicPartitionPruner();
    this.numSplits = Optional.empty();
  }

  private void prepare(InputInitializerContext initializerContext) throws IOException, SerDeException {
    userPayloadProto =
        MRInputHelpers.parseMRInputPayload(initializerContext.getInputUserPayload());

    this.conf = TezUtils.createConfFromByteString(userPayloadProto.getConfigurationBytes());
    
    this.jobConf = new JobConf(conf);

    // Read all credentials into the credentials instance stored in JobConf.
    ShimLoader.getHadoopShims().getMergedCredentials(jobConf);

    this.work = Utilities.getMapWork(jobConf);

    this.splitLocationProvider =
        Utils.getSplitLocationProvider(conf, work.getCacheAffinity(), LOG);
    LOG.info("SplitLocationProvider: " + splitLocationProvider);
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<Event> initialize() throws Exception {
    if (getContext() != null) {
      // called from Tez AM.
      prepare(getContext());
    }

    // Setup the map work for this thread. Pruning modified the work instance to potentially remove
    // partitions. The same work instance must be used when generating splits.
    Utilities.setMapWork(jobConf, work);
    try {
      boolean sendSerializedEvents =
          conf.getBoolean("mapreduce.tez.input.initializer.serialize.event.payload", true);

      // perform dynamic partition pruning
      if (pruner != null) {
        pruner.initialize(getContext(), work, jobConf);
        pruner.prune();
      }

      InputSplitInfoMem inputSplitInfo = null;
      boolean generateConsistentSplits = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_TEZ_GENERATE_CONSISTENT_SPLITS);
      LOG.info("GenerateConsistentSplitsInHive=" + generateConsistentSplits);
      String realInputFormatName = conf.get("mapred.input.format.class");
      boolean groupingEnabled = userPayloadProto.getGroupingEnabled();
      if (groupingEnabled) {
        // Need to instantiate the realInputFormat
        InputFormat<?, ?> inputFormat =
          (InputFormat<?, ?>) ReflectionUtils.newInstance(JavaUtils.loadClass(realInputFormatName),
            jobConf);

        int availableSlots = getAvailableSlotsCalculator().getAvailableSlots();

        if (HiveConf.getLongVar(conf, HiveConf.ConfVars.MAPRED_MIN_SPLIT_SIZE, 1) <= 1) {
          // broken configuration from mapred-default.xml
          final long blockSize = conf.getLongBytes(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
            DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
          final long minGrouping = conf.getLong(
            TezMapReduceSplitsGrouper.TEZ_GROUPING_SPLIT_MIN_SIZE,
            TezMapReduceSplitsGrouper.TEZ_GROUPING_SPLIT_MIN_SIZE_DEFAULT);
          final long preferredSplitSize = Math.min(blockSize / 2, minGrouping);
          HiveConf.setLongVar(jobConf, HiveConf.ConfVars.MAPRED_MIN_SPLIT_SIZE, preferredSplitSize);
          LOG.info("The preferred split size is " + preferredSplitSize);
        }

        float waves;
        // Create the un-grouped splits
        if (numSplits.isPresent()) {
          waves = numSplits.get().floatValue() / availableSlots;
        } else {
          waves =
              conf.getFloat(TezMapReduceSplitsGrouper.TEZ_GROUPING_SPLIT_WAVES,
                  TezMapReduceSplitsGrouper.TEZ_GROUPING_SPLIT_WAVES_DEFAULT);

        }

        InputSplit[] splits;
        if (generateSingleSplit &&
          conf.get(HiveConf.ConfVars.HIVE_TEZ_INPUT_FORMAT.varname).equals(HiveInputFormat.class.getName())) {
          MapWork mapWork = Utilities.getMapWork(jobConf);
          List<Path> paths = Utilities.getInputPathsTez(jobConf, mapWork);
          FileSystem fs = paths.get(0).getFileSystem(jobConf);
          FileStatus[] fileStatuses = fs.listStatus(paths.get(0));
          if (fileStatuses.length == 0) {
            // generate single split typically happens when reading data out of order by queries.
            // if order by query returns no rows, no files will exists in input path
            splits = new InputSplit[0];
          } else {
            // if files exists in input path then it has to be 1 as this code path gets triggered only
            // of order by queries which is expected to write only one file (written by one reducer)
            Preconditions.checkState(paths.size() == 1 && fileStatuses.length == 1 &&
                mapWork.getAliasToPartnInfo().size() == 1,
              "Requested to generate single split. Paths and fileStatuses are expected to be 1. " +
                "Got paths: " + paths.size() + " fileStatuses: " + fileStatuses.length);
            splits = new InputSplit[1];
            FileStatus fileStatus = fileStatuses[0];
            BlockLocation[] locations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
            Set<String> hostsSet = new HashSet<>();
            for (BlockLocation location : locations) {
              hostsSet.addAll(Lists.newArrayList(location.getHosts()));
            }
            String[] hosts = hostsSet.toArray(new String[0]);
            FileSplit fileSplit = new FileSplit(fileStatus.getPath(), 0, fileStatus.getLen(), hosts);
            String alias = mapWork.getAliases().get(0);
            PartitionDesc partDesc = mapWork.getAliasToPartnInfo().get(alias);
            String partIF = partDesc.getInputFileFormatClassName();
            splits[0] = new HiveInputFormat.HiveInputSplit(fileSplit, partIF);
          }
        } else {
          // Raw splits
          splits = inputFormat.getSplits(jobConf, numSplits.orElse(Math.multiplyExact(availableSlots, (int)waves)));
        }
        // Sort the splits, so that subsequent grouping is consistent.
        Arrays.sort(splits, new InputSplitComparator());
        LOG.info("Number of input splits: " + splits.length + ". " + availableSlots
          + " available slots, " + waves + " waves. Input format is: " + realInputFormatName);

        // increment/set input counters
        InputInitializerContext inputInitializerContext = getContext();
        TezCounters tezCounters = null;
        String counterName;
        String groupName = null;
        String vertexName = null;
        if (inputInitializerContext != null) {
          try {
            tezCounters = new TezCounters();
            groupName = HiveInputCounters.class.getName();
            vertexName = jobConf.get(Operator.CONTEXT_NAME_KEY, "");
            counterName = Utilities.getVertexCounterName(HiveInputCounters.RAW_INPUT_SPLITS.name(), vertexName);
            tezCounters.findCounter(groupName, counterName).increment(splits.length);
            final List<Path> paths = Utilities.getInputPathsTez(jobConf, work);
            counterName = Utilities.getVertexCounterName(HiveInputCounters.INPUT_DIRECTORIES.name(), vertexName);
            tezCounters.findCounter(groupName, counterName).increment(paths.size());
            final Set<String> files = new HashSet<>();
            for (InputSplit inputSplit : splits) {
              if (inputSplit instanceof FileSplit) {
                final FileSplit fileSplit = (FileSplit) inputSplit;
                final Path path = fileSplit.getPath();
                // The assumption here is the path is a file. Only case this is different is ACID deltas.
                // The isFile check is avoided here for performance reasons.
                final String fileStr = path.toString();
                if (!files.contains(fileStr)) {
                  files.add(fileStr);
                }
              }
            }
            counterName = Utilities.getVertexCounterName(HiveInputCounters.INPUT_FILES.name(), vertexName);
            tezCounters.findCounter(groupName, counterName).increment(files.size());
          } catch (Exception e) {
            LOG.warn("Caught exception while trying to update Tez counters", e);
          }
        }

        if (work.getIncludedBuckets() != null) {
          splits = pruneBuckets(work, splits);
        }

        Multimap<Integer, InputSplit> groupedSplits =
            splitGrouper.generateGroupedSplits(jobConf, conf, splits, waves, availableSlots, splitLocationProvider);
        // And finally return them in a flat array
        InputSplit[] flatSplits = groupedSplits.values().toArray(new InputSplit[0]);
        LOG.info("Number of split groups: " + flatSplits.length);
        if (inputInitializerContext != null) {
          try {
            counterName = Utilities.getVertexCounterName(HiveInputCounters.GROUPED_INPUT_SPLITS.name(), vertexName);
            tezCounters.findCounter(groupName, counterName).setValue(flatSplits.length);

            LOG.debug("Published tez counters: {}", tezCounters);

            inputInitializerContext.addCounters(tezCounters);
          } catch (Exception e) {
            LOG.warn("Caught exception while trying to update Tez counters", e);
          }
        }

        List<TaskLocationHint> locationHints = splitGrouper.createTaskLocationHints(flatSplits, generateConsistentSplits);

        inputSplitInfo =
            new InputSplitInfoMem(flatSplits, locationHints, flatSplits.length, null, jobConf);
      } else {
        // no need for grouping and the target #of tasks.
        // This code path should never be triggered at the moment. If grouping is disabled,
        // DAGUtils uses MRInputAMSplitGenerator.
        // If this is used in the future - make sure to disable grouping in the payload, if it isn't already disabled
        throw new RuntimeException(
            "HiveInputFormat does not support non-grouped splits, InputFormatName is: "
                + realInputFormatName);
        // inputSplitInfo = MRInputHelpers.generateInputSplitsToMem(jobConf, false, 0);
      }

      return createEventList(sendSerializedEvents, inputSplitInfo);
    } finally {
      Utilities.clearWork(jobConf);
    }
  }

  private InputSplit[] pruneBuckets(MapWork work, InputSplit[] splits) {
    final BitSet buckets = work.getIncludedBuckets();
    final String bucketIn = buckets.toString();
    List<InputSplit> filteredSplits = new ArrayList<InputSplit>(splits.length / 2);
    for (InputSplit split : splits) {
      final int bucket = Utilities.parseSplitBucket(split);
      if (bucket < 0 || buckets.get(bucket)) {
        // match or UNKNOWN
        filteredSplits.add(split);
      } else {
        LOG.info("Pruning with IN ({}) - removing {}", bucketIn, split);
      }
    }
    if (filteredSplits.size() < splits.length) {
      // reallocate only if any filters pruned
      splits = filteredSplits.toArray(new InputSplit[filteredSplits.size()]);
    }
    return splits;
  }

  private List<Event> createEventList(boolean sendSerializedEvents, InputSplitInfoMem inputSplitInfo) {

    List<Event> events = Lists.newArrayListWithCapacity(inputSplitInfo.getNumTasks() + 1);

    InputConfigureVertexTasksEvent configureVertexEvent =
        InputConfigureVertexTasksEvent.create(inputSplitInfo.getNumTasks(),
        VertexLocationHint.create(inputSplitInfo.getTaskLocationHints()),
        InputSpecUpdate.getDefaultSinglePhysicalInputSpecUpdate());
    events.add(configureVertexEvent);

    if (sendSerializedEvents) {
      MRSplitsProto splitsProto = inputSplitInfo.getSplitsProto();
      int count = 0;
      for (MRSplitProto mrSplit : splitsProto.getSplitsList()) {
        InputDataInformationEvent diEvent = InputDataInformationEvent.createWithSerializedPayload(
            count++, mrSplit.toByteString().asReadOnlyByteBuffer());
        events.add(diEvent);
      }
    } else {
      int count = 0;
      for (org.apache.hadoop.mapred.InputSplit split : inputSplitInfo.getOldFormatSplits()) {
        InputDataInformationEvent diEvent = InputDataInformationEvent.createWithObjectPayload(
            count++, split);
        events.add(diEvent);
      }
    }
    return events;
  }

  @Override
  public void onVertexStateUpdated(VertexStateUpdate stateUpdate) {
    // pruner registers for vertex state updates after it is ready to handle them
    // so we do not worry about events coming before pruner was initialized
    pruner.processVertex(stateUpdate.getVertexName());
  }

  @Override
  public void handleInputInitializerEvent(List<InputInitializerEvent> events) throws Exception {
    for (InputInitializerEvent e : events) {
      pruner.addEvent(e);
    }
  }

  // Descending sort based on split size| Followed by file name. Followed by startPosition.
  static class InputSplitComparator implements Comparator<InputSplit> {
    @Override
    public int compare(InputSplit o1, InputSplit o2) {
      try {
        long len1 = o1.getLength();
        long len2 = o2.getLength();
        if (len1 < len2) {
          return 1;
        } else if (len1 == len2) {
          // If the same size. Sort on file name followed by startPosition.
          if (o1 instanceof FileSplit && o2 instanceof FileSplit) {
            FileSplit fs1 = (FileSplit) o1;
            FileSplit fs2 = (FileSplit) o2;
            if (fs1.getPath() != null && fs2.getPath() != null) {
              int pathComp = (fs1.getPath().compareTo(fs2.getPath()));
              if (pathComp == 0) {
                // Compare start Position
                long startPos1 = fs1.getStart();
                long startPos2 = fs2.getStart();
                if (startPos1 > startPos2) {
                  return 1;
                } else if (startPos1 < startPos2) {
                  return -1;
                } else {
                  return 0;
                }
              } else {
                return pathComp;
              }
            }
          }
          // No further checks if not a file split. Return equality.
          return 0;
        } else {
          return -1;
        }
      } catch (IOException e) {
        throw new RuntimeException("Problem getting input split size", e);
      }
    }
  }

  private AvailableSlotsCalculator getAvailableSlotsCalculator() throws Exception {
    Class<?> clazz = Class.forName(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_SPLITS_AVAILABLE_SLOTS_CALCULATOR_CLASS),
            true, Utilities.getSessionSpecifiedClassLoader());
    AvailableSlotsCalculator slotsCalculator = (AvailableSlotsCalculator) ReflectionUtil.newInstance(clazz, null);
    slotsCalculator.initialize(conf, this);
    return slotsCalculator;
  }
}
