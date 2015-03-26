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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.split.TezGroupedSplit;
import org.apache.hadoop.mapred.split.TezMapredSplitsGrouper;
import org.apache.tez.dag.api.TaskLocationHint;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

/**
 * SplitGrouper is used to combine splits based on head room and locality. It
 * also enforces restrictions around schema, file format and bucketing.
 */
public class SplitGrouper {

  private static final Log LOG = LogFactory.getLog(SplitGrouper.class);

  // TODO This needs to be looked at. Map of Map to Map... Made concurrent for now since split generation
  // can happen in parallel.
  private static final Map<Map<String, PartitionDesc>, Map<String, PartitionDesc>> cache =
      new ConcurrentHashMap<>();

  private final TezMapredSplitsGrouper tezGrouper = new TezMapredSplitsGrouper();



  /**
   * group splits for each bucket separately - while evenly filling all the
   * available slots with tasks
   */
  public Multimap<Integer, InputSplit> group(Configuration conf,
      Multimap<Integer, InputSplit> bucketSplitMultimap, int availableSlots, float waves)
      throws IOException {

    // figure out how many tasks we want for each bucket
    Map<Integer, Integer> bucketTaskMap =
        estimateBucketSizes(availableSlots, waves, bucketSplitMultimap.asMap());

    // allocate map bucket id to grouped splits
    Multimap<Integer, InputSplit> bucketGroupedSplitMultimap =
        ArrayListMultimap.<Integer, InputSplit> create();

    // use the tez grouper to combine splits once per bucket
    for (int bucketId : bucketSplitMultimap.keySet()) {
      Collection<InputSplit> inputSplitCollection = bucketSplitMultimap.get(bucketId);

      InputSplit[] rawSplits = inputSplitCollection.toArray(new InputSplit[0]);
      InputSplit[] groupedSplits =
          tezGrouper.getGroupedSplits(conf, rawSplits, bucketTaskMap.get(bucketId),
              HiveInputFormat.class.getName());

      LOG.info("Original split size is " + rawSplits.length + " grouped split size is "
          + groupedSplits.length + ", for bucket: " + bucketId);

      for (InputSplit inSplit : groupedSplits) {
        bucketGroupedSplitMultimap.put(bucketId, inSplit);
      }
    }

    return bucketGroupedSplitMultimap;
  }


  /**
   * Create task location hints from a set of input splits
   * @param splits the actual splits
   * @return taskLocationHints - 1 per input split specified
   * @throws IOException
   */
  public List<TaskLocationHint> createTaskLocationHints(InputSplit[] splits) throws IOException {

    List<TaskLocationHint> locationHints = Lists.newArrayListWithCapacity(splits.length);

    for (InputSplit split : splits) {
      String rack = (split instanceof TezGroupedSplit) ? ((TezGroupedSplit) split).getRack() : null;
      if (rack == null) {
        if (split.getLocations() != null) {
          locationHints.add(TaskLocationHint.createTaskLocationHint(new HashSet<String>(Arrays.asList(split
              .getLocations())), null));
        } else {
          locationHints.add(TaskLocationHint.createTaskLocationHint(null, null));
        }
      } else {
        locationHints.add(TaskLocationHint.createTaskLocationHint(null, Collections.singleton(rack)));
      }
    }

    return locationHints;
  }

  /** Generate groups of splits, separated by schema evolution boundaries */
  public Multimap<Integer, InputSplit> generateGroupedSplits(JobConf jobConf,
                                                                    Configuration conf,
                                                                    InputSplit[] splits,
                                                                    float waves, int availableSlots)
      throws Exception {
    return generateGroupedSplits(jobConf, conf, splits, waves, availableSlots, null, true);
  }

  /** Generate groups of splits, separated by schema evolution boundaries */
  public Multimap<Integer, InputSplit> generateGroupedSplits(JobConf jobConf,
                                                                    Configuration conf,
                                                                    InputSplit[] splits,
                                                                    float waves, int availableSlots,
                                                                    String inputName,
                                                                    boolean groupAcrossFiles) throws
      Exception {

    MapWork work = populateMapWork(jobConf, inputName);
    Multimap<Integer, InputSplit> bucketSplitMultiMap =
        ArrayListMultimap.<Integer, InputSplit> create();

    int i = 0;
    InputSplit prevSplit = null;
    for (InputSplit s : splits) {
      // this is the bit where we make sure we don't group across partition
      // schema boundaries
      if (schemaEvolved(s, prevSplit, groupAcrossFiles, work)) {
        ++i;
        prevSplit = s;
      }
      bucketSplitMultiMap.put(i, s);
    }
    LOG.info("# Src groups for split generation: " + (i + 1));

    // group them into the chunks we want
    Multimap<Integer, InputSplit> groupedSplits =
        this.group(jobConf, bucketSplitMultiMap, availableSlots, waves);

    return groupedSplits;
  }


  /**
   * get the size estimates for each bucket in tasks. This is used to make sure
   * we allocate the head room evenly
   */
  private Map<Integer, Integer> estimateBucketSizes(int availableSlots, float waves,
                                                    Map<Integer, Collection<InputSplit>> bucketSplitMap) {

    // mapping of bucket id to size of all splits in bucket in bytes
    Map<Integer, Long> bucketSizeMap = new HashMap<Integer, Long>();

    // mapping of bucket id to number of required tasks to run
    Map<Integer, Integer> bucketTaskMap = new HashMap<Integer, Integer>();

    // compute the total size per bucket
    long totalSize = 0;
    boolean earlyExit = false;
    for (int bucketId : bucketSplitMap.keySet()) {
      long size = 0;
      for (InputSplit s : bucketSplitMap.get(bucketId)) {
        // the incoming split may not be a file split when we are re-grouping TezGroupedSplits in
        // the case of SMB join. So in this case, we can do an early exit by not doing the
        // calculation for bucketSizeMap. Each bucket will assume it can fill availableSlots * waves
        // (preset to 0.5) for SMB join.
        if (!(s instanceof FileSplit)) {
          bucketTaskMap.put(bucketId, (int) (availableSlots * waves));
          earlyExit = true;
          continue;
        }
        FileSplit fsplit = (FileSplit) s;
        size += fsplit.getLength();
        totalSize += fsplit.getLength();
      }
      bucketSizeMap.put(bucketId, size);
    }

    if (earlyExit) {
      return bucketTaskMap;
    }

    // compute the number of tasks
    for (int bucketId : bucketSizeMap.keySet()) {
      int numEstimatedTasks = 0;
      if (totalSize != 0) {
        // availableSlots * waves => desired slots to fill
        // sizePerBucket/totalSize => weight for particular bucket. weights add
        // up to 1.
        numEstimatedTasks =
            (int) (availableSlots * waves * bucketSizeMap.get(bucketId) / totalSize);
      }

      LOG.info("Estimated number of tasks: " + numEstimatedTasks + " for bucket " + bucketId);
      if (numEstimatedTasks == 0) {
        numEstimatedTasks = 1;
      }
      bucketTaskMap.put(bucketId, numEstimatedTasks);
    }

    return bucketTaskMap;
  }

  private static MapWork populateMapWork(JobConf jobConf, String inputName) {
    MapWork work = null;
    if (inputName != null) {
      work = (MapWork) Utilities.getMergeWork(jobConf, inputName);
      // work can still be null if there is no merge work for this input
    }
    if (work == null) {
      work = Utilities.getMapWork(jobConf);
    }

    return work;
  }

  private boolean schemaEvolved(InputSplit s, InputSplit prevSplit, boolean groupAcrossFiles,
                                       MapWork work) throws IOException {
    boolean retval = false;
    Path path = ((FileSplit) s).getPath();
    PartitionDesc pd =
        HiveFileFormatUtils.getPartitionDescFromPathRecursively(work.getPathToPartitionInfo(),
            path, cache);
    String currentDeserializerClass = pd.getDeserializerClassName();
    Class<?> currentInputFormatClass = pd.getInputFileFormatClass();

    Class<?> previousInputFormatClass = null;
    String previousDeserializerClass = null;
    if (prevSplit != null) {
      Path prevPath = ((FileSplit) prevSplit).getPath();
      if (!groupAcrossFiles) {
        return !path.equals(prevPath);
      }
      PartitionDesc prevPD =
          HiveFileFormatUtils.getPartitionDescFromPathRecursively(work.getPathToPartitionInfo(),
              prevPath, cache);
      previousDeserializerClass = prevPD.getDeserializerClassName();
      previousInputFormatClass = prevPD.getInputFileFormatClass();
    }

    if ((currentInputFormatClass != previousInputFormatClass)
        || (!currentDeserializerClass.equals(previousDeserializerClass))) {
      retval = true;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Adding split " + path + " to src new group? " + retval);
    }
    return retval;
  }



}
