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
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSplit;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.split.SplitLocationProvider;
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

  private static final Logger LOG = LoggerFactory.getLogger(SplitGrouper.class);

  // TODO This needs to be looked at. Map of Map to Map... Made concurrent for now since split generation
  // can happen in parallel.
  private static final Map<Map<Path, PartitionDesc>, Map<Path, PartitionDesc>> cache =
      new ConcurrentHashMap<>();

  private final TezMapredSplitsGrouper tezGrouper = new TezMapredSplitsGrouper();

  /**
   * group splits for each bucket separately - while evenly filling all the
   * available slots with tasks
   */
  public Multimap<Integer, InputSplit> group(Configuration conf,
      Multimap<Integer, InputSplit> bucketSplitMultimap, int availableSlots, float waves,
                                             SplitLocationProvider splitLocationProvider)
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
      Class inputFormatClass = conf.getClass("mapred.input.format.class",
              HiveInputFormat.class);
      if (inputFormatClass != BucketizedHiveInputFormat.class &&
              inputFormatClass != HiveInputFormat.class) {
        // As of now only these two formats are supported.
        inputFormatClass = HiveInputFormat.class;
      }
      InputSplit[] rawSplits = inputSplitCollection.toArray(new InputSplit[0]);
      InputSplit[] groupedSplits =
          tezGrouper.getGroupedSplits(conf, rawSplits, bucketTaskMap.get(bucketId),
                  inputFormatClass.getName(), new ColumnarSplitSizeEstimator(), splitLocationProvider);

      LOG.info("Original split count is " + rawSplits.length + " grouped split count is "
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
   * @param consistentLocations whether to re-order locations for each split, if it's a file split
   * @return taskLocationHints - 1 per input split specified
   * @throws IOException
   */
  public List<TaskLocationHint> createTaskLocationHints(InputSplit[] splits, boolean consistentLocations) throws IOException {

    List<TaskLocationHint> locationHints = Lists.newArrayListWithCapacity(splits.length);

    for (InputSplit split : splits) {
      String rack = (split instanceof TezGroupedSplit) ? ((TezGroupedSplit) split).getRack() : null;
      if (rack == null) {
        String [] locations = split.getLocations();
        if (locations != null && locations.length > 0) {
          // Worthwhile only if more than 1 split, consistentGroupingEnabled and is a FileSplit
          if (consistentLocations && locations.length > 1 && split instanceof FileSplit) {
            Arrays.sort(locations);
            FileSplit fileSplit = (FileSplit) split;
            Path path = fileSplit.getPath();
            long startLocation = fileSplit.getStart();
            int hashCode = Objects.hash(path, startLocation);
            int startIndex = hashCode % locations.length;
            LinkedHashSet<String> locationSet = new LinkedHashSet<>(locations.length);
            // Set up the locations starting from startIndex, and wrapping around the sorted array.
            for (int i = 0 ; i < locations.length ; i++) {
              int index = (startIndex + i) % locations.length;
              locationSet.add(locations[index]);
            }
            locationHints.add(TaskLocationHint.createTaskLocationHint(locationSet, null));
          } else {
            locationHints.add(TaskLocationHint
                .createTaskLocationHint(new LinkedHashSet<String>(Arrays.asList(split
                    .getLocations())), null));
          }
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
                                                                    float waves, int availableSlots,
                                                                    SplitLocationProvider locationProvider)
      throws Exception {
    return generateGroupedSplits(jobConf, conf, splits, waves, availableSlots, null, true, locationProvider);
  }

  /**
   * Generate groups of splits, separated by schema evolution boundaries
   * OR
   * When used from compactor, group splits based on the bucket number of the input files
   * (in this case, splits for same logical bucket but different schema, end up in same group)
   */
  public Multimap<Integer, InputSplit> generateGroupedSplits(JobConf jobConf, Configuration conf, InputSplit[] splits,
      float waves, int availableSlots, String inputName, boolean groupAcrossFiles,
      SplitLocationProvider locationProvider) throws Exception {
    boolean isMinorCompaction = true;
    MapWork mapWork = populateMapWork(jobConf, inputName);
    // ArrayListMultimap is important here to retain the ordering for the splits.
    Multimap<Integer, InputSplit> schemaGroupedSplitMultiMap = ArrayListMultimap.<Integer, InputSplit> create();
    if (HiveConf.getVar(jobConf, HiveConf.ConfVars.SPLIT_GROUPING_MODE).equalsIgnoreCase("compactor")) {
      List<Path> paths = Utilities.getInputPathsTez(jobConf, mapWork);
      for (Path path : paths) {
        List<String> aliases = mapWork.getPathToAliases().get(path);
        if ((aliases != null) && (aliases.size() == 1)) {
          Operator<? extends OperatorDesc> op = mapWork.getAliasToWork().get(aliases.get(0));
          if ((op != null) && (op instanceof TableScanOperator)) {
            TableScanOperator tableScan = (TableScanOperator) op;
            PartitionDesc partitionDesc = mapWork.getAliasToPartnInfo().get(aliases.get(0));
            isMinorCompaction &= AcidUtils.isCompactionTable(partitionDesc.getTableDesc().getProperties());
            if (!tableScan.getConf().isTranscationalTable() && !isMinorCompaction) {
              String splitPath = getFirstSplitPath(splits);
              String errorMessage =
                  "Compactor split grouping is enabled only for transactional tables. Please check the path: "
                      + splitPath;
              LOG.error(errorMessage);
              throw new RuntimeException(errorMessage);
            }
          }
        }
      }
      /**
       * The expectation is that each InputSplit is a {@link org.apache.hadoop.hive.ql.io.HiveInputFormat.HiveInputSplit} 
       * wrapping an OrcSplit. So group these splits by bucketId and within each bucketId, sort by writeId, stmtId, 
       * rowIdOffset or splitStart. For 'original' splits (w/o acid meta cols in the file) SyntheticBucketProperties 
       * should always be there and so rowIdOffset is there. For 'native' acid files, OrcSplit doesn't have 
       * the 1st rowid in the split, so splitStart is used to sort. This should achieve the required sorting invariance 
       * (sort by: writeId, stmtId, rowIdOffset within each bucket) needed for Acid tables.
       * See: {@link org.apache.hadoop.hive.ql.io.AcidInputFormat}
       * Create a TezGroupedSplit for each bucketId and return.
       * TODO: Are there any other config values (split size etc) that can override this per writer split grouping?
       */
      return getCompactorSplitGroups(splits, conf, isMinorCompaction);
    }

    int i = 0;
    InputSplit prevSplit = null;
    for (InputSplit s : splits) {
      // this is the bit where we make sure we don't group across partition schema boundaries
      if (schemaEvolved(s, prevSplit, groupAcrossFiles, mapWork)) {
        ++i;
        prevSplit = s;
      }
      schemaGroupedSplitMultiMap.put(i, s);
    }
    LOG.info("# Src groups for split generation: " + (i + 1));
    // group them into the chunks we want
    Multimap<Integer, InputSplit> groupedSplits =
        this.group(jobConf, schemaGroupedSplitMultiMap, availableSlots, waves, locationProvider);
    return groupedSplits;
  }
  
  // Returns the path of the first split in this list for logging purposes
  private String getFirstSplitPath(InputSplit[] splits) {
    if (splits.length == 0) {
      throw new RuntimeException("The list of splits provided for grouping is empty.");
    }
    Path splitPath = ((FileSplit) splits[0]).getPath();
   
    return splitPath.toString();
  }


  /**
   * Takes a list of {@link org.apache.hadoop.hive.ql.io.HiveInputFormat.HiveInputSplit}s
   * and groups them for Acid Compactor, creating one TezGroupedSplit per bucket number.
   */
  Multimap<Integer, InputSplit> getCompactorSplitGroups(InputSplit[] rawSplits, Configuration conf,
      boolean isMinorCompaction) {
    // Note: For our case, this multimap will essentially contain one value (one TezGroupedSplit) per key 
    Multimap<Integer, InputSplit> bucketSplitMultiMap = ArrayListMultimap.<Integer, InputSplit> create();
    HiveInputFormat.HiveInputSplit[] splits = new HiveInputFormat.HiveInputSplit[rawSplits.length];
    int i = 0;
    for (InputSplit is : rawSplits) {
      HiveInputFormat.HiveInputSplit hiveInputSplit = (HiveInputFormat.HiveInputSplit) is;
      OrcSplit o1 = (OrcSplit) hiveInputSplit.getInputSplit();
      try {
        if (isMinorCompaction) {
          o1.parse(conf, o1.getRootDir().getParent());
        } else {
          o1.parse(conf);
        }
      } catch (IOException e) {
        throw new RuntimeException();
      }
      splits[i++] = hiveInputSplit;
    }
    Arrays.sort(splits, new ComparatorCompactor());
    TezGroupedSplit tgs = null;
    int previousWriterId = Integer.MIN_VALUE;
    Path rootDir = null;
    for (i = 0; i < splits.length; i++) {
      int writerId = ((OrcSplit) splits[i].getInputSplit()).getBucketId();
      if (!isMinorCompaction) {
        if (rootDir == null) {
          rootDir = ((OrcSplit) splits[i].getInputSplit()).getRootDir();
        }
        Path rootDirFromCurrentSplit = ((OrcSplit) splits[i].getInputSplit()).getRootDir();
        // These splits should belong to the same partition
        assert rootDir.equals(rootDirFromCurrentSplit);
      }
      if (writerId != previousWriterId) {
        // Create a new grouped split for this writerId
        tgs = new TezGroupedSplit(1, "org.apache.hadoop.hive.ql.io.HiveInputFormat", null, null);
        bucketSplitMultiMap.put(writerId, tgs);
      }
      tgs.addSplit(splits[i]);
      previousWriterId = writerId;
    }
    return bucketSplitMultiMap;
  }
  
  static class ComparatorCompactor implements Comparator<HiveInputFormat.HiveInputSplit>, Serializable {

    @Override
    public int compare(HiveInputFormat.HiveInputSplit h1, HiveInputFormat.HiveInputSplit h2) {
      //sort: bucketId,writeId,stmtId,rowIdOffset,splitStart
      if(h1 == h2) {
        return 0;
      }
      OrcSplit o1 = (OrcSplit)h1.getInputSplit();
      OrcSplit o2 = (OrcSplit)h2.getInputSplit();
      // Note: this is the bucket number as seen in the file name.
      // Hive 3.0 encodes a bunch of info in the Acid schema's bucketId attribute.
      // See: {@link org.apache.hadoop.hive.ql.io.BucketCodec.V1} for details.
      if(o1.getBucketId() != o2.getBucketId()) {
        return o1.getBucketId() < o2.getBucketId() ? -1 : 1;
      }
      if(o1.getWriteId() != o2.getWriteId()) {
        return o1.getWriteId() < o2.getWriteId() ? -1 : 1;
      }
      if(o1.getStatementId() != o2.getStatementId()) {
        return o1.getStatementId() < o2.getStatementId() ? -1 : 1;
      }
      long rowOffset1 = o1.getSyntheticAcidProps() == null ? 0 : o1.getSyntheticAcidProps().getRowIdOffset();
      long rowOffset2 = o2.getSyntheticAcidProps() == null ? 0 : o2.getSyntheticAcidProps().getRowIdOffset();
      if(rowOffset1 != rowOffset2) {
        //if 2 splits are from the same file (delta/base in fact), they either both have syntheticAcidProps or both do not
        return rowOffset1 < rowOffset2 ? -1 : 1;
      }
      if(o1.getStart() != o2.getStart()) {
        return o1.getStart() < o2.getStart() ? -1 : 1;
      }
      throw new RuntimeException("Found 2 equal splits: " + o1 + " and " + o2);
    }
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

    // TODO HIVE-12255. Make use of SplitSizeEstimator.
    // The actual task computation needs to be looked at as well.
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
    PartitionDesc pd = HiveFileFormatUtils.getFromPathRecursively(
        work.getPathToPartitionInfo(), path, cache);
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
          HiveFileFormatUtils.getFromPathRecursively(work.getPathToPartitionInfo(),
              prevPath, cache);
      previousDeserializerClass = prevPD.getDeserializerClassName();
      previousInputFormatClass = prevPD.getInputFileFormatClass();
    }

    if ((currentInputFormatClass != previousInputFormatClass)
        || (!currentDeserializerClass.equals(previousDeserializerClass))) {
      retval = true;
    }

    LOG.debug("Adding split {} to src new group? {}", path, retval);

    return retval;
  }



}
