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

package org.apache.hadoop.hive.ql.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.hive.common.StringInternUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.ql.Driver.LockedDriverState;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.parse.SplitSample;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.HadoopShims.CombineFileInputFormatShim;
import org.apache.hadoop.hive.shims.HadoopShimsSecure.InputSplitShim;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;


/**
 * CombineHiveInputFormat is a parameterized InputFormat which looks at the path
 * name and determine the correct InputFormat for that path name from
 * mapredPlan.pathToPartitionInfo(). It can be used to read files with different
 * input format in the same map-reduce job.
 */
public class CombineHiveInputFormat<K extends WritableComparable, V extends Writable>
    extends HiveInputFormat<K, V> {

  private static final String CLASS_NAME = CombineHiveInputFormat.class.getName();
  public static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  // max number of threads we can use to check non-combinable paths
  private static final int MAX_CHECK_NONCOMBINABLE_THREAD_NUM = 50;
  private static final int DEFAULT_NUM_PATH_PER_THREAD = 100;

  private class CheckNonCombinablePathCallable implements Callable<Set<Integer>> {
    private final Path[] paths;
    private final int start;
    private final int length;
    private final JobConf conf;
    private final boolean isMerge;

    public CheckNonCombinablePathCallable(
        Path[] paths, int start, int length, JobConf conf, boolean isMerge) {
      this.paths = paths;
      this.start = start;
      this.length = length;
      this.conf = conf;
      this.isMerge = isMerge;
    }

    @Override
    public Set<Integer> call() throws Exception {
      Set<Integer> nonCombinablePathIndices = new HashSet<Integer>();
      for (int i = 0; i < length; i++) {
        PartitionDesc part = HiveFileFormatUtils.getFromPathRecursively(
                pathToPartitionInfo, paths[i + start],
                IOPrepareCache.get().allocatePartitionDescMap());
        // Use HiveInputFormat if any of the paths is not splittable
        Class<? extends InputFormat> inputFormatClass = part.getInputFileFormatClass();
        InputFormat<WritableComparable, Writable> inputFormat =
            getInputFormatFromCache(inputFormatClass, conf);
        boolean isAvoidSplitCombine = inputFormat instanceof AvoidSplitCombination &&
            ((AvoidSplitCombination) inputFormat).shouldSkipCombine(paths[i + start], conf);
        TableDesc tbl = part.getTableDesc();
        boolean isMmNonMerge = false;
        if (tbl != null) {
          isMmNonMerge = !isMerge && AcidUtils.isInsertOnlyTable(tbl.getProperties());
        } else {
          // This would be the case for obscure tasks like truncate column (unsupported for MM).
          Utilities.FILE_OP_LOGGER.warn("Assuming not insert-only; no table in partition spec " + part);
        }

        if (isAvoidSplitCombine || isMmNonMerge) {
          Utilities.FILE_OP_LOGGER.info("The path [" + paths[i + start] +
              "] is being parked for HiveInputFormat.getSplits");
          nonCombinablePathIndices.add(i + start);
        }
      }
      return nonCombinablePathIndices;
    }
  }

  /**
   * CombineHiveInputSplit encapsulates an InputSplit with its corresponding
   * inputFormatClassName. A CombineHiveInputSplit comprises of multiple chunks
   * from different files. Since, they belong to a single directory, there is a
   * single inputformat for all the chunks.
   */
  public static class CombineHiveInputSplit extends InputSplitShim {

    private String inputFormatClassName;
    private CombineFileSplit inputSplitShim;
    private Map<Path, PartitionDesc> pathToPartitionInfo;

    public CombineHiveInputSplit() throws IOException {
      this(ShimLoader.getHadoopShims().getCombineFileInputFormat()
          .getInputSplitShim());
    }

    public CombineHiveInputSplit(CombineFileSplit inputSplitShim) throws IOException {
      this(inputSplitShim.getJob(), inputSplitShim);
    }
    public CombineHiveInputSplit(JobConf job, CombineFileSplit inputSplitShim)
        throws IOException {
      this(job, inputSplitShim, null);
    }
    public CombineHiveInputSplit(JobConf job, CombineFileSplit inputSplitShim,
        Map<Path, PartitionDesc> pathToPartitionInfo) throws IOException {
      this.inputSplitShim = inputSplitShim;
      this.pathToPartitionInfo = pathToPartitionInfo;
      if (job != null) {
        if (this.pathToPartitionInfo == null) {
          this.pathToPartitionInfo = Utilities.getMapWork(job).getPathToPartitionInfo();
        }

        // extract all the inputFormatClass names for each chunk in the
        // CombinedSplit.
        Path[] ipaths = inputSplitShim.getPaths();
        if (ipaths.length > 0) {
          PartitionDesc part = HiveFileFormatUtils
              .getFromPathRecursively(this.pathToPartitionInfo,
                  ipaths[0], IOPrepareCache.get().getPartitionDescMap());
          inputFormatClassName = part.getInputFileFormatClass().getName();
        }
      }
    }

    public CombineFileSplit getInputSplitShim() {
      return inputSplitShim;
    }

    /**
     * Returns the inputFormat class name for the i-th chunk.
     */
    public String inputFormatClassName() {
      return inputFormatClassName;
    }

    public void setInputFormatClassName(String inputFormatClassName) {
      this.inputFormatClassName = inputFormatClassName;
    }

    @Override
    public JobConf getJob() {
      return inputSplitShim.getJob();
    }

    @Override
    public long getLength() {
      return inputSplitShim.getLength();
    }

    /** Returns an array containing the startoffsets of the files in the split. */
    @Override
    public long[] getStartOffsets() {
      return inputSplitShim.getStartOffsets();
    }

    /** Returns an array containing the lengths of the files in the split. */
    @Override
    public long[] getLengths() {
      return inputSplitShim.getLengths();
    }

    /** Returns the start offset of the i<sup>th</sup> Path. */
    @Override
    public long getOffset(int i) {
      return inputSplitShim.getOffset(i);
    }

    /** Returns the length of the i<sup>th</sup> Path. */
    @Override
    public long getLength(int i) {
      return inputSplitShim.getLength(i);
    }

    /** Returns the number of Paths in the split. */
    @Override
    public int getNumPaths() {
      return inputSplitShim.getNumPaths();
    }

    /** Returns the i<sup>th</sup> Path. */
    @Override
    public Path getPath(int i) {
      return inputSplitShim.getPath(i);
    }

    /** Returns all the Paths in the split. */
    @Override
    public Path[] getPaths() {
      return inputSplitShim.getPaths();
    }

    /** Returns all the Paths where this input-split resides. */
    @Override
    public String[] getLocations() throws IOException {
      return inputSplitShim.getLocations();
    }

    /**
     * Prints this obejct as a string.
     */
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(inputSplitShim.toString());
      sb.append("InputFormatClass: " + inputFormatClassName);
      sb.append("\n");
      return sb.toString();
    }

    /**
     * Writable interface.
     */
    @Override
    public void readFields(DataInput in) throws IOException {
      inputSplitShim.readFields(in);
      inputFormatClassName = in.readUTF();
    }

    /**
     * Writable interface.
     */
    @Override
    public void write(DataOutput out) throws IOException {
      inputSplitShim.write(out);
      if (inputFormatClassName == null) {
        if (pathToPartitionInfo == null) {
          pathToPartitionInfo = Utilities.getMapWork(getJob()).getPathToPartitionInfo();
        }

        // extract all the inputFormatClass names for each chunk in the
        // CombinedSplit.
        PartitionDesc part = HiveFileFormatUtils.getFromPathRecursively(pathToPartitionInfo,
            inputSplitShim.getPath(0), IOPrepareCache.get().getPartitionDescMap());

        // create a new InputFormat instance if this is the first time to see
        // this class
        inputFormatClassName = part.getInputFileFormatClass().getName();
      }

      out.writeUTF(inputFormatClassName);
    }
  }

  // Splits are not shared across different partitions with different input formats.
  // For example, 2 partitions (1 sequencefile and 1 rcfile) will have 2 different splits
  private static class CombinePathInputFormat {
    private final List<Operator<? extends OperatorDesc>> opList;
    private final String inputFormatClassName;
    private final String deserializerClassName;

    public CombinePathInputFormat(List<Operator<? extends OperatorDesc>> opList,
                                  String inputFormatClassName,
                                  String deserializerClassName) {
      this.opList = opList;
      this.inputFormatClassName = inputFormatClassName;
      this.deserializerClassName = deserializerClassName;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof CombinePathInputFormat) {
        CombinePathInputFormat mObj = (CombinePathInputFormat) o;
        return (opList.equals(mObj.opList)) &&
            (inputFormatClassName.equals(mObj.inputFormatClassName)) &&
            (deserializerClassName == null ? (mObj.deserializerClassName == null) :
                deserializerClassName.equals(mObj.deserializerClassName));
      }
      return false;
    }

    @Override
    public int hashCode() {
      return (opList == null) ? 0 : opList.hashCode();
    }
  }

  /**
   * Create Hive splits based on CombineFileSplit.
   */
  private InputSplit[] getCombineSplits(JobConf job, int numSplits,
      Map<Path, PartitionDesc> pathToPartitionInfo)
      throws IOException {
    init(job);
    Map<Path, ArrayList<String>> pathToAliases = mrwork.getPathToAliases();
    Map<String, Operator<? extends OperatorDesc>> aliasToWork =
      mrwork.getAliasToWork();
    CombineFileInputFormatShim combine = ShimLoader.getHadoopShims()
        .getCombineFileInputFormat();

    InputSplit[] splits = null;
    if (combine == null) {
      splits = super.getSplits(job, numSplits);
      return splits;
    }

    if (combine.getInputPathsShim(job).length == 0) {
      throw new IOException("No input paths specified in job");
    }
    ArrayList<InputSplit> result = new ArrayList<InputSplit>();

    // combine splits only from same tables and same partitions. Do not combine splits from multiple
    // tables or multiple partitions.
    Path[] paths = StringInternUtils.internUriStringsInPathArray(combine.getInputPathsShim(job));

    List<Path> inpDirs = new ArrayList<Path>();
    List<Path> inpFiles = new ArrayList<Path>();
    Map<CombinePathInputFormat, CombineFilter> poolMap =
      new HashMap<CombinePathInputFormat, CombineFilter>();
    Set<Path> poolSet = new HashSet<Path>();
    LockedDriverState lDrvStat = LockedDriverState.getLockedDriverState();

    for (Path path : paths) {
      if (lDrvStat != null && lDrvStat.isAborted()) {
        throw new IOException("Operation is Canceled. ");
      }

      PartitionDesc part = HiveFileFormatUtils.getFromPathRecursively(
          pathToPartitionInfo, path, IOPrepareCache.get().allocatePartitionDescMap());
      TableDesc tableDesc = part.getTableDesc();
      if ((tableDesc != null) && tableDesc.isNonNative()) {
        return super.getSplits(job, numSplits);
      }

      // Use HiveInputFormat if any of the paths is not splittable
      Class inputFormatClass = part.getInputFileFormatClass();
      String inputFormatClassName = inputFormatClass.getName();
      InputFormat inputFormat = getInputFormatFromCache(inputFormatClass, job);
      String deserializerClassName = null;
      try {
        deserializerClassName = part.getDeserializer(job).getClass().getName();
      } catch (Exception e) {
        // ignore
      }
      FileSystem inpFs = path.getFileSystem(job);

      //don't combine if inputformat is a SymlinkTextInputFormat
      if (inputFormat instanceof SymlinkTextInputFormat) {
        splits = super.getSplits(job, numSplits);
        return splits;
      }

      Path filterPath = path;

      // Does a pool exist for this path already
      CombineFilter f = null;
      List<Operator<? extends OperatorDesc>> opList = null;

      if (!mrwork.isMapperCannotSpanPartns()) {
        //if mapper can span partitions, make sure a splits does not contain multiple
        // opList + inputFormatClassName + deserializerClassName combination
        // This is done using the Map of CombinePathInputFormat to PathFilter

        opList = HiveFileFormatUtils.doGetWorksFromPath(
                   pathToAliases, aliasToWork, filterPath);
        CombinePathInputFormat combinePathInputFormat =
            new CombinePathInputFormat(opList, inputFormatClassName, deserializerClassName);
        f = poolMap.get(combinePathInputFormat);
        if (f == null) {
          f = new CombineFilter(filterPath);
          LOG.info("CombineHiveInputSplit creating pool for " + path +
                   "; using filter path " + filterPath);
          combine.createPool(job, f);
          poolMap.put(combinePathInputFormat, f);
        } else {
          LOG.debug("CombineHiveInputSplit: pool is already created for " + path +
                   "; using filter path " + filterPath);
          f.addPath(filterPath);
        }
      } else {
        // In the case of tablesample, the input paths are pointing to files rather than directories.
        // We need to get the parent directory as the filtering path so that all files in the same
        // parent directory will be grouped into one pool but not files from different parent
        // directories. This guarantees that a split will combine all files in the same partition
        // but won't cross multiple partitions if the user has asked so.
        if (!path.getFileSystem(job).getFileStatus(path).isDir()) { // path is not directory
          filterPath = path.getParent();
          inpFiles.add(path);
          poolSet.add(filterPath);
        } else {
          inpDirs.add(path);
        }
      }
    }

    // Processing directories
    List<CombineFileSplit> iss = new ArrayList<CombineFileSplit>();
    if (!mrwork.isMapperCannotSpanPartns()) {
      //mapper can span partitions
      //combine into as few as one split, subject to the PathFilters set
      // using combine.createPool.
      iss = Arrays.asList(combine.getSplits(job, 1));
    } else {
      for (Path path : inpDirs) {
        processPaths(job, combine, iss, path);
      }

      if (inpFiles.size() > 0) {
        // Processing files
        for (Path filterPath : poolSet) {
          combine.createPool(job, new CombineFilter(filterPath));
        }
        processPaths(job, combine, iss, inpFiles.toArray(new Path[0]));
      }
    }

    if (mrwork.getNameToSplitSample() != null && !mrwork.getNameToSplitSample().isEmpty()) {
      iss = sampleSplits(iss);
    }

    for (CombineFileSplit is : iss) {
      CombineHiveInputSplit csplit = new CombineHiveInputSplit(job, is, pathToPartitionInfo);
      result.add(csplit);
    }
    LOG.info("number of splits " + result.size());
    return result.toArray(new InputSplit[result.size()]);
  }

  /**
   * Gets all the path indices that should not be combined
   */
  @VisibleForTesting
  public Set<Integer> getNonCombinablePathIndices(JobConf job, Path[] paths, int numThreads)
      throws ExecutionException, InterruptedException {
    LOG.info("Total number of paths: " + paths.length +
        ", launching " + numThreads + " threads to check non-combinable ones.");
    int numPathPerThread = (int) Math.ceil((double) paths.length / numThreads);

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    List<Future<Set<Integer>>> futureList = new ArrayList<Future<Set<Integer>>>(numThreads);
    try {
      boolean isMerge = mrwork != null && mrwork.isMergeFromResolver();
      for (int i = 0; i < numThreads; i++) {
        int start = i * numPathPerThread;
        int length = i != numThreads - 1 ? numPathPerThread : paths.length - start;
        futureList.add(executor.submit(new CheckNonCombinablePathCallable(
            paths, start, length, job, isMerge)));
      }
      Set<Integer> nonCombinablePathIndices = new HashSet<Integer>();
      for (Future<Set<Integer>> future : futureList) {
        nonCombinablePathIndices.addAll(future.get());
      }
      return nonCombinablePathIndices;
    } finally {
      executor.shutdownNow();
    }
  }

  /**
   * Create Hive splits based on CombineFileSplit.
   */
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.GET_SPLITS);
    init(job);

    ArrayList<InputSplit> result = new ArrayList<InputSplit>();

    Path[] paths = getInputPaths(job);

    List<Path> nonCombinablePaths = new ArrayList<Path>(paths.length / 2);
    List<Path> combinablePaths = new ArrayList<Path>(paths.length / 2);

    int numThreads = Math.min(MAX_CHECK_NONCOMBINABLE_THREAD_NUM,
        (int) Math.ceil((double) paths.length / DEFAULT_NUM_PATH_PER_THREAD));

    // This check is necessary because for Spark branch, the result array from
    // getInputPaths() above could be empty, and therefore numThreads could be 0.
    // In that case, Executors.newFixedThreadPool will fail.
    if (numThreads > 0) {
      try {
        Set<Integer> nonCombinablePathIndices = getNonCombinablePathIndices(job, paths, numThreads);
        for (int i = 0; i < paths.length; i++) {
          if (nonCombinablePathIndices.contains(i)) {
            nonCombinablePaths.add(paths[i]);
          } else {
            combinablePaths.add(paths[i]);
          }
        }
      } catch (Exception e) {
        LOG.error("Error checking non-combinable path", e);
        perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.GET_SPLITS);
        throw new IOException(e);
      }
    }

    // Store the previous value for the path specification
    String oldPaths = job.get(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR);
    if (LOG.isDebugEnabled()) {
      LOG.debug("The received input paths are: [" + oldPaths +
          "] against the property "
          + org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR);
    }

    // Process the normal splits
    if (nonCombinablePaths.size() > 0) {
      FileInputFormat.setInputPaths(job, nonCombinablePaths.toArray
          (new Path[nonCombinablePaths.size()]));
      InputSplit[] splits = super.getSplits(job, numSplits);
      for (InputSplit split : splits) {
        result.add(split);
      }
    }

    // Process the combine splits
    if (combinablePaths.size() > 0) {
      FileInputFormat.setInputPaths(job, combinablePaths.toArray
          (new Path[combinablePaths.size()]));
      Map<Path, PartitionDesc> pathToPartitionInfo = this.pathToPartitionInfo != null ?
          this.pathToPartitionInfo : Utilities.getMapWork(job).getPathToPartitionInfo();
      InputSplit[] splits = getCombineSplits(job, numSplits, pathToPartitionInfo);
      for (InputSplit split : splits) {
        result.add(split);
      }
    }

    // Restore the old path information back
    // This is just to prevent incompatibilities with previous versions Hive
    // if some application depends on the original value being set.
    if (oldPaths != null) {
      job.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR, oldPaths);
    }

    // clear work from ThreadLocal after splits generated in case of thread is reused in pool.
    Utilities.clearWorkMapForConf(job);

    if (result.isEmpty() && paths.length > 0 && job.getBoolean(Utilities.ENSURE_OPERATORS_EXECUTED, false)) {
      // If there are no inputs; the Execution engine skips the operator tree.
      // To prevent it from happening; an opaque  ZeroRows input is added here - when needed.
      result.add(
          new HiveInputSplit(new NullRowsInputFormat.DummyInputSplit(paths[0]), ZeroRowsInputFormat.class.getName()));
    }

    LOG.info("Number of all splits " + result.size());
    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.GET_SPLITS);
    return result.toArray(new InputSplit[result.size()]);
  }

  private void processPaths(JobConf job, CombineFileInputFormatShim combine,
      List<CombineFileSplit> iss, Path... path) throws IOException {
    JobConf currJob = new JobConf(job);
    FileInputFormat.setInputPaths(currJob, path);
    iss.addAll(Arrays.asList(combine.getSplits(currJob, 1)));
  }

  /**
   * This function is used to sample inputs for clauses like "TABLESAMPLE(1 PERCENT)"
   *
   * First, splits are grouped by alias they are for. If one split serves more than one
   * alias or not for any sampled alias, we just directly add it to returned list.
   * Then we find a list of exclusive splits for every alias to be sampled.
   * For each alias, we start from position of seedNumber%totalNumber, and keep add
   * splits until the total size hits percentage.
   * @param splits
   * @return the sampled splits
   */
  private List<CombineFileSplit> sampleSplits(List<CombineFileSplit> splits) {
    HashMap<String, SplitSample> nameToSamples = mrwork.getNameToSplitSample();
    List<CombineFileSplit> retLists = new ArrayList<CombineFileSplit>();
    Map<String, ArrayList<CombineFileSplit>> aliasToSplitList = new HashMap<String, ArrayList<CombineFileSplit>>();
    Map<Path, ArrayList<String>> pathToAliases = mrwork.getPathToAliases();
    Map<Path, ArrayList<String>> pathToAliasesNoScheme = removeScheme(pathToAliases);

    // Populate list of exclusive splits for every sampled alias
    //
    for (CombineFileSplit split : splits) {
      String alias = null;
      for (Path path : split.getPaths()) {
        boolean schemeless = path.toUri().getScheme() == null;
        List<String> l = HiveFileFormatUtils.doGetAliasesFromPath(
            schemeless ? pathToAliasesNoScheme : pathToAliases, path);
        // a path for a split unqualified the split from being sampled if:
        // 1. it serves more than one alias
        // 2. the alias it serves is not sampled
        // 3. it serves different alias than another path for the same split
        if (l.size() != 1 || !nameToSamples.containsKey(l.get(0)) ||
            (alias != null && !alias.equals(l.get(0)))) {
          alias = null;
          break;
        }
        alias = l.get(0);
      }

      if (alias != null) {
        // split exclusively serves alias, which needs to be sampled
        // add it to the split list of the alias.
        if (!aliasToSplitList.containsKey(alias)) {
          aliasToSplitList.put(alias, new ArrayList<CombineFileSplit>());
        }
        aliasToSplitList.get(alias).add(split);
      } else {
        // The split doesn't exclusively serve one alias
        retLists.add(split);
      }
    }

    // for every sampled alias, we figure out splits to be sampled and add
    // them to return list
    //
    for (Map.Entry<String, ArrayList<CombineFileSplit>> entry: aliasToSplitList.entrySet()) {
      ArrayList<CombineFileSplit> splitList = entry.getValue();
      long totalSize = 0;
      for (CombineFileSplit split : splitList) {
        totalSize += split.getLength();
      }

      SplitSample splitSample = nameToSamples.get(entry.getKey());

      long targetSize = splitSample.getTargetSize(totalSize);
      int startIndex = splitSample.getSeedNum() % splitList.size();
      long size = 0;
      for (int i = 0; i < splitList.size(); i++) {
        CombineFileSplit split = splitList.get((startIndex + i) % splitList.size());
        retLists.add(split);
        long splitgLength = split.getLength();
        if (size + splitgLength >= targetSize) {
          LOG.info("Sample alias " + entry.getValue() + " using " + (i + 1) + "splits");
          if (size + splitgLength > targetSize) {
            ((InputSplitShim)split).shrinkSplit(targetSize - size);
          }
          break;
        }
        size += splitgLength;
      }

    }

    return retLists;
  }

  Map<Path, ArrayList<String>> removeScheme(Map<Path, ArrayList<String>> pathToAliases) {
    Map<Path, ArrayList<String>> result = new HashMap<>();
    for (Map.Entry <Path, ArrayList<String>> entry : pathToAliases.entrySet()) {
      Path newKey = Path.getPathWithoutSchemeAndAuthority(entry.getKey());
      StringInternUtils.internUriStringsInPath(newKey);
      result.put(newKey, entry.getValue());
    }
    return result;
  }

  /**
   * Create a generic Hive RecordReader than can iterate over all chunks in a
   * CombinedFileSplit.
   */
  @Override
  public RecordReader getRecordReader(InputSplit split, JobConf job,
      Reporter reporter) throws IOException {
    if (!(split instanceof CombineHiveInputSplit)) {
      return super.getRecordReader(split, job, reporter);
    }

    CombineHiveInputSplit hsplit = (CombineHiveInputSplit) split;

    String inputFormatClassName = null;
    Class inputFormatClass = null;
    try {
      inputFormatClassName = hsplit.inputFormatClassName();
      inputFormatClass = job.getClassByName(inputFormatClassName);
    } catch (Exception e) {
      throw new IOException("cannot find class " + inputFormatClassName);
    }

    pushProjectionsAndFilters(job, inputFormatClass, hsplit.getPath(0));

    return ShimLoader.getHadoopShims().getCombineFileInputFormat()
        .getRecordReader(job,
        (CombineFileSplit) split, reporter,
        CombineHiveRecordReader.class);
  }

  static class CombineFilter implements PathFilter {
    private final Set<String> pStrings = new HashSet<String>();

    // store a path prefix in this TestFilter
    // PRECONDITION: p should always be a directory
    public CombineFilter(Path p) {
      // we need to keep the path part only because the Hadoop CombineFileInputFormat will
      // pass the path part only to accept().
      // Trailing the path with a separator to prevent partial matching.
      addPath(p);
    }

    public void addPath(Path p) {
      String pString = p.toUri().getPath();
      pStrings.add(pString);
    }

    // returns true if the specified path matches the prefix stored
    // in this TestFilter.
    @Override
    public boolean accept(Path path) {
      boolean find = false;
      while (path != null && !find) {
         if(pStrings.contains(path.toUri().getPath())) {
          find = true;
          break;
        }
        path = path.getParent();
      }
      return find;
    }

    @Override
    public String toString() {
      StringBuilder s = new StringBuilder();
      s.append("PathFilter: ");
      for (String pString : pStrings) {
        s.append(pString + " ");
      }
      return s.toString();
    }
  }

    /**
     * This is a marker interface that is used to identify the formats where
     * combine split generation is not applicable
     */
  public interface AvoidSplitCombination {
    boolean shouldSkipCombine(Path path, Configuration conf) throws IOException;
  }
}
