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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.StringInternUtils;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.hive.common.util.Ref;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.io.HiveIOExceptionHandlerUtil;
import org.apache.hadoop.hive.llap.io.api.LlapIo;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.spark.SparkDynamicPartitionPruner;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.common.util.ReflectionUtil;

/**
 * HiveInputFormat is a parameterized InputFormat which looks at the path name
 * and determine the correct InputFormat for that path name from
 * mapredPlan.pathToPartitionInfo(). It can be used to read files with different
 * input format in the same map-reduce job.
 */
public class HiveInputFormat<K extends WritableComparable, V extends Writable>
    implements InputFormat<K, V>, JobConfigurable {
  private static final String CLASS_NAME = HiveInputFormat.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  /**
   * A cache of InputFormat instances.
   */
  private static final Map<Class, InputFormat<WritableComparable, Writable>> inputFormats
    = new ConcurrentHashMap<Class, InputFormat<WritableComparable, Writable>>();

  private JobConf job;

  // both classes access by subclasses
  protected Map<Path, PartitionDesc> pathToPartitionInfo;
  protected MapWork mrwork;

  public static final class HiveInputSplitComparator implements Comparator<HiveInputSplit> {
    @Override
    public int compare(HiveInputSplit o1, HiveInputSplit o2) {
      int pathCompare = comparePath(o1.getPath(), o2.getPath());
      if (pathCompare != 0) {
        return pathCompare;
      }
      return Long.compare(o1.getStart(), o2.getStart());
    }

    private int comparePath(Path p1, Path p2) {
      return p1.compareTo(p2);
    }
  }


  /**
   * HiveInputSplit encapsulates an InputSplit with its corresponding
   * inputFormatClass. The reason that it derives from FileSplit is to make sure
   * "map.input.file" in MapTask.
   */
  public static class HiveInputSplit extends FileSplit implements InputSplit,
      Configurable {


    InputSplit inputSplit;
    String inputFormatClassName;

    public HiveInputSplit() {
      // This is the only public constructor of FileSplit
      super((Path) null, 0, 0, (String[]) null);
    }

    public HiveInputSplit(InputSplit inputSplit, String inputFormatClassName) {
      // This is the only public constructor of FileSplit
      super((Path) null, 0, 0, (String[]) null);
      this.inputSplit = inputSplit;
      this.inputFormatClassName = inputFormatClassName;
    }

    public InputSplit getInputSplit() {
      return inputSplit;
    }

    public String inputFormatClassName() {
      return inputFormatClassName;
    }

    @Override
    public Path getPath() {
      if (inputSplit instanceof FileSplit) {
        return ((FileSplit) inputSplit).getPath();
      }
      throw new RuntimeException(inputSplit + " is not a FileSplit");
    }

    /** The position of the first byte in the file to process. */
    @Override
    public long getStart() {
      if (inputSplit instanceof FileSplit) {
        return ((FileSplit) inputSplit).getStart();
      }
      return 0;
    }

    @Override
    public String toString() {
      return inputFormatClassName + ":" + inputSplit.toString();
    }

    @Override
    public long getLength() {
      long r = 0;
      try {
        r = inputSplit.getLength();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return r;
    }

    @Override
    public String[] getLocations() throws IOException {
      return inputSplit.getLocations();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      String inputSplitClassName = in.readUTF();
      try {
        inputSplit = (InputSplit) ReflectionUtil.newInstance(conf
            .getClassByName(inputSplitClassName), conf);
      } catch (Exception e) {
        throw new IOException(
            "Cannot create an instance of InputSplit class = "
            + inputSplitClassName + ":" + e.getMessage(), e);
      }
      inputSplit.readFields(in);
      inputFormatClassName = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(inputSplit.getClass().getName());
      inputSplit.write(out);
      out.writeUTF(inputFormatClassName);
    }

    Configuration conf;

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
    }
  }

  @Override
  public void configure(JobConf job) {
    this.job = job;
  }

  public static InputFormat<WritableComparable, Writable> wrapForLlap(
      InputFormat<WritableComparable, Writable> inputFormat, Configuration conf,
      PartitionDesc part) throws HiveException {
    if (!HiveConf.getBoolVar(conf, ConfVars.LLAP_IO_ENABLED, LlapProxy.isDaemon())) {
      return inputFormat; // LLAP not enabled, no-op.
    }
    String ifName = inputFormat.getClass().getCanonicalName();
    boolean isSupported = inputFormat instanceof LlapWrappableInputFormatInterface;
    boolean isCacheOnly = inputFormat instanceof LlapCacheOnlyInputFormatInterface;
    boolean isVectorized = Utilities.getIsVectorized(conf);
    if (!isVectorized) {
      // Pretend it's vectorized if the non-vector wrapped is enabled.
      isVectorized = HiveConf.getBoolVar(conf, ConfVars.LLAP_IO_NONVECTOR_WRAPPER_ENABLED)
          && (Utilities.getPlanPath(conf) != null);
    }
    boolean isSerdeBased = false;
    if (isVectorized && !isSupported
        && HiveConf.getBoolVar(conf, ConfVars.LLAP_IO_ENCODE_ENABLED)) {
      // See if we can use re-encoding to read the format thru IO elevator.
      isSupported = isSerdeBased = checkInputFormatForLlapEncode(conf, ifName);
    }
    if ((!isSupported || !isVectorized) && !isCacheOnly) {
      if (LOG.isInfoEnabled()) {
        LOG.info("Not using llap for " + ifName + ": supported = "
          + isSupported + ", vectorized = " + isVectorized + ", cache only = " + isCacheOnly);
      }
      return inputFormat;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing " + ifName);
    }

    @SuppressWarnings("unchecked")
    LlapIo<VectorizedRowBatch> llapIo = LlapProxy.getIo();
    if (llapIo == null) {
      if (LOG.isInfoEnabled()) {
        LOG.info("Not using LLAP IO because it is not initialized");
      }
      return inputFormat;
    }
    Deserializer serde = null;
    if (isSerdeBased) {
      if (part == null) {
        if (isCacheOnly) {
          LOG.info("Using cache only because there's no partition spec for SerDe-based IF");
          injectLlapCaches(inputFormat, llapIo, conf);
        } else {
          LOG.info("Not using LLAP IO because there's no partition spec for SerDe-based IF");
        }
        return inputFormat;
      }
      try {
        serde = part.getDeserializer(conf);
      } catch (Exception e) {
        throw new HiveException("Error creating SerDe for LLAP IO", e);
      }
    }
    if (isSupported && isVectorized) {
      InputFormat<?, ?> wrappedIf = llapIo.getInputFormat(inputFormat, serde);
      // null means we cannot wrap; the cause is logged inside.
      if (wrappedIf != null) {
        return castInputFormat(wrappedIf);
      }
    }
    if (isCacheOnly) {
      injectLlapCaches(inputFormat, llapIo, conf);
    }
    return inputFormat;
  }

  private static boolean checkInputFormatForLlapEncode(Configuration conf, String ifName) {
    String formatList = HiveConf.getVar(conf, ConfVars.LLAP_IO_ENCODE_FORMATS);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Checking " + ifName + " against " + formatList);
    }
    String[] formats = StringUtils.getStrings(formatList);
    if (formats != null) {
      for (String format : formats) {
        // TODO: should we check isAssignableFrom?
        if (ifName.equals(format)) {
          if (LOG.isInfoEnabled()) {
            LOG.info("Using SerDe-based LLAP reader for " + ifName);
          }
          return true;
        }
      }
    }
    return false;
  }

  public static void injectLlapCaches(InputFormat<WritableComparable, Writable> inputFormat,
      LlapIo<VectorizedRowBatch> llapIo, Configuration conf) {
    LOG.info("Injecting LLAP caches into " + inputFormat.getClass().getCanonicalName());
    conf.setInt("parquet.read.allocation.size", 1024*1024*1024); // Disable buffer splitting for now.
    llapIo.initCacheOnlyInputFormat(inputFormat);
  }

  public static boolean canWrapForLlap(Class<? extends InputFormat> clazz, boolean checkVector) {
    return LlapWrappableInputFormatInterface.class.isAssignableFrom(clazz) &&
        (!checkVector || BatchToRowInputFormat.class.isAssignableFrom(clazz));
  }

  public static boolean canInjectCaches(Class<? extends InputFormat> clazz) {
    return LlapCacheOnlyInputFormatInterface.class.isAssignableFrom(clazz);
  }

  @SuppressWarnings("unchecked")
  private static <T, U, V, W> InputFormat<T, U> castInputFormat(InputFormat<V, W> from) {
    // This is ugly in two ways...
    // 1) We assume that LlapWrappableInputFormatInterface has NullWritable as first parameter.
    //    Since we are using Java and not, say, a programming language, there's no way to check.
    // 2) We ignore the fact that 2nd arg is completely incompatible (VRB -> Writable), because
    //    vectorization currently works by magic, getting VRB from IF with non-VRB value param.
    // So we just cast blindly and hope for the best (which is obviously what happens).
    return (InputFormat<T, U>)from;
  }

  /** NOTE: this no longer wraps the IF for LLAP. Call wrapForLlap manually if needed. */
  public static InputFormat<WritableComparable, Writable> getInputFormatFromCache(
    Class inputFormatClass, JobConf job) throws IOException {
    InputFormat<WritableComparable, Writable> instance = inputFormats.get(inputFormatClass);
    if (instance == null) {
      try {
        instance = (InputFormat<WritableComparable, Writable>) ReflectionUtil
            .newInstance(inputFormatClass, job);
        // HBase input formats are not thread safe today. See HIVE-8808.
        String inputFormatName = inputFormatClass.getName().toLowerCase();
        if (!inputFormatName.contains("hbase")) {
          inputFormats.put(inputFormatClass, instance);
        }
      } catch (Exception e) {
        throw new IOException("Cannot create an instance of InputFormat class "
            + inputFormatClass.getName() + " as specified in mapredWork!", e);
      }
    }
    return instance;
  }

  @Override
  public RecordReader getRecordReader(InputSplit split, JobConf job,
      Reporter reporter) throws IOException {
    HiveInputSplit hsplit = (HiveInputSplit) split;
    InputSplit inputSplit = hsplit.getInputSplit();
    String inputFormatClassName = null;
    Class inputFormatClass = null;
    try {
      inputFormatClassName = hsplit.inputFormatClassName();
      inputFormatClass = job.getClassByName(inputFormatClassName);
    } catch (Exception e) {
      throw new IOException("cannot find class " + inputFormatClassName, e);
    }

    if (this.mrwork == null || pathToPartitionInfo == null) {
      init(job);
    }

    boolean nonNative = false;
    PartitionDesc part = HiveFileFormatUtils.getFromPathRecursively(
        pathToPartitionInfo, hsplit.getPath(), null);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Found spec for " + hsplit.getPath() + " " + part + " from " + pathToPartitionInfo);
    }

    try {
      if ((part != null) && (part.getTableDesc() != null)) {
        Utilities.copyTableJobPropertiesToConf(part.getTableDesc(), job);
        nonNative = part.getTableDesc().isNonNative();
      }
    } catch (HiveException e) {
      throw new IOException(e);
    }

    Path splitPath = hsplit.getPath();
    pushProjectionsAndFilters(job, inputFormatClass, splitPath, nonNative);

    InputFormat inputFormat = getInputFormatFromCache(inputFormatClass, job);
    try {
      inputFormat = HiveInputFormat.wrapForLlap(inputFormat, job, part);
    } catch (HiveException e) {
      throw new IOException(e);
    }
    RecordReader innerReader = null;
    try {
      innerReader = inputFormat.getRecordReader(inputSplit, job, reporter);
    } catch (Exception e) {
      innerReader = HiveIOExceptionHandlerUtil
          .handleRecordReaderCreationException(e, job);
    }
    HiveRecordReader<K,V> rr = new HiveRecordReader(innerReader, job);
    rr.initIOContext(hsplit, job, inputFormatClass, innerReader);
    return rr;
  }

  protected void init(JobConf job) {
    if (mrwork == null || pathToPartitionInfo == null) {
      if (HiveConf.getVar(job, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")) {
        mrwork = (MapWork) Utilities.getMergeWork(job);
        if (mrwork == null) {
          mrwork = Utilities.getMapWork(job);
        }
      } else {
        mrwork = Utilities.getMapWork(job);
      }

      // Prune partitions
      if (HiveConf.getVar(job, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("spark")
          && HiveConf.isSparkDPPAny(job)) {
        SparkDynamicPartitionPruner pruner = new SparkDynamicPartitionPruner();
        try {
          pruner.prune(mrwork, job);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      pathToPartitionInfo = mrwork.getPathToPartitionInfo();
    }
  }

  /*
   * AddSplitsForGroup collects separate calls to setInputPaths into one where possible.
   * The reason for this is that this is faster on some InputFormats. E.g.: Orc will start
   * a threadpool to do the work and calling it multiple times unnecessarily will create a lot
   * of unnecessary thread pools.
   */
  private void addSplitsForGroup(List<Path> dirs, TableScanOperator tableScan, JobConf conf,
      InputFormat inputFormat, Class<? extends InputFormat> inputFormatClass, int splits,
      TableDesc table, List<InputSplit> result)
          throws IOException {
    String tableName = table.getTableName();
    ValidWriteIdList validWriteIdList = AcidUtils.getTableValidWriteIdList(
        conf, tableName == null ? null : HiveStringUtils.normalizeIdentifier(tableName));
    ValidWriteIdList validMmWriteIdList = getMmValidWriteIds(conf, table, validWriteIdList);

    try {
      Utilities.copyJobSecretToTableProperties(table);
      Utilities.copyTablePropertiesToConf(table, conf);
      if (tableScan != null) {
        AcidUtils.setAcidOperationalProperties(conf, tableScan.getConf().isTranscationalTable(),
            tableScan.getConf().getAcidOperationalProperties());

        if (tableScan.getConf().isTranscationalTable() && (validWriteIdList == null)) {
          throw new IOException("Acid table: " + table.getTableName()
                  + " is missing from the ValidWriteIdList config: "
                  + conf.get(ValidTxnWriteIdList.VALID_TABLES_WRITEIDS_KEY));
        }
        if (validWriteIdList != null) {
          AcidUtils.setValidWriteIdList(conf, validWriteIdList);
        }
      }
    } catch (HiveException e) {
      throw new IOException(e);
    }

    if (tableScan != null) {
      pushFilters(conf, tableScan, this.mrwork);
    }

    List<Path> dirsWithFileOriginals = new ArrayList<>(), finalDirs = new ArrayList<>();
    processPathsForMmRead(dirs, conf, validMmWriteIdList, finalDirs, dirsWithFileOriginals);
    if (finalDirs.isEmpty() && dirsWithFileOriginals.isEmpty()) {
      // This is for transactional tables.
      if (!conf.getBoolean(Utilities.ENSURE_OPERATORS_EXECUTED, false)) {
        LOG.warn("No valid inputs found in " + dirs);
      } else if (validMmWriteIdList != null) {
        // AcidUtils.getAcidState() is already called to verify there is no input split.
        // Thus for a GroupByOperator summary row, set finalDirs and add a Dummy split here.
        result.add(new HiveInputSplit(new NullRowsInputFormat.DummyInputSplit(
            dirs.get(0).toString()), ZeroRowsInputFormat.class.getName()));
      }
      return; // No valid inputs.
    }

    conf.setInputFormat(inputFormat.getClass());
    int headerCount = 0;
    int footerCount = 0;
    if (table != null) {
      headerCount = Utilities.getHeaderCount(table);
      footerCount = Utilities.getFooterCount(table, conf);
      if (headerCount != 0 || footerCount != 0) {
        // Input file has header or footer, cannot be splitted.
        HiveConf.setLongVar(conf, ConfVars.MAPREDMINSPLITSIZE, Long.MAX_VALUE);
      }
    }

    if (!finalDirs.isEmpty()) {
      FileInputFormat.setInputPaths(conf, finalDirs.toArray(new Path[finalDirs.size()]));
      InputSplit[] iss = null;
      try {
        iss = inputFormat.getSplits(conf, splits);
      } catch (Exception ex) {
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        String tokenDump = "";
        for (Token<?> t : ugi.getCredentials().getAllTokens()) {
          tokenDump += t + ", ";
        }
        LOG.error("Failed; user " + ugi.getShortUserName() + " tokens " + tokenDump);
        throw ex;
      }
      for (InputSplit is : iss) {
        result.add(new HiveInputSplit(is, inputFormatClass.getName()));
      }
    }

    if (!dirsWithFileOriginals.isEmpty()) {
      // We are going to add splits for these directories with recursive = false, so we ignore
      // any subdirectories (deltas or original directories) and only read the original files.
      // The fact that there's a loop calling addSplitsForGroup already implies it's ok to
      // the real input format multiple times... however some split concurrency/etc configs
      // that are applied separately in each call will effectively be ignored for such splits.
      JobConf nonRecConf = createConfForMmOriginalsSplit(conf, dirsWithFileOriginals);
      InputSplit[] iss = inputFormat.getSplits(nonRecConf, splits);
      for (InputSplit is : iss) {
        result.add(new HiveInputSplit(is, inputFormatClass.getName()));
      }
    }

    if (result.isEmpty() && conf.getBoolean(Utilities.ENSURE_OPERATORS_EXECUTED, false)) {
      // If there are no inputs; the Execution engine skips the operator tree.
      // To prevent it from happening; an opaque  ZeroRows input is added here - when needed.
      result.add(new HiveInputSplit(new NullRowsInputFormat.DummyInputSplit(
          finalDirs.get(0).toString()), ZeroRowsInputFormat.class.getName()));
    }
  }

  public static JobConf createConfForMmOriginalsSplit(
      JobConf conf, List<Path> dirsWithFileOriginals) {
    JobConf nonRecConf = new JobConf(conf);
    FileInputFormat.setInputPaths(nonRecConf,
        dirsWithFileOriginals.toArray(new Path[dirsWithFileOriginals.size()]));
    nonRecConf.setBoolean(FileInputFormat.INPUT_DIR_RECURSIVE, false);
    nonRecConf.setBoolean("mapred.input.dir.recursive", false);
    // TODO: change to FileInputFormat.... field after MAPREDUCE-7086.
    nonRecConf.setBoolean("mapreduce.input.fileinputformat.input.dir.nonrecursive.ignore.subdirs", true);
    return nonRecConf;
  }

  protected ValidWriteIdList getMmValidWriteIds(
      JobConf conf, TableDesc table, ValidWriteIdList validWriteIdList) throws IOException {
    if (!AcidUtils.isInsertOnlyTable(table.getProperties())) return null;
    if (validWriteIdList == null) {
      validWriteIdList = AcidUtils.getTableValidWriteIdList( conf, table.getTableName());
      if (validWriteIdList == null) {
        throw new IOException("Insert-Only table: " + table.getTableName()
                + " is missing from the ValidWriteIdList config: "
                + conf.get(ValidTxnWriteIdList.VALID_TABLES_WRITEIDS_KEY));
      }
    }
    return validWriteIdList;
  }

  public static void processPathsForMmRead(List<Path> dirs, Configuration conf,
      ValidWriteIdList validWriteIdList, List<Path> finalPaths,
      List<Path> pathsWithFileOriginals) throws IOException {
    if (validWriteIdList == null) {
      finalPaths.addAll(dirs);
      return;
    }
    boolean allowOriginals = HiveConf.getBoolVar(conf, ConfVars.HIVE_MM_ALLOW_ORIGINALS);
    for (Path dir : dirs) {
      processForWriteIds(
          dir, conf, validWriteIdList, allowOriginals, finalPaths, pathsWithFileOriginals);
    }
  }

  private static void processForWriteIds(Path dir, Configuration conf,
      ValidWriteIdList validWriteIdList, boolean allowOriginals, List<Path> finalPaths,
      List<Path> pathsWithFileOriginals) throws IOException {
    FileSystem fs = dir.getFileSystem(conf);
    Utilities.FILE_OP_LOGGER.trace("Checking {} for inputs", dir);

    // Ignore nullscan-optimized paths.
    if (fs instanceof NullScanFileSystem) {
      finalPaths.add(dir);
      return;
    }

    // We need to iterate to detect original directories, that are supported in MM but not ACID.
    boolean hasOriginalFiles = false, hasAcidDirs = false;
    List<Path> originalDirectories = new ArrayList<>();
    for (FileStatus file : fs.listStatus(dir, AcidUtils.hiddenFileFilter)) {
      Path currDir = file.getPath();
      Utilities.FILE_OP_LOGGER.trace("Checking {} for being an input", currDir);
      if (!file.isDirectory()) {
        hasOriginalFiles = true;
      } else if (AcidUtils.extractWriteId(currDir) == null) {
        if (allowOriginals) {
          originalDirectories.add(currDir); // Add as is; it would become a recursive split.
        } else {
          Utilities.FILE_OP_LOGGER.debug("Ignoring unknown (original?) directory {}", currDir);
        }
      } else {
        hasAcidDirs = true;
      }
    }
    if (hasAcidDirs) {
      AcidUtils.Directory dirInfo = AcidUtils.getAcidState(
          dir, conf, validWriteIdList, Ref.from(false), true, null);

      // Find the base, created for IOW.
      Path base = dirInfo.getBaseDirectory();
      if (base != null) {
        Utilities.FILE_OP_LOGGER.debug("Adding input {}", base);
        finalPaths.add(base);
        // Base means originals no longer matter.
        originalDirectories.clear();
        hasOriginalFiles = false;
      }

      // Find the parsed delta files.
      for (AcidUtils.ParsedDelta delta : dirInfo.getCurrentDirectories()) {
        Utilities.FILE_OP_LOGGER.debug("Adding input {}", delta.getPath());
        finalPaths.add(delta.getPath());
      }
    }
    if (!originalDirectories.isEmpty()) {
      Utilities.FILE_OP_LOGGER.debug("Adding original directories {}", originalDirectories);
      finalPaths.addAll(originalDirectories);
    }
    if (hasOriginalFiles) {
      if (allowOriginals) {
        Utilities.FILE_OP_LOGGER.debug("Directory has original files {}", dir);
        pathsWithFileOriginals.add(dir);
      } else {
        Utilities.FILE_OP_LOGGER.debug("Ignoring unknown (original?) files in {}", dir);
      }
    }
  }
 

  Path[] getInputPaths(JobConf job) throws IOException {
    Path[] dirs;
    if (HiveConf.getVar(job, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("spark")) {
      dirs = mrwork.getPathToPartitionInfo().keySet().toArray(new Path[]{});
    } else {
      dirs = FileInputFormat.getInputPaths(job);
      if (dirs.length == 0) {
        // on tez we're avoiding to duplicate the file info in FileInputFormat.
        if (HiveConf.getVar(job, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")) {
          try {
            List<Path> paths = Utilities.getInputPathsTez(job, mrwork);
            dirs = paths.toArray(new Path[paths.size()]);
          } catch (Exception e) {
            throw new IOException("Could not create input files", e);
          }
        } else {
          throw new IOException("No input paths specified in job");
        }
      }
    }
    StringInternUtils.internUriStringsInPathArray(dirs);
    return dirs;
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.GET_SPLITS);
    init(job);
    Path[] dirs = getInputPaths(job);
    JobConf newjob = new JobConf(job);
    List<InputSplit> result = new ArrayList<InputSplit>();

    List<Path> currentDirs = new ArrayList<Path>();
    Class<? extends InputFormat> currentInputFormatClass = null;
    TableDesc currentTable = null;
    TableScanOperator currentTableScan = null;

    boolean pushDownProjection = false;
    //Buffers to hold filter pushdown information
    StringBuilder readColumnsBuffer = new StringBuilder(newjob.
      get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, ""));;
    StringBuilder readColumnNamesBuffer = new StringBuilder(newjob.
      get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, ""));
    // for each dir, get the InputFormat, and do getSplits.
    for (Path dir : dirs) {
      PartitionDesc part = getPartitionDescFromPath(pathToPartitionInfo, dir);
      Class<? extends InputFormat> inputFormatClass = part.getInputFileFormatClass();
      TableDesc table = part.getTableDesc();
      TableScanOperator tableScan = null;

      List<String> aliases = mrwork.getPathToAliases().get(dir);

      // Make filter pushdown information available to getSplits.
      if ((aliases != null) && (aliases.size() == 1)) {
        Operator op = mrwork.getAliasToWork().get(aliases.get(0));
        if ((op != null) && (op instanceof TableScanOperator)) {
          tableScan = (TableScanOperator) op;
          //Reset buffers to store filter push down columns
          readColumnsBuffer.setLength(0);
          readColumnNamesBuffer.setLength(0);
          // push down projections.
          ColumnProjectionUtils.appendReadColumns(readColumnsBuffer, readColumnNamesBuffer,
            tableScan.getNeededColumnIDs(), tableScan.getNeededColumns());
          pushDownProjection = true;
          // push down filters
          pushFilters(newjob, tableScan, this.mrwork);
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("aliases: {} pathToAliases: {} dir: {}", aliases, mrwork.getPathToAliases(), dir);
        }
      }

      if (!currentDirs.isEmpty() &&
          inputFormatClass.equals(currentInputFormatClass) &&
          table.equals(currentTable) &&
          tableScan == currentTableScan) {
        currentDirs.add(dir);
        continue;
      }

      if (!currentDirs.isEmpty()) {
        if (LOG.isInfoEnabled()) {
          LOG.info("Generating splits as currentDirs is not empty. currentDirs: {}", currentDirs);
        }

        // set columns to read in conf
        if (pushDownProjection) {
          pushProjection(newjob, readColumnsBuffer, readColumnNamesBuffer);
        }

        addSplitsForGroup(currentDirs, currentTableScan, newjob,
            getInputFormatFromCache(currentInputFormatClass, job),
            currentInputFormatClass, currentDirs.size()*(numSplits / dirs.length),
            currentTable, result);
      }

      currentDirs.clear();
      currentDirs.add(dir);
      currentTableScan = tableScan;
      currentTable = table;
      currentInputFormatClass = inputFormatClass;
    }

    // set columns to read in conf
    if (pushDownProjection) {
      pushProjection(newjob, readColumnsBuffer, readColumnNamesBuffer);
    }

    if (dirs.length != 0) { // TODO: should this be currentDirs?
      if (LOG.isInfoEnabled()) {
        LOG.info("Generating splits for dirs: {}", dirs);
      }
      addSplitsForGroup(currentDirs, currentTableScan, newjob,
          getInputFormatFromCache(currentInputFormatClass, job),
          currentInputFormatClass, currentDirs.size()*(numSplits / dirs.length),
          currentTable, result);
    }

    Utilities.clearWorkMapForConf(job);
    if (LOG.isInfoEnabled()) {
      LOG.info("number of splits " + result.size());
    }
    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.GET_SPLITS);
    return result.toArray(new HiveInputSplit[result.size()]);
  }

  private void pushProjection(final JobConf newjob, final StringBuilder readColumnsBuffer,
      final StringBuilder readColumnNamesBuffer) {
    String readColIds = readColumnsBuffer.toString();
    String readColNames = readColumnNamesBuffer.toString();
    newjob.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    newjob.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, readColIds);
    newjob.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, readColNames);

    if (LOG.isInfoEnabled()) {
      LOG.info("{} = {}", ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, readColIds);
      LOG.info("{} = {}", ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, readColNames);
    }
  }


  protected static PartitionDesc getPartitionDescFromPath(
      Map<Path, PartitionDesc> pathToPartitionInfo, Path dir)
      throws IOException {
    PartitionDesc partDesc = pathToPartitionInfo.get(dir);
    if (partDesc == null) {
      // Note: we could call HiveFileFormatUtils.getPartitionDescFromPathRecursively for MM tables.
      //       The recursive call is usually needed for non-MM tables, because the path management
      //       is not strict and the code does whatever. That should not happen for MM tables.
      //       Keep it like this for now; may need replacement if we find a valid use case.
      partDesc = pathToPartitionInfo.get(Path.getPathWithoutSchemeAndAuthority(dir));
    }
    if (partDesc == null) {
      throw new IOException("cannot find dir = " + dir.toString()
          + " in " + pathToPartitionInfo);
    }

    return partDesc;
  }

  public static void pushFilters(JobConf jobConf, TableScanOperator tableScan,
    final MapWork mrwork) {

    // ensure filters are not set from previous pushFilters
    jobConf.unset(TableScanDesc.FILTER_TEXT_CONF_STR);
    jobConf.unset(TableScanDesc.FILTER_EXPR_CONF_STR);

    Utilities.unsetSchemaEvolution(jobConf);

    TableScanDesc scanDesc = tableScan.getConf();
    if (scanDesc == null) {
      return;
    }

    Utilities.addTableSchemaToConf(jobConf, tableScan);

    // construct column name list and types for reference by filter push down
    Utilities.setColumnNameList(jobConf, tableScan);
    Utilities.setColumnTypeList(jobConf, tableScan);
    // push down filters
    ExprNodeGenericFuncDesc filterExpr = (ExprNodeGenericFuncDesc)scanDesc.getFilterExpr();
    if (filterExpr == null) {
      return;
    }

    // disable filter pushdown for mapreduce(except for storage handlers) when there are more than one table aliases,
    // since we don't clone jobConf per alias
    if (mrwork != null && mrwork.getAliases() != null && mrwork.getAliases().size() > 1
        && jobConf.get(ConfVars.HIVE_EXECUTION_ENGINE.varname).equals("mr")
        && (scanDesc.getTableMetadata() == null
            || !(scanDesc.getTableMetadata().getStorageHandler() instanceof HiveStoragePredicateHandler))) {
      return;
    }

    String serializedFilterObj = scanDesc.getSerializedFilterObject();
    String serializedFilterExpr = scanDesc.getSerializedFilterExpr();
    boolean hasObj = serializedFilterObj != null, hasExpr = serializedFilterExpr != null;
    if (!hasObj) {
      Serializable filterObject = scanDesc.getFilterObject();
      if (filterObject != null) {
        serializedFilterObj = SerializationUtilities.serializeObject(filterObject);
      }
    }
    if (serializedFilterObj != null) {
      jobConf.set(TableScanDesc.FILTER_OBJECT_CONF_STR, serializedFilterObj);
    }
    if (!hasExpr) {
      serializedFilterExpr = SerializationUtilities.serializeExpression(filterExpr);
    }
    String filterText = filterExpr.getExprString();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Pushdown initiated with filterText = " + filterText + ", filterExpr = "
          + filterExpr + ", serializedFilterExpr = " + serializedFilterExpr + " ("
          + (hasExpr ? "desc" : "new") + ")" + (serializedFilterObj == null ? "" :
            (", serializedFilterObj = " + serializedFilterObj + " (" + (hasObj ? "desc" : "new")
                + ")")));
    }
    jobConf.set(TableScanDesc.FILTER_TEXT_CONF_STR, filterText);
    jobConf.set(TableScanDesc.FILTER_EXPR_CONF_STR, serializedFilterExpr);
  }

  protected void pushProjectionsAndFilters(JobConf jobConf, Class inputFormatClass,
      Path splitPath) {
    pushProjectionsAndFilters(jobConf, inputFormatClass, splitPath, false);
  }

  protected void pushProjectionsAndFilters(JobConf jobConf, Class inputFormatClass,
      Path splitPath, boolean nonNative) {
    Path splitPathWithNoSchema = Path.getPathWithoutSchemeAndAuthority(splitPath);
    if (this.mrwork == null) {
      init(job);
    }

    if(this.mrwork.getPathToAliases() == null) {
      return;
    }

    ArrayList<String> aliases = new ArrayList<String>();
    Iterator<Entry<Path, ArrayList<String>>> iterator = this.mrwork
        .getPathToAliases().entrySet().iterator();

    Set<Path> splitParentPaths = null;
    int pathsSize = this.mrwork.getPathToAliases().entrySet().size();
    while (iterator.hasNext()) {
      Entry<Path, ArrayList<String>> entry = iterator.next();
      Path key = entry.getKey();
      boolean match;
      if (nonNative) {
        // For non-native tables, we need to do an exact match to avoid
        // HIVE-1903.  (The table location contains no files, and the string
        // representation of its path does not have a trailing slash.)
        match =
          splitPath.equals(key) || splitPathWithNoSchema.equals(key);
      } else {
        // But for native tables, we need to do a prefix match for
        // subdirectories.  (Unlike non-native tables, prefix mixups don't seem
        // to be a potential problem here since we are always dealing with the
        // path to something deeper than the table location.)
        if (pathsSize > 1) {
          // Comparing paths multiple times creates lots of objects &
          // creates GC pressure for tables having large number of partitions.
          // In such cases, use pre-computed paths for comparison
          if (splitParentPaths == null) {
            splitParentPaths = new HashSet<>();
            FileUtils.populateParentPaths(splitParentPaths, splitPath);
            FileUtils.populateParentPaths(splitParentPaths, splitPathWithNoSchema);
          }
          match = splitParentPaths.contains(key);
        } else {
          match = FileUtils.isPathWithinSubtree(splitPath, key)
              || FileUtils.isPathWithinSubtree(splitPathWithNoSchema, key);
        }
      }
      if (match) {
        ArrayList<String> list = entry.getValue();
        for (String val : list) {
          aliases.add(val);
        }
      }
    }

    for (String alias : aliases) {
      Operator<? extends OperatorDesc> op = this.mrwork.getAliasToWork().get(
        alias);
      if (op instanceof TableScanOperator) {
        TableScanOperator ts = (TableScanOperator) op;
        // push down projections.
        ColumnProjectionUtils.appendReadColumns(
            jobConf, ts.getNeededColumnIDs(), ts.getNeededColumns(), ts.getNeededNestedColumnPaths());
        // push down filters
        pushFilters(jobConf, ts, this.mrwork);

        AcidUtils.setAcidOperationalProperties(job, ts.getConf().isTranscationalTable(),
            ts.getConf().getAcidOperationalProperties());
        AcidUtils.setValidWriteIdList(job, ts.getConf());
      }
    }
  }
}
