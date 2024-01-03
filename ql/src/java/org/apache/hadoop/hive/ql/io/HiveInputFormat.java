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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.StringInternUtils;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.common.type.TimestampTZUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.io.HiveIOExceptionHandlerUtil;
import org.apache.hadoop.hive.llap.io.api.LlapIo;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.tez.HashableInputSplit;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.NullRowsInputFormat.NullRowsRecordReader;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.hive.common.util.Ref;
import org.apache.hive.common.util.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.ClosedByInterruptException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.lang.Integer.min;

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
      Configurable, HashableInputSplit {


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

    @Override
    public byte[] getBytesForHash() {
      if (inputSplit instanceof HashableInputSplit) {
        return ((HashableInputSplit)inputSplit).getBytesForHash();
      } else {
        // Explicitly using only the start offset of a split, and not the length. Splits generated on
        // block boundaries and stripe boundaries can vary slightly. Try hashing both to the same node.
        // There is the drawback of potentially hashing the same data on multiple nodes though, when a
        // large split is sent to 1 node, and a second invocation uses smaller chunks of the previous
        // large split and send them to different nodes.
        byte[] pathBytes = getPath().toString().getBytes();
        byte[] allBytes = new byte[pathBytes.length + 8];
        System.arraycopy(pathBytes, 0, allBytes, 0, pathBytes.length);
        SerDeUtils.writeLong(allBytes, pathBytes.length, getStart() >> 3);
        return allBytes;
      }
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
      LOG.info("Not using llap for " + ifName + ": supported = " + isSupported + ", vectorized = " + isVectorized
          + ", cache only = " + isCacheOnly);
      return inputFormat;
    }
    LOG.debug("Processing {}", ifName);

    @SuppressWarnings("unchecked")
    LlapIo<VectorizedRowBatch> llapIo = LlapProxy.getIo();
    if (llapIo == null) {
      LOG.info("Not using LLAP IO because it is not initialized");
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

  public static boolean checkInputFormatForLlapEncode(Configuration conf, String ifName) {
    String formatList = HiveConf.getVar(conf, ConfVars.LLAP_IO_ENCODE_FORMATS);
    LOG.debug("Checking {} against {}", ifName, formatList);
    String[] formats = StringUtils.getStrings(formatList);
    if (formats != null) {
      for (String format : formats) {
        // TODO: should we check isAssignableFrom?
        if (ifName.equals(format)) {
          LOG.info("Using SerDe-based LLAP reader for " + ifName);
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

  public static boolean canInjectCaches(Class<? extends InputFormat> clazz, boolean isVectorized) {
    if (LlapCacheOnlyInputFormatInterface.VectorizedOnly.class.isAssignableFrom(clazz)) {
      return isVectorized;
    }
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
    pushProjectionsAndFiltersAndAsOf(job, splitPath);

    InputFormat inputFormat = getInputFormatFromCache(inputFormatClass, job);
    if (HiveConf.getBoolVar(job, ConfVars.LLAP_IO_ENABLED, LlapProxy.isDaemon())) {
      try {
        inputFormat = HiveInputFormat.wrapForLlap(inputFormat, job, part);
      } catch (HiveException e) {
        throw new IOException(e);
      }
    }
    RecordReader innerReader = null;
    try {
      // Handle the special header/footer skipping cases here.
      innerReader = RecordReaderWrapper.create(inputFormat, hsplit, part.getTableDesc(), job, reporter);
    } catch (Exception e) {
      Throwable rootCause = JavaUtils.findRootCause(e);
      if (checkLimitReached(job)
          && (rootCause instanceof InterruptedException || rootCause instanceof ClosedByInterruptException)) {
        LOG.info("Ignoring exception while getting record reader as limit is reached", rootCause);
        innerReader = new NullRowsRecordReader(job, split);
      } else {
        innerReader = HiveIOExceptionHandlerUtil
            .handleRecordReaderCreationException(e, job);
      }
    }
    HiveRecordReader<K,V> rr = new HiveRecordReader(innerReader, job);
    rr.initIOContext(hsplit, job, inputFormatClass, innerReader);
    return rr;
  }

  private boolean checkLimitReached(JobConf job) {
    /*
     * Assuming that "tez.mapreduce.vertex.name" is present in case of tez.
     * If it's not present (e.g. different execution engine), then checkLimitReachedForVertex will return
     * false due to an invalid cache key (like: "null_limit_reached"), so this will silently acts as
     * limit hasn't been reached, which is a proper behavior in case we don't support early bailout.
     */
    return LimitOperator.checkLimitReachedForVertex(job, job.get("tez.mapreduce.vertex.name"));
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
      pushFiltersAndAsOf(conf, tableScan, this.mrwork);
    }

    List<Path> dirsWithFileOriginals = Collections.synchronizedList(new ArrayList<>()),
            finalDirs = Collections.synchronizedList(new ArrayList<>());
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
    if (table != null) {
      int headerCount = Utilities.getHeaderCount(table);
      int footerCount = Utilities.getFooterCount(table, conf);
      if (headerCount != 0 || footerCount != 0) {
        if (TextInputFormat.class.isAssignableFrom(inputFormatClass) && isUncompressedInput(finalDirs, conf)) {
          SkippingTextInputFormat skippingTextInputFormat = new SkippingTextInputFormat();
          skippingTextInputFormat.configure(conf, headerCount, footerCount);
          inputFormat = skippingTextInputFormat;
        } else {
          // if the input is Compressed OR not text we have no way of splitting them!
          // In that case RecordReader should take care of header/footer skipping!
          HiveConf.setLongVar(conf, ConfVars.MAPRED_MIN_SPLIT_SIZE, Long.MAX_VALUE);
        }
      }
    }

    if (!finalDirs.isEmpty()) {
      FileInputFormat.setInputPaths(conf, finalDirs.toArray(new Path[finalDirs.size()]));
      InputSplit[] iss = inputFormat.getSplits(conf, splits);
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
    if (!AcidUtils.isInsertOnlyTable(table.getProperties())) {
      return null;
    }
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

  public boolean isUncompressedInput(List<Path> finalPaths, Configuration conf) throws IOException {
    CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
    for (Path curr : finalPaths) {
      FileSystem fs = curr.getFileSystem(conf);
      if (fs.isDirectory(curr)) {
        List<FileStatus> results = new ArrayList<>();
        FileUtils.listStatusRecursively(fs, fs.getFileStatus(curr), results);
        for (FileStatus fileStatus : results) {
          if (compressionCodecs.getCodec(fileStatus.getPath()) != null) {
            return false;
          }
        }
      } else if (compressionCodecs.getCodec(curr) != null) {
        return false;
      }
    }
    return true;
  }

  public static void processPathsForMmRead(List<Path> dirs, Configuration conf,
      ValidWriteIdList validWriteIdList, List<Path> finalPaths,
      List<Path> pathsWithFileOriginals) throws IOException {
    if (validWriteIdList == null) {
      finalPaths.addAll(dirs);
      return;
    }
    boolean allowOriginals = HiveConf.getBoolVar(conf, ConfVars.HIVE_MM_ALLOW_ORIGINALS);

    int numThreads = min(HiveConf.getIntVar(conf, ConfVars.HIVE_COMPUTE_SPLITS_NUM_THREADS), dirs.size());
    List<Future<Void>> pathFutures = new ArrayList<>();
    ExecutorService pool = null;
    if (numThreads > 1) {
      pool = Executors.newFixedThreadPool(numThreads,
              new ThreadFactoryBuilder().setDaemon(true).setNameFormat("MM-Split-Paths-%d").build());
    }

    try {
      for (Path dir : dirs) {
        if (pool != null) {
          Future<Void> pathFuture = pool.submit(() -> {
            processForWriteIdsForMmRead(dir, conf, validWriteIdList, allowOriginals, finalPaths, pathsWithFileOriginals);
            return null;
          });
          pathFutures.add(pathFuture);
        } else {
          processForWriteIdsForMmRead(dir, conf, validWriteIdList, allowOriginals, finalPaths, pathsWithFileOriginals);
        }
      }
      try {
        for (Future<Void> pathFuture : pathFutures) {
          pathFuture.get();
        }
      } catch (InterruptedException | ExecutionException e) {
        for (Future<Void> future : pathFutures) {
          future.cancel(true);
        }
        throw new IOException(e);
      }
    } finally {
      if (pool != null) {
        pool.shutdown();
      }
    }
  }

  private static void processForWriteIdsForMmRead(Path dir, Configuration conf,
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
    for (FileStatus file : fs.listStatus(dir, FileUtils.HIDDEN_FILES_PATH_FILTER)) {
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
      AcidDirectory dirInfo = AcidUtils.getAcidState(
          fs, dir, conf, validWriteIdList, Ref.from(false), true);

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
    StringInternUtils.internUriStringsInPathArray(dirs);
    return dirs;
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.GET_SPLITS);
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
    boolean fetchVirtualColumns = newjob.getBoolean(ColumnProjectionUtils.FETCH_VIRTUAL_COLUMNS_CONF_STR, false);
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
          ColumnProjectionUtils.appendReadColumns(
                  readColumnsBuffer, readColumnNamesBuffer,
            tableScan.getNeededColumnIDs(), tableScan.getNeededColumns());
          fetchVirtualColumns = tableScan.getConf().hasVirtualCols();
          pushDownProjection = true;
          // push down filters and as of information
          pushFiltersAndAsOf(newjob, tableScan, this.mrwork);
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
        LOG.info("Generating splits as currentDirs is not empty. currentDirs: {}", currentDirs);

        // set columns to read in conf
        if (pushDownProjection) {
          pushProjection(newjob, readColumnsBuffer, readColumnNamesBuffer, fetchVirtualColumns);
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
      pushProjection(newjob, readColumnsBuffer, readColumnNamesBuffer, fetchVirtualColumns);
    }

    if (dirs.length != 0) { // TODO: should this be currentDirs?
      LOG.info("Generating splits for dirs: {}", dirs);
      addSplitsForGroup(currentDirs, currentTableScan, newjob,
          getInputFormatFromCache(currentInputFormatClass, job),
          currentInputFormatClass, currentDirs.size()*(numSplits / dirs.length),
          currentTable, result);
    }

    Utilities.clearWorkMapForConf(job);
    LOG.info("number of splits " + result.size());
    perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.GET_SPLITS);
    return result.toArray(new HiveInputSplit[result.size()]);
  }

  private void pushProjection(final JobConf newjob, final StringBuilder readColumnsBuffer,
      final StringBuilder readColumnNamesBuffer, final boolean fetchVirtualColumns) {
    String readColIds = readColumnsBuffer.toString();
    String readColNames = readColumnNamesBuffer.toString();
    newjob.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
    newjob.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, readColIds);
    newjob.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, readColNames);
    newjob.setBoolean(ColumnProjectionUtils.FETCH_VIRTUAL_COLUMNS_CONF_STR, fetchVirtualColumns);

    LOG.info("{} = {}", ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, readColIds);
    LOG.info("{} = {}", ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, readColNames);
    LOG.info("{} = {}", ColumnProjectionUtils.FETCH_VIRTUAL_COLUMNS_CONF_STR, fetchVirtualColumns);
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

  public static void pushFiltersAndAsOf(JobConf jobConf, TableScanOperator tableScan,
    final MapWork mrwork) {
    // Push as of information
    pushAsOf(jobConf, tableScan);

    // ensure filters are not set from previous pushFilters
    jobConf.unset(TableScanDesc.FILTER_TEXT_CONF_STR);
    jobConf.unset(TableScanDesc.FILTER_EXPR_CONF_STR);

    Utilities.unsetSchemaEvolution(jobConf);

    TableScanDesc scanDesc = tableScan.getConf();
    if (scanDesc == null) {
      return;
    }

    Utilities.addTableSchemaToConf(jobConf, tableScan);
    Utilities.setPartitionColumnNames(jobConf, tableScan);

    // construct column name list and types for reference by filter push down
    Utilities.setColumnNameList(jobConf, tableScan);
    Utilities.setColumnTypeList(jobConf, tableScan);
    // push down filters
    ExprNodeGenericFuncDesc filterExpr = scanDesc.getFilterExpr();
    String pruningFilter = jobConf.get(TableScanDesc.PARTITION_PRUNING_FILTER);
    // If we have a pruning filter then combine it with the original
    if (pruningFilter != null) {
      ExprNodeGenericFuncDesc pruningExpr = SerializationUtilities.deserializeExpression(pruningFilter);
      if (filterExpr != null) {
        // Combine the 2 filters with AND
        filterExpr = ExprNodeDescUtils.and(filterExpr, pruningExpr);
      } else {
        // Use the pruning filter if there was no filter before
        filterExpr = pruningExpr;
      }

      // Set the combined filter in the TableScanDesc and remove the pruning filter
      scanDesc.setFilterExpr(filterExpr);
      scanDesc.setSerializedFilterExpr(SerializationUtilities.serializeExpression(filterExpr));
      jobConf.unset(TableScanDesc.PARTITION_PRUNING_FILTER);
    }
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

  protected static void pushAsOf(Configuration jobConf, TableScanOperator ts) {
    TableScanDesc scanDesc = ts.getConf();
    if (scanDesc.getAsOfTimestamp() != null) {
      ZoneId timeZone = SessionState.get() == null ? new HiveConf().getLocalTimeZone() :
          SessionState.get().getConf().getLocalTimeZone();
      TimestampTZ time = TimestampTZUtil.parse(scanDesc.getAsOfTimestamp(), timeZone);
      jobConf.set(TableScanDesc.AS_OF_TIMESTAMP, Long.toString(time.toEpochMilli()));
    }

    if (scanDesc.getAsOfVersion() != null) {
      jobConf.set(TableScanDesc.AS_OF_VERSION, scanDesc.getAsOfVersion());
    }

    if (scanDesc.getVersionIntervalFrom() != null) {
      jobConf.set(TableScanDesc.FROM_VERSION, scanDesc.getVersionIntervalFrom());
    }

    if (scanDesc.getSnapshotRef() != null) {
      jobConf.set(TableScanDesc.SNAPSHOT_REF, scanDesc.getSnapshotRef());
    }
  }

  protected void pushProjectionsAndFiltersAndAsOf(JobConf jobConf, Path splitPath) {
    Path splitPathWithNoSchema = Path.getPathWithoutSchemeAndAuthority(splitPath);
    if (this.mrwork == null) {
      init(job);
    }

    if(this.mrwork.getPathToAliases() == null) {
      return;
    }

    ArrayList<String> aliases = new ArrayList<String>();
    Iterator<Entry<Path, List<String>>> iterator = this.mrwork.getPathToAliases().entrySet().iterator();

    Set<Path> splitParentPaths = null;
    int pathsSize = this.mrwork.getPathToAliases().entrySet().size();
    while (iterator.hasNext()) {
      Entry<Path, List<String>> entry = iterator.next();
      Path key = entry.getKey();
      // Note for HIVE-1903: for non-native tables we might only see a table location provided as path in splitPath.
      // In this case the code part below should still work, as the "key" will be an exact match for splitPath.
      // Also: we should not anticipate table paths to be under other tables' locations.
      boolean match;
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
      if (match) {
        List<String> list = entry.getValue();
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
                jobConf,
                ts.getNeededColumnIDs(),
                ts.getNeededColumns(),
                ts.getNeededNestedColumnPaths(),
                ts.getConf().hasVirtualCols());
        // push down filters and as of information
        pushFiltersAndAsOf(jobConf, ts, this.mrwork);

        AcidUtils.setAcidOperationalProperties(job, ts.getConf().isTranscationalTable(),
            ts.getConf().getAcidOperationalProperties());
        AcidUtils.setValidWriteIdList(job, ts.getConf());
      }
    }
  }
}
