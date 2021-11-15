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

import java.io.IOException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hive.common.util.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;

/**
 * An util class for various Hive file format tasks.
 * registerOutputFormatSubstitute(Class, Class) getOutputFormatSubstitute(Class)
 * are added for backward compatibility. They return the newly added
 * HiveOutputFormat for the older ones.
 *
 */
public final class HiveFileFormatUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HiveFileFormatUtils.class);

  public static class FileChecker {
    // we don't have many file formats that implement InputFormatChecker. We won't be holding
    // multiple instances of such classes
    private static final int MAX_CACHE_SIZE = 16;

    // immutable maps
    Map<Class<? extends InputFormat>, Class<? extends InputFormatChecker>> inputFormatCheckerMap;
    Map<Class<? extends InputFormat>, Class<? extends InputFormatChecker>> textInputFormatCheckerMap;
    Map<Class<?>, Class<? extends OutputFormat>> outputFormatSubstituteMap;

    // mutable thread-safe map to store instances
    Cache<Class<? extends InputFormatChecker>, InputFormatChecker> inputFormatCheckerInstanceCache;

    // classloader invokes this static block when its first loaded (lazy initialization).
    // Class loading is thread safe.
    private static class Factory {
      static final FileChecker INSTANCE = new FileChecker();
    }

    public static FileChecker getInstance() {
      return Factory.INSTANCE;
    }

    private FileChecker() {
      // read-only maps (initialized once)
      inputFormatCheckerMap = ImmutableMap
          .<Class<? extends InputFormat>, Class<? extends InputFormatChecker>>builder()
          .put(SequenceFileInputFormat.class, SequenceFileInputFormatChecker.class)
          .put(RCFileInputFormat.class, RCFileInputFormat.class)
          .put(OrcInputFormat.class, OrcInputFormat.class)
          .put(MapredParquetInputFormat.class, MapredParquetInputFormat.class)
          .build();
      textInputFormatCheckerMap = ImmutableMap
          .<Class<? extends InputFormat>, Class<? extends InputFormatChecker>>builder()
          .put(SequenceFileInputFormat.class, SequenceFileInputFormatChecker.class)
          .build();
      outputFormatSubstituteMap = ImmutableMap
          .<Class<?>, Class<? extends OutputFormat>>builder()
          .put(IgnoreKeyTextOutputFormat.class, HiveIgnoreKeyTextOutputFormat.class)
          .put(SequenceFileOutputFormat.class, HiveSequenceFileOutputFormat.class)
          .build();

      // updatable map that holds instances of the class
      inputFormatCheckerInstanceCache = CacheBuilder.newBuilder().maximumSize(MAX_CACHE_SIZE)
          .build();
    }

    public Set<Class<? extends InputFormat>> registeredClasses() {
      return inputFormatCheckerMap.keySet();
    }

    public Set<Class<? extends InputFormat>> registeredTextClasses() {
      return textInputFormatCheckerMap.keySet();
    }

    public Class<? extends OutputFormat> getOutputFormatSubstiture(Class<?> origin) {
      return outputFormatSubstituteMap.get(origin);
    }

    public Class<? extends InputFormatChecker> getInputFormatCheckerClass(Class<?> inputFormat) {
      return inputFormatCheckerMap.get(inputFormat);
    }

    public void putInputFormatCheckerInstance(
        Class<? extends InputFormatChecker> checkerCls, InputFormatChecker instanceCls) {
      inputFormatCheckerInstanceCache.put(checkerCls, instanceCls);
    }

    public InputFormatChecker getInputFormatCheckerInstance(
        Class<? extends InputFormatChecker> checkerCls) {
      return inputFormatCheckerInstanceCache.getIfPresent(checkerCls);
    }
  }

  /**
   * get a OutputFormat's substitute HiveOutputFormat.
   */
  @SuppressWarnings("unchecked")
  public static Class<? extends OutputFormat> getOutputFormatSubstitute(
      Class<?> origin) {
    if (origin == null || HiveOutputFormat.class.isAssignableFrom(origin)) {
      return (Class<? extends OutputFormat>) origin;  // hive native
    }
    Class<? extends OutputFormat> substitute = FileChecker.getInstance()
        .getOutputFormatSubstiture(origin);
    if (substitute != null) {
      return substitute;  // substituted
    }
    return (Class<? extends OutputFormat>) origin;
  }

  /**
   * Checks if files are in same format as the given input format.
   *
   * Note: an empty set of files is considered compliant.
   */
  @SuppressWarnings("unchecked")
  public static boolean checkInputFormat(FileSystem fs, HiveConf conf,
      Class<? extends InputFormat> inputFormatCls, List<FileStatus> files)
      throws HiveException {
    if (files.isEmpty()) {
      return true;
    }
    Class<? extends InputFormatChecker> checkerCls = FileChecker.getInstance()
        .getInputFormatCheckerClass(inputFormatCls);
    if (checkerCls == null
        && inputFormatCls.isAssignableFrom(TextInputFormat.class)) {
      // we get a text input format here, we can not determine a file is text
      // according to its content, so we can do is to test if other file
      // format can accept it. If one other file format can accept this file,
      // we treat this file as text file, although it maybe not.
      return checkTextInputFormat(fs, conf, files);
    }

    if (checkerCls != null) {
      InputFormatChecker checkerInstance = FileChecker.getInstance()
          .getInputFormatCheckerInstance(checkerCls);
      try {
        if (checkerInstance == null) {
          checkerInstance = checkerCls.newInstance();
          FileChecker.getInstance().putInputFormatCheckerInstance(checkerCls, checkerInstance);
        }
        return checkerInstance.validateInput(fs, conf, files);
      } catch (Exception e) {
        throw new HiveException(e);
      }
    }
    return true;
  }

  @SuppressWarnings("unchecked")
  private static boolean checkTextInputFormat(FileSystem fs, HiveConf conf,
      List<FileStatus> files) throws HiveException {
    List<FileStatus> files2 = new LinkedList<>(files);
    Iterator<FileStatus> iter = files2.iterator();
    while (iter.hasNext()) {
      FileStatus file = iter.next();
      if (file == null) continue;
      if (isPipe(fs, file)) {
        LOG.info("Skipping format check for " + file.getPath() + " as it is a pipe");
        iter.remove();
      }
    }
    if (files2.isEmpty()) return true;
    Set<Class<? extends InputFormat>> inputFormatter = FileChecker.getInstance().registeredTextClasses();
    for (Class<? extends InputFormat> reg : inputFormatter) {
      boolean result = checkInputFormat(fs, conf, reg, files2);
      if (result) {
        return false;
      }
    }
    return true;
  }

  // See include/uapi/linux/stat.h
  private static final int S_IFIFO = 0010000;
  private static boolean isPipe(FileSystem fs, FileStatus file) {
    if (fs instanceof DistributedFileSystem) {
      return false; // Shortcut for HDFS.
    }
    int mode = 0;
    Object pathToLog = file.getPath();
    try {
      java.nio.file.Path realPath = Paths.get(file.getPath().toUri());
      pathToLog = realPath;
      mode = (Integer)Files.getAttribute(realPath, "unix:mode");
    } catch (FileSystemNotFoundException t) {
      return false; // Probably not a local filesystem; no need to check.
    } catch (UnsupportedOperationException | IOException
        | SecurityException | IllegalArgumentException t) {
      LOG.info("Failed to check mode for " + pathToLog + ": "
        + t.getMessage() + " (" + t.getClass() + ")");
      return false;
    }
    return (mode & S_IFIFO) != 0;
  }

  public static RecordWriter getHiveRecordWriter(JobConf jc,
      TableDesc tableInfo, Class<? extends Writable> outputClass,
      FileSinkDesc conf, Path outPath, Reporter reporter) throws HiveException {
    HiveOutputFormat<?, ?> hiveOutputFormat = getHiveOutputFormat(jc, tableInfo);
    try {
      boolean isCompressed = conf.getCompressed();
      JobConf jc_output = jc;
      if (isCompressed) {
        jc_output = new JobConf(jc);
        String codecStr = conf.getCompressCodec();
        if (codecStr != null && !codecStr.trim().equals("")) {
          Class<? extends CompressionCodec> codec = 
              JavaUtils.loadClass(codecStr);
          FileOutputFormat.setOutputCompressorClass(jc_output, codec);
        }
        String type = conf.getCompressType();
        if (type != null && !type.trim().equals("")) {
          CompressionType style = CompressionType.valueOf(type);
          SequenceFileOutputFormat.setOutputCompressionType(jc, style);
        }
      }
      return hiveOutputFormat.getHiveRecordWriter(jc_output, outPath, outputClass, isCompressed,
          tableInfo.getProperties(), reporter);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public static HiveOutputFormat<?, ?> getHiveOutputFormat(Configuration conf, TableDesc tableDesc)
      throws HiveException {
    return getHiveOutputFormat(conf, tableDesc.getOutputFileFormatClass());
  }

  public static HiveOutputFormat<?, ?> getHiveOutputFormat(Configuration conf, PartitionDesc partDesc)
      throws HiveException {
    return getHiveOutputFormat(conf, partDesc.getOutputFileFormatClass());
  }

  private static HiveOutputFormat<?, ?> getHiveOutputFormat(
      Configuration conf, Class<? extends OutputFormat> outputClass) throws HiveException {
    OutputFormat<?, ?> outputFormat = ReflectionUtil.newInstance(outputClass, conf);
    if (!(outputFormat instanceof HiveOutputFormat)) {
      outputFormat = new HivePassThroughOutputFormat(outputFormat);
    }
    return (HiveOutputFormat<?, ?>) outputFormat;
  }

  public static RecordUpdater getAcidRecordUpdater(JobConf jc, TableDesc tableInfo, int bucket, FileSinkDesc conf,
      Path outPath, ObjectInspector inspector, Reporter reporter, int rowIdColNum) throws HiveException, IOException {
    return getAcidRecordUpdater(jc, tableInfo, bucket, conf, outPath, inspector, reporter, rowIdColNum, null);
  }

  public static RecordUpdater getAcidRecordUpdater(JobConf jc, TableDesc tableInfo, int bucket,
                                                   FileSinkDesc conf, Path outPath,
                                                   ObjectInspector inspector,
                                                   Reporter reporter, int rowIdColNum, Integer attemptId)
      throws HiveException, IOException {
    HiveOutputFormat<?, ?> hiveOutputFormat = getHiveOutputFormat(jc, tableInfo);
    AcidOutputFormat<?, ?> acidOutputFormat = null;
    if (hiveOutputFormat instanceof AcidOutputFormat) {
      acidOutputFormat = (AcidOutputFormat)hiveOutputFormat;
    } else {
      throw new HiveException("Unable to create RecordUpdater for HiveOutputFormat that does not " +
          "implement AcidOutputFormat");
    }
    // TODO not 100% sure about this.  This call doesn't set the compression type in the conf
    // file the way getHiveRecordWriter does, as ORC appears to read the value for itself.  Not
    // sure if this is correct or not.
    return getRecordUpdater(jc, acidOutputFormat,
        bucket, inspector, tableInfo.getProperties(), outPath, reporter, rowIdColNum, conf, attemptId);
  }

  private static RecordUpdater getRecordUpdater(JobConf jc,
                                                AcidOutputFormat<?, ?> acidOutputFormat,
                                                int bucket,
                                                ObjectInspector inspector,
                                                Properties tableProp,
                                                Path outPath,
                                                Reporter reporter,
                                                int rowIdColNum,
                                                FileSinkDesc conf,
                                                Integer attemptId) throws IOException {
    return acidOutputFormat.getRecordUpdater(outPath, new AcidOutputFormat.Options(jc)
        .isCompressed(conf.getCompressed())
        .tableProperties(tableProp)
        .reporter(reporter)
        .writingBase(conf.getInsertOverwrite())
        .minimumWriteId(conf.getTableWriteId())
        .maximumWriteId(conf.getTableWriteId())
        .bucket(bucket)
        .inspector(inspector)
        .recordIdColumn(rowIdColNum)
        .statementId(conf.getStatementId())
        .maxStmtId(conf.getMaxStmtId())
        .finalDestination(conf.getDestPath())
        .attemptId(attemptId)
        .temporary(conf.isTemporary()));
  }

  public static <T> T getFromPathRecursively(Map<Path, T> pathToPartitionInfo, Path dir,
      Map<Map<Path, T>, Map<Path, T>> cacheMap) throws IOException {
    return getFromPathRecursively(pathToPartitionInfo, dir, cacheMap, false);
  }

  public static <T> T getFromPathRecursively(Map<Path, T> pathToPartitionInfo, Path dir,
      Map<Map<Path, T>, Map<Path, T>> cacheMap, boolean ignoreSchema) throws IOException {
    return getFromPathRecursively(pathToPartitionInfo, dir, cacheMap, ignoreSchema, false);
  }

  public static <T> T getFromPathRecursively(Map<Path, T> pathToPartitionInfo, Path dir,
      Map<Map<Path, T>, Map<Path, T>> cacheMap, boolean ignoreSchema, boolean ifPresent)
          throws IOException {
    T part = getFromPath(pathToPartitionInfo, dir);

    if (part == null
        && (ignoreSchema
            || (dir.toUri().getScheme() == null || dir.toUri().getScheme().trim().equals(""))
            || FileUtils.pathsContainNoScheme(pathToPartitionInfo.keySet()))) {

      Map<Path, T> newPathToPartitionInfo = null;
      if (cacheMap != null) {
        newPathToPartitionInfo = cacheMap.get(pathToPartitionInfo);
      }

      if (newPathToPartitionInfo == null) { // still null
        newPathToPartitionInfo = populateNewT(pathToPartitionInfo);

        if (cacheMap != null) {
          cacheMap.put(pathToPartitionInfo, newPathToPartitionInfo);
        }
      }
      part = getFromPath(newPathToPartitionInfo, dir);
    }
    if (part != null || ifPresent) {
      return part;
    } else {
      throw new IOException("cannot find dir = " + dir.toString()
                          + " in pathToPartitionInfo: " + pathToPartitionInfo.keySet());
    }
  }

  private static <T> Map<Path, T> populateNewT(Map<Path, T> pathToPartitionInfo) {
    Map<Path, T> newPathToPartitionInfo = new HashMap<>();
    for (Map.Entry<Path, T> entry: pathToPartitionInfo.entrySet()) {
      T partDesc = entry.getValue();
      Path pathOnly = Path.getPathWithoutSchemeAndAuthority(entry.getKey());
      newPathToPartitionInfo.put(pathOnly, partDesc);
    }
    return newPathToPartitionInfo;
  }

  private static <T> T getFromPath(
      Map<Path, T> pathToPartitionInfo, Path dir) {
    
    // We first do exact match, and then do prefix matching. The latter is due to input dir
    // could be /dir/ds='2001-02-21'/part-03 where part-03 is not part of partition
    Path path = FileUtils.getParentRegardlessOfScheme(dir,pathToPartitionInfo.keySet());
    
    if(path == null) {
      // FIXME: old implementation returned null; exception maybe?
      return null;
    }
    return pathToPartitionInfo.get(path);
  }

  private static boolean foundAlias(Map<Path, List<String>> pathToAliases, Path path) {
    List<String> aliases = pathToAliases.get(path);
    if ((aliases == null) || (aliases.isEmpty())) {
      return false;
    }
    return true;
  }

  private static Path getMatchingPath(Map<Path, List<String>> pathToAliases, Path dir) {
    // First find the path to be searched
    Path path = dir;
    if (foundAlias(pathToAliases, path)) {
      return path;
    }

    Path dirPath = Path.getPathWithoutSchemeAndAuthority(dir);
    if (foundAlias(pathToAliases, dirPath)) {
      return dirPath;
    }

    while (path!=null && dirPath!=null) {
      path=path.getParent();
      dirPath=dirPath.getParent();
      //first try full match
      if (foundAlias(pathToAliases, path)) {
        return path;
      }
      if (foundAlias(pathToAliases, dirPath)) {
        return dirPath;
      }
    }
    return null;
  }

  /**
   * Get the list of operators from the operator tree that are needed for the path
   * @param pathToAliases  mapping from path to aliases
   * @param aliasToWork    The operator tree to be invoked for a given alias
   * @param dir            The path to look for
   **/
  public static List<Operator<? extends OperatorDesc>> doGetWorksFromPath(Map<Path, List<String>> pathToAliases,
      Map<String, Operator<? extends OperatorDesc>> aliasToWork, Path dir) {
    List<Operator<? extends OperatorDesc>> opList = new ArrayList<Operator<? extends OperatorDesc>>();

    List<String> aliases = doGetAliasesFromPath(pathToAliases, dir);
    for (String alias : aliases) {
      opList.add(aliasToWork.get(alias));
    }
    return opList;
  }

  /**
   * Get the list of aliases from the opeerator tree that are needed for the path
   * @param pathToAliases  mapping from path to aliases
   * @param dir            The path to look for
   **/
  public static List<String> doGetAliasesFromPath(Map<Path, List<String>> pathToAliases, Path dir) {
    if (pathToAliases == null) {
      return Collections.emptyList();
    }
    Path path = getMatchingPath(pathToAliases, dir);
    return pathToAliases.get(path);
  }

  private HiveFileFormatUtils() {
    // prevent instantiation
  }

  public static class NullOutputCommitter extends OutputCommitter {
    @Override
    public void setupJob(JobContext jobContext) { }
    @Override
    public void cleanupJob(JobContext jobContext) { }

    @Override
    public void setupTask(TaskAttemptContext taskContext) { }
    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext) {
      return false;
    }
    @Override
    public void commitTask(TaskAttemptContext taskContext) { }
    @Override
    public void abortTask(TaskAttemptContext taskContext) { }
  }

  /**
   * Hive uses side effect files exclusively for it's output. It also manages
   * the setup/cleanup/commit of output from the hive client. As a result it does
   * not need support for the same inside the MR framework
   *
   * This routine sets the appropriate options related to bypass setup/cleanup/commit
   * support in the MR framework, but does not set the OutputFormat class.
   */
  public static void prepareJobOutput(JobConf conf) {
    conf.setOutputCommitter(NullOutputCommitter.class);

    // option to bypass job setup and cleanup was introduced in hadoop-21 (MAPREDUCE-463)
    // but can be backported. So we disable setup/cleanup in all versions >= 0.19
    conf.setBoolean(MRJobConfig.SETUP_CLEANUP_NEEDED, false);

    // option to bypass task cleanup task was introduced in hadoop-23 (MAPREDUCE-2206)
    // but can be backported. So we disable setup/cleanup in all versions >= 0.19
    conf.setBoolean(MRJobConfig.TASK_CLEANUP_NEEDED, false);
  }
}
