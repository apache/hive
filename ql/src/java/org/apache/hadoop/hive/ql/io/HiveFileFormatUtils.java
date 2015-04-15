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

package org.apache.hadoop.hive.ql.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.shims.ShimLoader;
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
import org.apache.hadoop.util.Shell;
import org.apache.hive.common.util.ReflectionUtil;

/**
 * An util class for various Hive file format tasks.
 * registerOutputFormatSubstitute(Class, Class) getOutputFormatSubstitute(Class)
 * are added for backward compatibility. They return the newly added
 * HiveOutputFormat for the older ones.
 *
 */
public final class HiveFileFormatUtils {

  static {
    outputFormatSubstituteMap =
        new ConcurrentHashMap<Class<?>, Class<? extends OutputFormat>>();
    HiveFileFormatUtils.registerOutputFormatSubstitute(
        IgnoreKeyTextOutputFormat.class, HiveIgnoreKeyTextOutputFormat.class);
    HiveFileFormatUtils.registerOutputFormatSubstitute(
        SequenceFileOutputFormat.class, HiveSequenceFileOutputFormat.class);
  }

  @SuppressWarnings("unchecked")
  private static Map<Class<?>, Class<? extends OutputFormat>>
    outputFormatSubstituteMap;

  /**
   * register a substitute.
   *
   * @param origin
   *          the class that need to be substituted
   * @param substitute
   */
  @SuppressWarnings("unchecked")
  public static void registerOutputFormatSubstitute(Class<?> origin,
      Class<? extends HiveOutputFormat> substitute) {
    outputFormatSubstituteMap.put(origin, substitute);
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
    Class<? extends OutputFormat> substitute = outputFormatSubstituteMap.get(origin);
    if (substitute != null) {
      return substitute;  // substituted
    }
    return (Class<? extends OutputFormat>) origin;
  }

  /**
   * get the final output path of a given FileOutputFormat.
   *
   * @param parent
   *          parent dir of the expected final output path
   * @param jc
   *          job configuration
   * @deprecated
   */
  @Deprecated
  public static Path getOutputFormatFinalPath(Path parent, String taskId, JobConf jc,
      HiveOutputFormat<?, ?> hiveOutputFormat, boolean isCompressed,
      Path defaultFinalPath) throws IOException {
    if (hiveOutputFormat instanceof HiveIgnoreKeyTextOutputFormat) {
      return new Path(parent, taskId
          + Utilities.getFileExtension(jc, isCompressed));
    }
    return defaultFinalPath;
  }

  static {
    inputFormatCheckerMap =
        new HashMap<Class<? extends InputFormat>, Class<? extends InputFormatChecker>>();
    HiveFileFormatUtils.registerInputFormatChecker(
        SequenceFileInputFormat.class, SequenceFileInputFormatChecker.class);
    HiveFileFormatUtils.registerInputFormatChecker(RCFileInputFormat.class,
        RCFileInputFormat.class);
    inputFormatCheckerInstanceCache =
        new HashMap<Class<? extends InputFormatChecker>, InputFormatChecker>();
  }

  @SuppressWarnings("unchecked")
  private static Map<Class<? extends InputFormat>, Class<? extends InputFormatChecker>> inputFormatCheckerMap;

  private static Map<Class<? extends InputFormatChecker>, InputFormatChecker> inputFormatCheckerInstanceCache;

  /**
   * register an InputFormatChecker for a given InputFormat.
   *
   * @param format
   *          the class that need to be substituted
   * @param checker
   */
  @SuppressWarnings("unchecked")
  public static synchronized void registerInputFormatChecker(
      Class<? extends InputFormat> format,
      Class<? extends InputFormatChecker> checker) {
    inputFormatCheckerMap.put(format, checker);
  }

  /**
   * get an InputFormatChecker for a file format.
   */
  public static synchronized Class<? extends InputFormatChecker> getInputFormatChecker(
      Class<?> inputFormat) {
    Class<? extends InputFormatChecker> result = inputFormatCheckerMap
        .get(inputFormat);
    return result;
  }

  /**
   * checks if files are in same format as the given input format.
   */
  @SuppressWarnings("unchecked")
  public static boolean checkInputFormat(FileSystem fs, HiveConf conf,
      Class<? extends InputFormat> inputFormatCls, ArrayList<FileStatus> files)
      throws HiveException {
    if (files.size() > 0) {
      Class<? extends InputFormatChecker> checkerCls = getInputFormatChecker(inputFormatCls);
      if (checkerCls == null
          && inputFormatCls.isAssignableFrom(TextInputFormat.class)) {
        // we get a text input format here, we can not determine a file is text
        // according to its content, so we can do is to test if other file
        // format can accept it. If one other file format can accept this file,
        // we treat this file as text file, although it maybe not.
        return checkTextInputFormat(fs, conf, files);
      }

      if (checkerCls != null) {
        InputFormatChecker checkerInstance = inputFormatCheckerInstanceCache
            .get(checkerCls);
        try {
          if (checkerInstance == null) {
            checkerInstance = checkerCls.newInstance();
            inputFormatCheckerInstanceCache.put(checkerCls, checkerInstance);
          }
          return checkerInstance.validateInput(fs, conf, files);
        } catch (Exception e) {
          throw new HiveException(e);
        }
      }
      return true;
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  private static boolean checkTextInputFormat(FileSystem fs, HiveConf conf,
      ArrayList<FileStatus> files) throws HiveException {
    Set<Class<? extends InputFormat>> inputFormatter = inputFormatCheckerMap
        .keySet();
    for (Class<? extends InputFormat> reg : inputFormatter) {
      boolean result = checkInputFormat(fs, conf, reg, files);
      if (result) {
        return false;
      }
    }
    return true;
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
              (Class<? extends CompressionCodec>) JavaUtils.loadClass(codecStr);
          FileOutputFormat.setOutputCompressorClass(jc_output, codec);
        }
        String type = conf.getCompressType();
        if (type != null && !type.trim().equals("")) {
          CompressionType style = CompressionType.valueOf(type);
          SequenceFileOutputFormat.setOutputCompressionType(jc, style);
        }
      }
      return getRecordWriter(jc_output, hiveOutputFormat, outputClass,
          isCompressed, tableInfo.getProperties(), outPath, reporter);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public static RecordWriter getRecordWriter(JobConf jc,
      OutputFormat<?, ?> outputFormat,
      Class<? extends Writable> valueClass, boolean isCompressed,
      Properties tableProp, Path outPath, Reporter reporter
      ) throws IOException, HiveException {
    if (!(outputFormat instanceof HiveOutputFormat)) {
      outputFormat = new HivePassThroughOutputFormat(outputFormat);
    }
    return ((HiveOutputFormat)outputFormat).getHiveRecordWriter(
        jc, outPath, valueClass, isCompressed, tableProp, reporter);
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

  public static RecordUpdater getAcidRecordUpdater(JobConf jc, TableDesc tableInfo, int bucket,
                                                   FileSinkDesc conf, Path outPath,
                                                   ObjectInspector inspector,
                                                   Reporter reporter, int rowIdColNum)
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
    return getRecordUpdater(jc, acidOutputFormat, conf.getCompressed(), conf.getTransactionId(),
        bucket, inspector, tableInfo.getProperties(), outPath, reporter, rowIdColNum);
  }


  private static RecordUpdater getRecordUpdater(JobConf jc,
                                                AcidOutputFormat<?, ?> acidOutputFormat,
                                                boolean isCompressed,
                                                long txnId,
                                                int bucket,
                                                ObjectInspector inspector,
                                                Properties tableProp,
                                                Path outPath,
                                                Reporter reporter,
                                                int rowIdColNum) throws IOException {
    return acidOutputFormat.getRecordUpdater(outPath, new AcidOutputFormat.Options(jc)
        .isCompressed(isCompressed)
        .tableProperties(tableProp)
        .reporter(reporter)
        .writingBase(false)
        .minimumTransactionId(txnId)
        .maximumTransactionId(txnId)
        .bucket(bucket)
        .inspector(inspector)
        .recordIdColumn(rowIdColNum));
  }

  public static PartitionDesc getPartitionDescFromPathRecursively(
      Map<String, PartitionDesc> pathToPartitionInfo, Path dir,
      Map<Map<String, PartitionDesc>, Map<String, PartitionDesc>> cacheMap)
      throws IOException {
    return getPartitionDescFromPathRecursively(pathToPartitionInfo, dir,
        cacheMap, false);
  }

  public static PartitionDesc getPartitionDescFromPathRecursively(
      Map<String, PartitionDesc> pathToPartitionInfo, Path dir,
      Map<Map<String, PartitionDesc>, Map<String, PartitionDesc>> cacheMap,
      boolean ignoreSchema) throws IOException {

    PartitionDesc part = doGetPartitionDescFromPath(pathToPartitionInfo, dir);

    if (part == null
        && (ignoreSchema
            || (dir.toUri().getScheme() == null || dir.toUri().getScheme().trim()
            .equals(""))
            || pathsContainNoScheme(pathToPartitionInfo)
            )

        ) {

      Map<String, PartitionDesc> newPathToPartitionInfo = null;
      if (cacheMap != null) {
        newPathToPartitionInfo = cacheMap.get(pathToPartitionInfo);
      }

      if (newPathToPartitionInfo == null) { // still null
        newPathToPartitionInfo = new HashMap<String, PartitionDesc>();
        populateNewPartitionDesc(pathToPartitionInfo, newPathToPartitionInfo);

        if (cacheMap != null) {
          cacheMap.put(pathToPartitionInfo, newPathToPartitionInfo);
        }
      }
      part = doGetPartitionDescFromPath(newPathToPartitionInfo, dir);
    }
    if (part != null) {
      return part;
    } else {
      throw new IOException("cannot find dir = " + dir.toString()
                          + " in pathToPartitionInfo: " + pathToPartitionInfo.keySet());
    }
  }

  private static boolean pathsContainNoScheme(Map<String, PartitionDesc> pathToPartitionInfo) {

    for( Entry<String, PartitionDesc> pe  : pathToPartitionInfo.entrySet()){
      if(new Path(pe.getKey()).toUri().getScheme() != null){
        return false;
      }
    }
    return true;

  }

  private static void populateNewPartitionDesc(
      Map<String, PartitionDesc> pathToPartitionInfo,
      Map<String, PartitionDesc> newPathToPartitionInfo) {
    for (Map.Entry<String, PartitionDesc> entry: pathToPartitionInfo.entrySet()) {
      String entryKey = entry.getKey();
      PartitionDesc partDesc = entry.getValue();
      Path newP = new Path(entryKey);
      String pathOnly = newP.toUri().getPath();
      newPathToPartitionInfo.put(pathOnly, partDesc);
    }
  }

  private static PartitionDesc doGetPartitionDescFromPath(
      Map<String, PartitionDesc> pathToPartitionInfo, Path dir) {
    // We first do exact match, and then do prefix matching. The latter is due to input dir
    // could be /dir/ds='2001-02-21'/part-03 where part-03 is not part of partition
    String dirPath = dir.toUri().getPath();
    PartitionDesc part = pathToPartitionInfo.get(dir.toString());
    if (part == null) {
      //      LOG.warn("exact match not found, try ripping input path's theme and authority");
      part = pathToPartitionInfo.get(dirPath);
    }

    if (part == null) {
      Path curPath = new Path(dir.toUri().getPath()).getParent();
      dir = dir.getParent();
      while (dir != null) {

        // first try full match
        part = pathToPartitionInfo.get(dir.toString());
        if (part == null) {

          // exact match not found, try ripping input path's scheme and authority
          part = pathToPartitionInfo.get(curPath.toString());
        }
        if (part != null) {
          break;
        }
        dir = dir.getParent();
        curPath = curPath.getParent();
      }
    }
    return part;
  }

  private static boolean foundAlias(Map<String, ArrayList<String>> pathToAliases,
                                    String path) {
    List<String> aliases = pathToAliases.get(path);
    if ((aliases == null) || (aliases.isEmpty())) {
      return false;
    }
    return true;
  }

  private static String getMatchingPath(Map<String, ArrayList<String>> pathToAliases,
                                        Path dir) {
    // First find the path to be searched
    String path = dir.toString();
    if (foundAlias(pathToAliases, path)) {
      return path;
    }

    String dirPath = dir.toUri().getPath();
    if(Shell.WINDOWS){
      //temp hack
      //do this to get rid of "/" before the drive letter in windows
      dirPath = new Path(dirPath).toString();
    }
    if (foundAlias(pathToAliases, dirPath)) {
      return dirPath;
    }
    path = dirPath;

    String dirStr = dir.toString();
    int dirPathIndex = dirPath.lastIndexOf(Path.SEPARATOR);
    int dirStrIndex = dirStr.lastIndexOf(Path.SEPARATOR);
    while (dirPathIndex >= 0 && dirStrIndex >= 0) {
      dirStr = dirStr.substring(0, dirStrIndex);
      dirPath = dirPath.substring(0, dirPathIndex);
      //first try full match
      if (foundAlias(pathToAliases, dirStr)) {
        return dirStr;
      }
      if (foundAlias(pathToAliases, dirPath)) {
        return dirPath;
      }
      dirPathIndex = dirPath.lastIndexOf(Path.SEPARATOR);
      dirStrIndex = dirStr.lastIndexOf(Path.SEPARATOR);
    }
    return null;
  }

  /**
   * Get the list of operators from the operator tree that are needed for the path
   * @param pathToAliases  mapping from path to aliases
   * @param aliasToWork    The operator tree to be invoked for a given alias
   * @param dir            The path to look for
   **/
  public static List<Operator<? extends OperatorDesc>> doGetWorksFromPath(
    Map<String, ArrayList<String>> pathToAliases,
    Map<String, Operator<? extends OperatorDesc>> aliasToWork, Path dir) {
    List<Operator<? extends OperatorDesc>> opList =
      new ArrayList<Operator<? extends OperatorDesc>>();

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
  public static List<String> doGetAliasesFromPath(
    Map<String, ArrayList<String>> pathToAliases,
    Path dir) {
    if (pathToAliases == null) {
      return new ArrayList<String>();
    }
    String path = getMatchingPath(pathToAliases, dir);
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
    conf.setBoolean(ShimLoader.getHadoopShims().getHadoopConfNames().get("MAPREDSETUPCLEANUPNEEDED"), false);

    // option to bypass task cleanup task was introduced in hadoop-23 (MAPREDUCE-2206)
    // but can be backported. So we disable setup/cleanup in all versions >= 0.19
    conf.setBoolean(ShimLoader.getHadoopShims().getHadoopConfNames().get("MAPREDTASKCLEANUPNEEDED"), false);
  }
}
