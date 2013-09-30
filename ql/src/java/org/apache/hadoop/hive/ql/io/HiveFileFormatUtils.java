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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HivePassThroughOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.ReflectionUtils;

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
        new HashMap<Class<? extends OutputFormat>, Class<? extends HiveOutputFormat>>();
    HiveFileFormatUtils.registerOutputFormatSubstitute(
        IgnoreKeyTextOutputFormat.class, HiveIgnoreKeyTextOutputFormat.class);
    HiveFileFormatUtils.registerOutputFormatSubstitute(
        SequenceFileOutputFormat.class, HiveSequenceFileOutputFormat.class);
  }

  static String realoutputFormat;

  @SuppressWarnings("unchecked")
  private static Map<Class<? extends OutputFormat>, Class<? extends HiveOutputFormat>>
  outputFormatSubstituteMap;

  /**
   * register a substitute.
   *
   * @param origin
   *          the class that need to be substituted
   * @param substitute
   */
  @SuppressWarnings("unchecked")
  public static synchronized void registerOutputFormatSubstitute(
      Class<? extends OutputFormat> origin,
      Class<? extends HiveOutputFormat> substitute) {
    outputFormatSubstituteMap.put(origin, substitute);
  }

  /**
   * get a OutputFormat's substitute HiveOutputFormat.
   */
  @SuppressWarnings("unchecked")
  public static synchronized Class<? extends HiveOutputFormat> getOutputFormatSubstitute(
      Class<?> origin, boolean storagehandlerflag) {
    if (HiveOutputFormat.class.isAssignableFrom(origin)) {
      return (Class<? extends HiveOutputFormat>) origin;
    }
    Class<? extends HiveOutputFormat> result = outputFormatSubstituteMap
        .get(origin);
    //register this output format into the map for the first time
    if ((storagehandlerflag == true) && (result == null)) {
      HiveFileFormatUtils.setRealOutputFormatClassName(origin.getName());
      result = HivePassThroughOutputFormat.class;
      HiveFileFormatUtils.registerOutputFormatSubstitute((Class<? extends OutputFormat>) origin,HivePassThroughOutputFormat.class);
    }
    return result;
  }

  /**
   * get a RealOutputFormatClassName corresponding to the HivePassThroughOutputFormat
   */
  @SuppressWarnings("unchecked")
  public static String getRealOutputFormatClassName() 
  {
    return realoutputFormat;
  }

  /**
   * set a RealOutputFormatClassName corresponding to the HivePassThroughOutputFormat
   */
  public static void setRealOutputFormatClassName(
      String destination) {
    if (destination != null){
      realoutputFormat = destination;
    }
    else {
      return;
    }
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

  public static FSRecordWriter getHiveRecordWriter(JobConf jc,
      TableDesc tableInfo, Class<? extends Writable> outputClass,
      FileSinkDesc conf, Path outPath, Reporter reporter) throws HiveException {
    boolean storagehandlerofhivepassthru = false;
    HiveOutputFormat<?, ?> hiveOutputFormat;
    try {
      if (tableInfo.getJobProperties() != null) {
        if (tableInfo.getJobProperties().get(HivePassThroughOutputFormat.HIVE_PASSTHROUGH_STORAGEHANDLER_OF_JOBCONFKEY) != null) {
            jc.set(HivePassThroughOutputFormat.HIVE_PASSTHROUGH_STORAGEHANDLER_OF_JOBCONFKEY,tableInfo.getJobProperties().get(HivePassThroughOutputFormat.HIVE_PASSTHROUGH_STORAGEHANDLER_OF_JOBCONFKEY));
            storagehandlerofhivepassthru  = true;
         }
      }
      if (storagehandlerofhivepassthru) {
         hiveOutputFormat = ReflectionUtils.newInstance(tableInfo.getOutputFileFormatClass(),jc);
      }
      else {
         hiveOutputFormat = tableInfo.getOutputFileFormatClass().newInstance();
      }
      boolean isCompressed = conf.getCompressed();
      JobConf jc_output = jc;
      if (isCompressed) {
        jc_output = new JobConf(jc);
        String codecStr = conf.getCompressCodec();
        if (codecStr != null && !codecStr.trim().equals("")) {
          Class<? extends CompressionCodec> codec = (Class<? extends CompressionCodec>) Class
              .forName(codecStr);
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

  public static FSRecordWriter getRecordWriter(JobConf jc,
      HiveOutputFormat<?, ?> hiveOutputFormat,
      final Class<? extends Writable> valueClass, boolean isCompressed,
      Properties tableProp, Path outPath, Reporter reporter
      ) throws IOException, HiveException {
    if (hiveOutputFormat != null) {
      return hiveOutputFormat.getHiveRecordWriter(jc, outPath, valueClass,
          isCompressed, tableProp, reporter);
    }
    return null;
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
      String dirStr = dir.toString();
      int dirPathIndex = dirPath.lastIndexOf(Path.SEPARATOR);
      int dirStrIndex = dirStr.lastIndexOf(Path.SEPARATOR);
      while (dirPathIndex >= 0 && dirStrIndex >= 0) {
        dirStr = dirStr.substring(0, dirStrIndex);
        dirPath = dirPath.substring(0, dirPathIndex);
        //first try full match
        part = pathToPartitionInfo.get(dirStr);
        if (part == null) {
          // LOG.warn("exact match not found, try ripping input path's theme and authority");
          part = pathToPartitionInfo.get(dirPath);
        }
        if (part != null) {
          break;
        }
        dirPathIndex = dirPath.lastIndexOf(Path.SEPARATOR);
        dirStrIndex = dirStr.lastIndexOf(Path.SEPARATOR);
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
}
