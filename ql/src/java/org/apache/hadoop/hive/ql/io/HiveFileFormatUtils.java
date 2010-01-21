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
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.fileSinkDesc;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * An util class for various Hive file format tasks.
 * registerOutputFormatSubstitute(Class, Class) getOutputFormatSubstitute(Class)
 * are added for backward compatibility. They return the newly added
 * HiveOutputFormat for the older ones.
 * 
 */
public class HiveFileFormatUtils {

  static {
    outputFormatSubstituteMap = new HashMap<Class<? extends OutputFormat>, Class<? extends HiveOutputFormat>>();
    HiveFileFormatUtils.registerOutputFormatSubstitute(
        IgnoreKeyTextOutputFormat.class, HiveIgnoreKeyTextOutputFormat.class);
    HiveFileFormatUtils.registerOutputFormatSubstitute(
        SequenceFileOutputFormat.class, HiveSequenceFileOutputFormat.class);
  }

  @SuppressWarnings("unchecked")
  private static Map<Class<? extends OutputFormat>, Class<? extends HiveOutputFormat>> outputFormatSubstituteMap;

  /**
   * register a substitute
   * 
   * @param origin
   *          the class that need to be substituted
   * @param substitute
   */
  @SuppressWarnings("unchecked")
  public synchronized static void registerOutputFormatSubstitute(
      Class<? extends OutputFormat> origin,
      Class<? extends HiveOutputFormat> substitute) {
    outputFormatSubstituteMap.put(origin, substitute);
  }

  /**
   * get a OutputFormat's substitute HiveOutputFormat
   */
  @SuppressWarnings("unchecked")
  public synchronized static Class<? extends HiveOutputFormat> getOutputFormatSubstitute(
      Class<?> origin) {
    if (HiveOutputFormat.class.isAssignableFrom(origin)) {
      return (Class<? extends HiveOutputFormat>) origin;
    }
    Class<? extends HiveOutputFormat> result = outputFormatSubstituteMap
        .get(origin);
    return result;
  }

  /**
   * get the final output path of a given FileOutputFormat.
   * 
   * @param parent
   *          parent dir of the expected final output path
   * @param jc
   *          job configuration
   */
  public static Path getOutputFormatFinalPath(Path parent, JobConf jc,
      HiveOutputFormat<?, ?> hiveOutputFormat, boolean isCompressed,
      Path defaultFinalPath) throws IOException {
    if (hiveOutputFormat instanceof HiveIgnoreKeyTextOutputFormat) {
      return new Path(parent, Utilities.getTaskId(jc)
          + Utilities.getFileExtension(jc, isCompressed));
    }
    return defaultFinalPath;
  }

  static {
    inputFormatCheckerMap = new HashMap<Class<? extends InputFormat>, Class<? extends InputFormatChecker>>();
    HiveFileFormatUtils.registerInputFormatChecker(
        SequenceFileInputFormat.class, SequenceFileInputFormatChecker.class);
    HiveFileFormatUtils.registerInputFormatChecker(RCFileInputFormat.class,
        RCFileInputFormat.class);
    inputFormatCheckerInstanceCache = new HashMap<Class<? extends InputFormatChecker>, InputFormatChecker>();
  }

  @SuppressWarnings("unchecked")
  private static Map<Class<? extends InputFormat>, Class<? extends InputFormatChecker>> inputFormatCheckerMap;

  private static Map<Class<? extends InputFormatChecker>, InputFormatChecker> inputFormatCheckerInstanceCache;

  /**
   * register an InputFormatChecker for a given InputFormat
   * 
   * @param format
   *          the class that need to be substituted
   * @param checker
   */
  @SuppressWarnings("unchecked")
  public synchronized static void registerInputFormatChecker(
      Class<? extends InputFormat> format,
      Class<? extends InputFormatChecker> checker) {
    inputFormatCheckerMap.put(format, checker);
  }

  /**
   * get an InputFormatChecker for a file format.
   */
  public synchronized static Class<? extends InputFormatChecker> getInputFormatChecker(
      Class<?> inputFormat) {
    Class<? extends InputFormatChecker> result = inputFormatCheckerMap
        .get(inputFormat);
    return result;
  }

  /**
   * checks if files are in same format as the given input format
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
      tableDesc tableInfo, Class<? extends Writable> outputClass,
      fileSinkDesc conf, Path outPath) throws HiveException {
    try {
      HiveOutputFormat<?, ?> hiveOutputFormat = tableInfo
          .getOutputFileFormatClass().newInstance();
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
          isCompressed, tableInfo.getProperties(), outPath);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public static RecordWriter getRecordWriter(JobConf jc,
      HiveOutputFormat<?, ?> hiveOutputFormat,
      final Class<? extends Writable> valueClass, boolean isCompressed,
      Properties tableProp, Path outPath) throws IOException, HiveException {
    if (hiveOutputFormat != null) {
      return hiveOutputFormat.getHiveRecordWriter(jc, outPath, valueClass,
          isCompressed, tableProp, null);
    }
    return null;
  }

}
