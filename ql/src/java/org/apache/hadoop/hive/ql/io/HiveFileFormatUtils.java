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
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.StringUtils;

/**
 * An util class for various Hive file format tasks.
 * registerOutputFormatSubstitute(Class, Class) 
 * getOutputFormatSubstitute(Class) are added for backward 
 * compatibility. They return the newly added HiveOutputFormat for the older 
 * ones.
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
    if (HiveOutputFormat.class.isAssignableFrom(origin))
      return (Class<? extends HiveOutputFormat>) origin;
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
    HiveFileFormatUtils.registerInputFormatChecker(SequenceFileInputFormat.class, SequenceFileInputFormatChecker.class);
    HiveFileFormatUtils.registerInputFormatChecker(RCFileInputFormat.class, RCFileInputFormat.class);
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
    Class<? extends InputFormatChecker> result = inputFormatCheckerMap.get(inputFormat);
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
      if(checkerCls==null && inputFormatCls.isAssignableFrom(TextInputFormat.class)) {
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
      if (result)
        return false;
    }
    return true;
  }
  
  
  public static String READ_COLUMN_IDS_CONF_STR = "hive.io.file.readcolumn.ids";

  /**
   * Sets read columns' ids(start from zero) for RCFile's Reader. Once a column
   * is included in the list, RCFile's reader will not skip its value.
   * 
   */
  public static void setReadColumnIDs(Configuration conf, ArrayList<Integer> ids) {
    String id = null;
    if (ids != null) {
      for (int i = 0; i < ids.size(); i++) {
        if (i == 0) {
          id = "" + ids.get(i);
        } else {
          id = id + StringUtils.COMMA_STR + ids.get(i);
        }
      }
    }

    if (id == null || id.length() <= 0) {
      conf.set(READ_COLUMN_IDS_CONF_STR, "");
      return;
    }

    conf.set(READ_COLUMN_IDS_CONF_STR, id);
  }

  /**
   * Returns an array of column ids(start from zero) which is set in the given
   * parameter <tt>conf</tt>.
   */
  public static ArrayList<Integer> getReadColumnIDs(Configuration conf) {
    String skips = conf.get(READ_COLUMN_IDS_CONF_STR, "");
    String[] list = StringUtils.split(skips);
    ArrayList<Integer> result = new ArrayList<Integer>(list.length);
    for (int i = 0; i < list.length; i++) {
      result.add(Integer.parseInt(list[i]));
    }
    return result;
  }

  /**
   * Clears the read column ids set in the conf, and will read all columns.
   */
  public static void setFullyReadColumns(Configuration conf) {
    conf.set(READ_COLUMN_IDS_CONF_STR, "");
  }
}
