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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

/**
 * An util class for various Hive file format tasks.
 * {@link #registerOutputFormatSubstitute(Class, Class) and 
 * {@link #getOutputFormatSubstitute(Class)} are added for backward 
 * compatibility. They return the newly added HiveOutputFormat for the older 
 * ones.
 * 
 * }
 * 
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

  /**
   * checks if files are in same format as the given input format
   */
  public static boolean checkInputFormat(FileSystem fs, HiveConf conf,
      Class<? extends InputFormat> inputFormatCls, ArrayList<FileStatus> files)
      throws HiveException {
    if (files.size() > 0) {
      boolean tableIsSequenceFile = inputFormatCls
          .equals(SequenceFileInputFormat.class);
      int fileId = 0;
      boolean fileIsSequenceFile = true;
      try {
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, files.get(
            fileId).getPath(), conf);
        reader.close();
      } catch (IOException e) {
        fileIsSequenceFile = false;
      }
      if (!fileIsSequenceFile && tableIsSequenceFile) {
        throw new HiveException(
            "Cannot load text files into a table stored as SequenceFile.");
      }
      if (fileIsSequenceFile && !tableIsSequenceFile) {
        throw new HiveException(
            "Cannot load SequenceFiles into a table stored as TextFile.");
      }
      return true;
    }
    return false;
  }
}
