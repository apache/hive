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
import java.util.Properties;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 *  This pass through class is used to wrap OutputFormat implementations such that new OutputFormats not derived from
 *  HiveOutputFormat gets through the checker
 */

public class HivePassThroughOutputFormat<K, V> implements Configurable, HiveOutputFormat<K, V>{

  private OutputFormat<? super WritableComparable<?>, ? super Writable> actualOutputFormat;
  private String actualOutputFormatClass = "";
  private Configuration conf;
  private boolean initialized;
  public static final String HIVE_PASSTHROUGH_OF_CLASSNAME =
                                  "org.apache.hadoop.hive.ql.io.HivePassThroughOutputFormat";

  public static final String HIVE_PASSTHROUGH_STORAGEHANDLER_OF_JOBCONFKEY =
                                 "hive.passthrough.storagehandler.of";

  public HivePassThroughOutputFormat() {
    //construct this class through ReflectionUtils from FileSinkOperator
    this.actualOutputFormat = null;
    this.initialized = false;
  }

  private void createActualOF() throws IOException {
    Class<? extends OutputFormat> cls;
    try {
      int e;
      if (actualOutputFormatClass != null)
       {
        cls =
           (Class<? extends OutputFormat>) Class.forName(actualOutputFormatClass, true,
                JavaUtils.getClassLoader());
      } else {
        throw new RuntimeException("Null pointer detected in actualOutputFormatClass");
      }
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
    OutputFormat<? super WritableComparable<?>, ? super Writable> actualOF =
         (OutputFormat<? super WritableComparable, ? super Writable>)
            ReflectionUtils.newInstance(cls, this.getConf());
    this.actualOutputFormat = actualOF;
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
    if (this.initialized == false) {
      createActualOF();
      this.initialized = true;
    }
   this.actualOutputFormat.checkOutputSpecs(ignored, job);
  }

  @Override
  public org.apache.hadoop.mapred.RecordWriter<K, V> getRecordWriter(FileSystem ignored,
       JobConf job, String name, Progressable progress) throws IOException {
    if (this.initialized == false) {
      createActualOF();
      this.initialized = true;
    }
    return (RecordWriter<K, V>) this.actualOutputFormat.getRecordWriter(ignored,
                 job, name, progress);
  }

  @Override
  public FSRecordWriter getHiveRecordWriter(
      JobConf jc, Path finalOutPath, Class<? extends Writable> valueClass, boolean isCompressed,
      Properties tableProperties, Progressable progress) throws IOException {
    if (this.initialized == false) {
      createActualOF();
    }
    if (this.actualOutputFormat instanceof HiveOutputFormat) {
      return ((HiveOutputFormat<K, V>) this.actualOutputFormat).getHiveRecordWriter(jc,
           finalOutPath, valueClass, isCompressed, tableProperties, progress);
    }
    else {
      FileSystem fs = finalOutPath.getFileSystem(jc);
      HivePassThroughRecordWriter hivepassthroughrecordwriter = new HivePassThroughRecordWriter(
              this.actualOutputFormat.getRecordWriter(fs, jc, null, progress));
      return hivepassthroughrecordwriter;
    }
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration config) {
    if (config.get(HIVE_PASSTHROUGH_STORAGEHANDLER_OF_JOBCONFKEY) != null) {
      actualOutputFormatClass = config.get(HIVE_PASSTHROUGH_STORAGEHANDLER_OF_JOBCONFKEY);
    }
    this.conf = config;
  }
}
