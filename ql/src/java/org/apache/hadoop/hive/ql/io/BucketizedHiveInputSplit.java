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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveInputFormat.HiveInputSplit;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.HadoopShims.CombineFileInputFormatShim;
import org.apache.hadoop.hive.shims.HadoopShims.InputSplitShim;
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
import org.apache.hadoop.util.ReflectionUtils;

/**
 * HiveInputSplit encapsulates an InputSplit with its corresponding
 * inputFormatClass. The reason that it derives from FileSplit is to make sure
 * "map.input.file" in MapTask.
 */
public class BucketizedHiveInputSplit extends HiveInputSplit {

  protected InputSplit[] inputSplits;
  protected String inputFormatClassName;

  public String getInputFormatClassName() {
    return inputFormatClassName;
  }

  public void setInputFormatClassName(String inputFormatClassName) {
    this.inputFormatClassName = inputFormatClassName;
  }

  public BucketizedHiveInputSplit() {
    // This is the only public constructor of FileSplit
    super();
  }

  public BucketizedHiveInputSplit(InputSplit[] inputSplits,
      String inputFormatClassName) {
    // This is the only public constructor of FileSplit
    super();

    assert (inputSplits != null && inputSplits.length > 0);
    this.inputSplits = inputSplits;
    this.inputFormatClassName = inputFormatClassName;
  }

  public int getNumSplits() {
    return inputSplits.length;
  }

  public InputSplit getSplit(int idx) {
    assert (idx >= 0 && idx < inputSplits.length);
    return inputSplits[idx];
  }

  public String inputFormatClassName() {
    return inputFormatClassName;
  }

  @Override
  public Path getPath() {
    if (inputSplits != null && inputSplits.length > 0
        && inputSplits[0] instanceof FileSplit) {
      return ((FileSplit) inputSplits[0]).getPath();
    }
    return new Path("");
  }

  /** The position of the first byte in the file to process. */
  @Override
  public long getStart() {
    if (inputSplits != null && inputSplits.length > 0
        && inputSplits[0] instanceof FileSplit) {
      return ((FileSplit) inputSplits[0]).getStart();
    }
    return 0;
  }

  @Override
  public String toString() {
    if (inputSplits != null && inputSplits.length > 0) {
      return inputFormatClassName + ":" + inputSplits[0].toString();
    }
    return inputFormatClassName + ":null";
  }

  @Override
  public long getLength() {
    long r = 0;
    if (inputSplits != null) {
      try {
        for (InputSplit inputSplit : inputSplits) {
          r += inputSplit.getLength();
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return r;
  }

  public long getLength(int idx) {
    if (inputSplits != null) {
      try {
        return inputSplits[idx].getLength();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return -1;
  }

  @Override
  public String[] getLocations() throws IOException {
    assert (inputSplits != null && inputSplits.length > 0);
    return inputSplits[0].getLocations();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    String inputSplitClassName = in.readUTF();

    int numSplits = in.readInt();
    inputSplits = new InputSplit[numSplits];
    for (int i = 0; i < numSplits; i++) {
      try {
        inputSplits[i] = (InputSplit) ReflectionUtils.newInstance(conf
            .getClassByName(inputSplitClassName), conf);
      } catch (Exception e) {
        throw new IOException(
            "Cannot create an instance of InputSplit class = "
                + inputSplitClassName + ":" + e.getMessage());
      }
      inputSplits[i].readFields(in);
    }
    inputFormatClassName = in.readUTF();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    assert (inputSplits != null && inputSplits.length > 0);
    out.writeUTF(inputSplits[0].getClass().getName());
    out.writeInt(inputSplits.length);
    for (InputSplit inputSplit : inputSplits) {
      inputSplit.write(out);
    }
    out.writeUTF(inputFormatClassName);
  }
}
