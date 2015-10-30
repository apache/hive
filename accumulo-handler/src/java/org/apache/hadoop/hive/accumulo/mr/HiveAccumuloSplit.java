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

package org.apache.hadoop.hive.accumulo.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.accumulo.core.client.mapred.RangeInputSplit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveInputFormat.HiveInputSplit;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps RangeInputSplit into a FileSplit so Hadoop won't complain when it tries to make its own
 * Path.
 *
 * <p>
 * If the {@link RangeInputSplit} is used directly, it will hit a branch of code in
 * {@link HiveInputSplit} which generates an invalid Path. Wrap it ourselves so that it doesn't
 * error
 */
public class HiveAccumuloSplit extends FileSplit implements InputSplit {
  private static final Logger log = LoggerFactory.getLogger(HiveAccumuloSplit.class);

  private RangeInputSplit split;

  public HiveAccumuloSplit() {
    super((Path) null, 0, 0, (String[]) null);
    split = new RangeInputSplit();
  }

  public HiveAccumuloSplit(RangeInputSplit split, Path dummyPath) {
    super(dummyPath, 0, 0, (String[]) null);
    this.split = split;
  }

  public RangeInputSplit getSplit() {
    return this.split;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    split.readFields(in);
  }

  @Override
  public String toString() {
    return "HiveAccumuloSplit: " + split;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    split.write(out);
  }

  @Override
  public long getLength() {
    int len = 0;
    try {
      return split.getLength();
    } catch (IOException e) {
      log.error("Error getting length for split: " + StringUtils.stringifyException(e));
    }
    return len;
  }

  @Override
  public String[] getLocations() throws IOException {
    return split.getLocations();
  }
}
