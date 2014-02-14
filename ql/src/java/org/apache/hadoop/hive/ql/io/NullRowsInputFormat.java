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
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * NullRowsInputFormat outputs null rows, maximum 100.
 */
public class NullRowsInputFormat implements InputFormat<NullWritable, NullWritable>,
    JobConfigurable {

  static final int MAX_ROW = 100; // to prevent infinite loop
  static final Log LOG = LogFactory.getLog(NullRowsRecordReader.class.getName());

  public static class DummyInputSplit implements InputSplit {
    public DummyInputSplit() {
    }

    @Override
    public long getLength() throws IOException {
      return 1;
    }

    @Override
    public String[] getLocations() throws IOException {
      return new String[0];
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
    }

  }

  public static class NullRowsRecordReader implements RecordReader<NullWritable, NullWritable> {

    private int counter;

    public NullRowsRecordReader() {
    }
    @Override
    public void close() throws IOException {
    }

    @Override
    public NullWritable createKey() {
      return NullWritable.get();
    }

    @Override
    public NullWritable createValue() {
      return NullWritable.get();
    }

    @Override
    public long getPos() throws IOException {
      return counter;
    }

    @Override
    public float getProgress() throws IOException {
      return (float)counter / MAX_ROW;
    }

    @Override
    public boolean next(NullWritable arg0, NullWritable arg1) throws IOException {
      if (counter++ < MAX_ROW) {
        return true;
      }
      return false;
    }
  }

  @Override
  public RecordReader<NullWritable, NullWritable> getRecordReader(InputSplit arg0,
      JobConf arg1, Reporter arg2) throws IOException {
    return new NullRowsRecordReader();
  }

  @Override
  public InputSplit[] getSplits(JobConf arg0, int arg1) throws IOException {
    InputSplit[] ret = new InputSplit[1];
    ret[0] = new DummyInputSplit();
    LOG.info("Calculating splits");
    return ret;
  }

  @Override
  public void configure(JobConf job) {
    LOG.info("Using null rows input format");
  }

}
