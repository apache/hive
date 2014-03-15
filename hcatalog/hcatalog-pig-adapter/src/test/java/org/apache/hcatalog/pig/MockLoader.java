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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hcatalog.pig;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;

/**
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.pig.MockLoader} instead
 */
public class MockLoader extends LoadFunc {
  private static final class MockRecordReader extends RecordReader<Object, Object> {
    @Override
    public void close() throws IOException {
    }

    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
      return "mockKey";
    }

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
      return "mockValue";
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return 0.5f;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext arg1) throws IOException,
      InterruptedException {
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return true;
    }
  }

  private static final class MockInputSplit extends InputSplit implements Writable {
    private String location;

    public MockInputSplit() {
    }

    public MockInputSplit(String location) {
      this.location = location;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      return new String[]{location};
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
      return 10000000;
    }

    @Override
    public boolean equals(Object arg0) {
      return arg0 == this;
    }

    @Override
    public int hashCode() {
      return location.hashCode();
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
      location = arg0.readUTF();
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
      arg0.writeUTF(location);
    }
  }

  private static final class MockInputFormat extends InputFormat {

    private final String location;

    public MockInputFormat(String location) {
      this.location = location;
    }

    @Override
    public RecordReader createRecordReader(InputSplit arg0, TaskAttemptContext arg1)
      throws IOException, InterruptedException {
      return new MockRecordReader();
    }

    @Override
    public List getSplits(JobContext arg0) throws IOException, InterruptedException {
      return Arrays.asList(new MockInputSplit(location));
    }
  }

  private static final Map<String, Iterable<Tuple>> locationToData = new HashMap<String, Iterable<Tuple>>();

  public static void setData(String location, Iterable<Tuple> data) {
    locationToData.put(location, data);
  }

  private String location;

  private Iterator<Tuple> data;

  @Override
  public String relativeToAbsolutePath(String location, Path curDir) throws IOException {
    return location;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    this.location = location;
    if (location == null) {
      throw new IOException("null location passed to MockLoader");
    }
    this.data = locationToData.get(location).iterator();
    if (this.data == null) {
      throw new IOException("No data configured for location: " + location);
    }
  }

  @Override
  public Tuple getNext() throws IOException {
    if (data == null) {
      throw new IOException("data was not correctly initialized in MockLoader");
    }
    return data.hasNext() ? data.next() : null;
  }

  @Override
  public InputFormat getInputFormat() throws IOException {
    return new MockInputFormat(location);
  }

  @Override
  public void prepareToRead(RecordReader arg0, PigSplit arg1) throws IOException {
  }

}
