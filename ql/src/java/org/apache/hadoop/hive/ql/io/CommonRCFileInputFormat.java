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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * CommonRCFileInputFormat.
 * Wrapper class that calls the correct input format for RC file base on
 * HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED parameter
 */
public class CommonRCFileInputFormat extends FileInputFormat<Writable, Writable>
    implements InputFormatChecker, VectorizedInputFormatInterface{

  RCFileInputFormat<LongWritable, BytesRefArrayWritable> rcif =
      new RCFileInputFormat<LongWritable, BytesRefArrayWritable>();
  VectorizedRCFileInputFormat vrcif = new VectorizedRCFileInputFormat();

  private static class CommonOrcRecordReader
      implements RecordReader<Writable, Writable> {

    final RecordReader<NullWritable, VectorizedRowBatch> vrcrr;
    final RecordReader<LongWritable, BytesRefArrayWritable> rcrr;

    public CommonOrcRecordReader(RecordReader<NullWritable, VectorizedRowBatch> vrcrr,
        RecordReader<LongWritable, BytesRefArrayWritable> rcrr) {
      this.vrcrr = vrcrr;
      this.rcrr = rcrr;
    }

    @Override
    public void close() throws IOException {
      if (vrcrr != null) {
        vrcrr.close();
      } else {
        rcrr.close();
      }

    }

    @Override
    public Writable createKey() {
      if (vrcrr != null) {
        return vrcrr.createKey();
      } else {
        return rcrr.createKey();
      }
    }

    @Override
    public Writable createValue() {
      if (vrcrr != null) {
        return vrcrr.createValue();
      } else {
        return rcrr.createValue();
      }
    }

    @Override
    public long getPos() throws IOException {
      if (vrcrr != null) {
        return vrcrr.getPos();
      } else {
        return rcrr.getPos();
      }
    }

    @Override
    public float getProgress() throws IOException {
      if (vrcrr != null) {
        return vrcrr.getProgress();
      } else {
        return rcrr.getProgress();
      }
    }

    @Override
    public boolean next(Writable arg0, Writable arg1) throws IOException {
      if (vrcrr != null) {
        return vrcrr.next(NullWritable.get(), (VectorizedRowBatch) arg1);
      } else {
        LongWritable d = new LongWritable();
        return rcrr.next(d, (BytesRefArrayWritable) arg1);
      }
    }

  }

  @Override
  public boolean validateInput(FileSystem fs, HiveConf conf, ArrayList<FileStatus> files)
      throws IOException {
    boolean vectorPath =
        conf.getBoolean(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED.toString(), true);
    if (vectorPath) {
      return vrcif.validateInput(fs, conf, files);
    } else {
      return rcif.validateInput(fs, conf, files);
    }
  }

  @Override
  public RecordReader<Writable, Writable> getRecordReader(InputSplit split, JobConf conf,
      Reporter reporter) throws IOException {
    boolean vectorPath = conf.getBoolean(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED.toString(), true);
    if (vectorPath) {
      RecordReader<NullWritable, VectorizedRowBatch> vrcrr = vrcif.getRecordReader(split, conf, reporter);
      return new CommonOrcRecordReader(vrcrr, null);
    } else {
      RecordReader<LongWritable, BytesRefArrayWritable> rcrr = rcif.getRecordReader(split, conf, reporter);
      return new CommonOrcRecordReader(null, rcrr);
    }
  }
}
