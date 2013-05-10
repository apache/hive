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

package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.InputFormatChecker;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;


public class CommonOrcInputFormat extends FileInputFormat<NullWritable, Writable>
    implements InputFormatChecker, VectorizedInputFormatInterface {

  OrcInputFormat oif = new OrcInputFormat();
  VectorizedOrcInputFormat voif = new VectorizedOrcInputFormat();

  private static class CommonOrcRecordReader
      implements RecordReader<NullWritable, Writable> {

    final RecordReader<NullWritable, VectorizedRowBatch> vorr;
    final RecordReader<NullWritable, OrcStruct> orr;

    public CommonOrcRecordReader(RecordReader<NullWritable, VectorizedRowBatch> vorr,
        RecordReader<NullWritable, OrcStruct> orr) {
      this.vorr = vorr;
      this.orr = orr;
    }

    @Override
    public void close() throws IOException {
      if (vorr != null) {
        vorr.close();
      } else {
        orr.close();
      }

    }

    @Override
    public NullWritable createKey() {
      if (vorr != null) {
        return vorr.createKey();
      } else {
        return orr.createKey();
      }
    }

    @Override
    public Writable createValue() {
      if (vorr != null) {
        return vorr.createValue();
      } else {
        return orr.createValue();
      }
    }

    @Override
    public long getPos() throws IOException {
      if (vorr != null) {
        return vorr.getPos();
      } else {
        return orr.getPos();
      }
    }

    @Override
    public float getProgress() throws IOException {
      if (vorr != null) {
        return vorr.getProgress();
      } else {
        return orr.getProgress();
      }
    }

    @Override
    public boolean next(NullWritable arg0, Writable arg1) throws IOException {
      if (vorr != null) {
        return vorr.next(arg0, (VectorizedRowBatch) arg1);
      } else {
        return orr.next(arg0, (OrcStruct) arg1);
      }
    }

  }

  @Override
  public boolean validateInput(FileSystem fs, HiveConf conf, ArrayList<FileStatus> files)
      throws IOException {
    boolean vectorPath = conf.getBoolean(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED.toString(),
        false);
    if (vectorPath) {
      return voif.validateInput(fs, conf, files);
    } else {
      return oif.validateInput(fs, conf, files);
    }
  }

  @Override
  public RecordReader<NullWritable, Writable> getRecordReader(InputSplit split, JobConf conf,
      Reporter reporter) throws IOException {
    boolean vectorPath = conf.getBoolean(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED.toString(),
        false);
    if (vectorPath) {
      RecordReader<NullWritable, VectorizedRowBatch> vorr = voif.getRecordReader(split, conf,
          reporter);
      return new CommonOrcRecordReader(vorr, null);
    } else {
      RecordReader<NullWritable, OrcStruct> orr = oif.getRecordReader(split, conf, reporter);
      return new CommonOrcRecordReader(null, orr);
    }
  }
}
