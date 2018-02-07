/*
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;

import org.apache.hadoop.hive.ql.exec.Utilities;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * NullRowsInputFormat outputs null rows, maximum 100.
 */
public class NullRowsInputFormat implements InputFormat<NullWritable, NullWritable>,
    JobConfigurable, VectorizedInputFormatInterface {

  static final int MAX_ROW = 100; // to prevent infinite loop
  static final Logger LOG = LoggerFactory.getLogger(NullRowsRecordReader.class.getName());

  public static class DummyInputSplit extends FileSplit {
    @SuppressWarnings("unused")  // Serialization ctor.
    private DummyInputSplit() {
      super();
    }

    public DummyInputSplit(String path) {
      this(new Path(path, "null"));
    }

    public DummyInputSplit(Path path) {
      super(path, 0, 1, (String[]) null);
    }
  }


  @SuppressWarnings("rawtypes")
  public static class NullRowsRecordReader implements RecordReader {

    private int counter;
    protected final VectorizedRowBatchCtx rbCtx;
    private final Object[] partitionValues;
    private boolean addPartitionCols = true;

    public NullRowsRecordReader(Configuration conf, InputSplit split) throws IOException {
      boolean isVectorMode = Utilities.getIsVectorized(conf);
      if (LOG.isDebugEnabled()) {
        LOG.debug(getClass().getSimpleName() + " in "
            + (isVectorMode ? "" : "non-") + "vector mode");
      }
      if (isVectorMode) {
        rbCtx = Utilities.getVectorizedRowBatchCtx(conf);
        int partitionColumnCount = rbCtx.getPartitionColumnCount();
        if (partitionColumnCount > 0) {
          partitionValues = new Object[partitionColumnCount];
          VectorizedRowBatchCtx.getPartitionValues(rbCtx, conf, (FileSplit)split, partitionValues);
        } else {
          partitionValues = null;
        }
      } else {
        rbCtx = null;
        partitionValues = null;
      }
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public NullWritable createKey() {
      return NullWritable.get();
    }

    @Override
    public Object createValue() {
      return rbCtx == null ? NullWritable.get() :
        rbCtx.createVectorizedRowBatch();
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
    public boolean next(Object arg0, Object value) throws IOException {
      if (rbCtx != null) {
        if (counter >= MAX_ROW) {
          return false;
        }
        makeNullVrb(value, MAX_ROW);
        counter = MAX_ROW;
        return true;
      } else if (counter++ < MAX_ROW) {
        return true;
      }
      return false;
    }

    protected void makeNullVrb(Object value, int size) {
      VectorizedRowBatch vrb = (VectorizedRowBatch)value;
      if (addPartitionCols) {
        if (partitionValues != null) {
          rbCtx.addPartitionColsToBatch(vrb, partitionValues);
        }
        addPartitionCols = false;
      }

      vrb.size = size;
      vrb.selectedInUse = false;
      for (int i = 0; i < rbCtx.getDataColumnCount(); i++) {
        ColumnVector cv = vrb.cols[i];
        if (cv == null) {
          continue;
        }
        cv.noNulls = false;
        cv.isRepeating = true;
        cv.isNull[0] = true;
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public RecordReader<NullWritable, NullWritable> getRecordReader(InputSplit split,
      JobConf conf, Reporter arg2) throws IOException {
    return new NullRowsRecordReader(conf, split);
  }

  @Override
  public InputSplit[] getSplits(JobConf conf, int arg1) throws IOException {
    // It's important to read the correct nulls! (in truth, the path is needed for SplitGrouper).
    String[] paths = conf.getTrimmedStrings(FileInputFormat.INPUT_DIR, (String[])null);
    if (paths == null) {
      throw new IOException("Cannot find path in conf");
    }
    InputSplit[] result = new InputSplit[paths.length];
    for (int i = 0; i < paths.length; ++i) {
      result[i] = new DummyInputSplit(paths[i]);
    }
    return result;
  }

  @Override
  public void configure(JobConf job) {
    LOG.info("Using null rows input format");
  }

}
