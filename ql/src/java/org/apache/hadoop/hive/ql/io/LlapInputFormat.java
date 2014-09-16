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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.api.Reader;
import org.apache.hadoop.hive.llap.api.RequestFactory;
import org.apache.hadoop.hive.llap.api.Vector;
import org.apache.hadoop.hive.llap.api.Vector.ColumnReader;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVectorVisitor;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.exec.vector.expressions.NullUtil;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.collect.Lists;

public class LlapInputFormat<T>
  implements InputFormat<NullWritable, T>, VectorizedInputFormatInterface {
  /** See RequestFactory class documentation on why this is necessary */
  private final static String IMPL_CLASS = "org.apache.hadoop.hive.llap.api.Llap";

  private final InputFormat<NullWritable, T> realInputFormat;
  private final RequestFactory reqFactory;
  private static final Log LOG = LogFactory.getLog(LlapInputFormat.class);

  public LlapInputFormat(InputFormat<NullWritable, T> realInputFormat, Configuration conf) {
    this.realInputFormat = realInputFormat;
    try {
      reqFactory = (RequestFactory)ReflectionUtils.newInstance(Class.forName(IMPL_CLASS), conf);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed to initialize impl", e);
    }
  }

  @Override
  public RecordReader<NullWritable, T> getRecordReader(InputSplit split, JobConf job,
      Reporter reporter) throws IOException {
    boolean isVectorMode = Utilities.isVectorMode(job);
    if (!isVectorMode) {
      LOG.error("No llap in non-vectorized mode; falling back to original");
      throw new UnsupportedOperationException("No llap in non-vectorized mode");
      // return realInputFormat.getRecordReader(split, job, reporter);
    }
    FileSplit fileSplit = (FileSplit)split; // should work
    reporter.setStatus(fileSplit.toString());
    try {
      List<Integer> includedCols = ColumnProjectionUtils.isReadAllColumns(job)
          ? null : ColumnProjectionUtils.getReadColumnIDs(job);
      if (includedCols.isEmpty()) {
        includedCols = null; // Also means read all columns? WTF?
      }
      Reader reader = reqFactory.createLocalRequest()
          .setSplit(split)
          .setSarg(SearchArgumentFactory.createFromConf(job))
          .setColumns(includedCols).submit();
      // TODO: presumably, we'll also pass the means to create original RecordReader
      //       for failover to LlapRecordReader somewhere around here. This will
      //       actually be quite complex because we'd somehow have to track what parts
      //       of file Llap has already returned, and skip these in fallback reader.
      // We are actually returning a completely wrong thing here wrt template parameters.
      // This is how vectorization does it presently; we hope the caller knows what it is doing.
      return createRecordReaderUnsafe(job, fileSplit, reader);
    } catch (Exception ex) {
      LOG.error("Local request failed; falling back to original", ex);
      throw new IOException(ex); // just rethrow for now, for clarity
      // return realInputFormat.getRecordReader(split, job, reporter);
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private RecordReader<NullWritable, T> createRecordReaderUnsafe(
      JobConf job, FileSplit fileSplit, Reader reader) {
    return (RecordReader)new VectorizedLlapRecordReader(reader, job, fileSplit);
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    return realInputFormat.getSplits(job, numSplits);
  }

  private static class VectorizedLlapRecordReader
      implements RecordReader<NullWritable, VectorizedRowBatch> {
    private final Reader reader;
    private Vector currentVector;
    private ColumnReader currentVectorSlice; // see VectorImpl, really just currentVector
    private int currentVectorOffset = -1; // number of rows read from current vector

    private VectorizedRowBatchCtx rbCtx;
    private final VrbHelper vrbHelper = new VrbHelper();
    private boolean addPartitionCols = true;

    public VectorizedLlapRecordReader(Reader reader, JobConf job, FileSplit split) {
      this.reader = reader;
      try {
        rbCtx = new VectorizedRowBatchCtx();
        rbCtx.init(job, split);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean next(NullWritable key, VectorizedRowBatch value) throws IOException {
      try {
        assert value != null;
        if (currentVector == null) {
          currentVector = reader.next();
          if (currentVector == null) {
            return false;
          }
          currentVectorOffset = 0;
        }
        // Add partition cols if necessary (see VectorizedOrcInputFormat for details).
        if (addPartitionCols) {
          rbCtx.addPartitionColsToBatch(value);
          addPartitionCols = false;
        }
        populateBatchFromVector(value);
        traceLogFirstRow(value);
      } catch (InterruptedException e) {
        throw new IOException(e);
      } catch (HiveException e) {
        throw new IOException(e);
      }
      return true;
    }

    private void traceLogFirstRow(VectorizedRowBatch value) {
      if (!LOG.isTraceEnabled()) return;
      String tmp = "First row is [";
      for (ColumnVector v : value.cols) {
        if (v instanceof LongColumnVector) {
          tmp += ((LongColumnVector)v).vector[0] + ", ";
        } else if (v instanceof DoubleColumnVector) {
          tmp += ((DoubleColumnVector)v).vector[0] + ", ";
        } else if (v == null) {
          tmp += "null, ";
        } else {
          tmp += "(something), ";
        }
      }
      LOG.trace(tmp + "]");
    }

    private void populateBatchFromVector(VectorizedRowBatch target) throws IOException {
      // TODO: eventually, when vectorized pipeline can work directly
        //       on vectors instead of VRB, this will be a noop.
      // TODO: track time spent building VRBs as opposed to processing.
      int rowCount = VectorizedRowBatch.DEFAULT_SIZE,
          rowsRemaining = currentVector.getNumberOfRows() - currentVectorOffset;
      if (rowsRemaining <= rowCount) {
        rowCount = rowsRemaining;
      }
      int[] columnMap = rbCtx.getIncludedColumnIndexes();
      if (columnMap.length != currentVector.getNumberOfColumns()) {
        throw new RuntimeException("Unexpected number of columns, VRB has " + columnMap.length
            + " included, but vector has " + currentVector.getNumberOfColumns());
      }
      vrbHelper.prepare(rowCount);
      // VRB was created from VrbCtx, so we already have pre-allocated column vectors
      for (int vectorIx = 0; vectorIx < currentVector.getNumberOfColumns(); ++vectorIx) {
        int colIx = columnMap[vectorIx];
        currentVectorSlice = currentVector.next(colIx, rowCount);
        target.cols[colIx].visit(vrbHelper);
      }
      target.selectedInUse = false;
      target.size = rowCount;

      if (rowsRemaining == rowCount) {
        currentVector = null;
        currentVectorOffset = -1;
      } else {
        currentVectorOffset += rowCount;
      }
    }

    @Override
    public NullWritable createKey() {
      return NullWritable.get();
    }

    @Override
    public VectorizedRowBatch createValue() {
      try {
        return rbCtx.createVectorizedRowBatch();
      } catch (HiveException e) {
        throw new RuntimeException("Error creating a batch", e);
      }
    }

    @Override
    public long getPos() throws IOException {
      return -1; // Position doesn't make sense for async reader, chunk order is arbitrary.
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }

    @Override
    public float getProgress() throws IOException {
      // TODO: plumb progress info thru the reader if we can get metadata from loader first.
      return 0.0f;
    }

    /**
     * There's one VrbHelper object per RecordReader object. We could just implement
     * the visitor in RecordReader itself, but it's a bit cleaner like this.
     */
    private final class VrbHelper implements ColumnVectorVisitor {
      private int rowCount = -1;
      public void prepare(int rowCount) {
        this.rowCount = rowCount;
      }

      @Override
      public void visit(LongColumnVector c) throws IOException {
        boolean hasNulls = currentVectorSlice.hasNulls();
        boolean isSameValue = currentVectorSlice.isSameValue();
        if (isSameValue) {
          if (hasNulls) {
            c.fillWithNulls();
          } else {
            c.fill(currentVectorSlice.getLong());
          }
        } else {
          c.reset();
          c.noNulls = !hasNulls;
          currentVectorSlice.copyLongs(c.vector, hasNulls ? c.isNull : null, 0);
          if (hasNulls) {
            NullUtil.setNullDataEntriesLong(c, false, null, rowCount);
          }
        }
      }


      @Override
      public void visit(DoubleColumnVector c) throws IOException {
        boolean hasNulls = currentVectorSlice.hasNulls();
        boolean isSameValue = currentVectorSlice.isSameValue();
        if (isSameValue) {
          if (hasNulls) {
            c.fillWithNulls();
          } else {
            c.fill(currentVectorSlice.getDouble());
          }
        } else {
          c.reset();
          c.noNulls = !hasNulls;
          currentVectorSlice.copyDoubles(c.vector, hasNulls ? c.isNull : null, 0);
          if (hasNulls) {
            NullUtil.setNullDataEntriesDouble(c, false, null, rowCount);
          }
        }
      }

      @Override
      public void visit(DecimalColumnVector c) throws IOException {
        boolean hasNulls = currentVectorSlice.hasNulls();
        boolean isSameValue = currentVectorSlice.isSameValue();
        if (isSameValue) {
          if (hasNulls) {
            c.fillWithNulls();
          } else {
            c.fill(currentVectorSlice.getDecimal());
          }
        } else {
          c.reset();
          c.noNulls = !hasNulls;
          currentVectorSlice.copyDecimals(c.vector, hasNulls ? c.isNull : null, 0);
          if (hasNulls) {
            NullUtil.setNullDataEntriesDecimal(c, false, null, rowCount);
          }
        }
      }

      @Override
      public void visit(BytesColumnVector c) throws IOException {
        boolean hasNulls = currentVectorSlice.hasNulls();
        boolean isSameValue = currentVectorSlice.isSameValue();
        if (isSameValue) {
          if (hasNulls) {
            c.fillWithNulls();
          } else {
            c.fill(currentVectorSlice.getBytes());
          }
        } else {
          c.reset();
          c.noNulls = !hasNulls;
          currentVectorSlice.copyBytes(c.vector, c.start, c.length, hasNulls ? c.isNull : null, 0);
          if (hasNulls) {
            NullUtil.setNullDataEntriesBytes(c, false, null, rowCount);
          }
        }
      }
    }
  }
}
