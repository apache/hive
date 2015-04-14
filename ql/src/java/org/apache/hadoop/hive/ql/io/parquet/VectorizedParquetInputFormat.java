/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.vector.VectorColumnAssign;
import org.apache.hadoop.hive.ql.exec.vector.VectorColumnAssignFactory;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import parquet.hadoop.ParquetInputFormat;

/**
 * Vectorized input format for Parquet files
 */
public class VectorizedParquetInputFormat extends FileInputFormat<NullWritable, VectorizedRowBatch>
  implements VectorizedInputFormatInterface {

  private static final Log LOG = LogFactory.getLog(VectorizedParquetInputFormat.class);

  /**
   * Vectorized record reader for vectorized Parquet input format
   */
  private static class VectorizedParquetRecordReader implements
      RecordReader<NullWritable, VectorizedRowBatch> {
    private static final Log LOG = LogFactory.getLog(VectorizedParquetRecordReader.class);

    private final ParquetRecordReaderWrapper internalReader;
      private VectorizedRowBatchCtx rbCtx;
      private ArrayWritable internalValues;
      private Void internalKey;
      private VectorColumnAssign[] assigners;

    public VectorizedParquetRecordReader(
        ParquetInputFormat<ArrayWritable> realInput,
        FileSplit split,
        JobConf conf, Reporter reporter) throws IOException, InterruptedException {
      internalReader = new ParquetRecordReaderWrapper(
        realInput,
        split,
        conf,
        reporter);
      try {
        rbCtx = new VectorizedRowBatchCtx();
        rbCtx.init(conf, split);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

      @Override
      public NullWritable createKey() {
        internalKey = internalReader.createKey();
        return NullWritable.get();
      }

      @Override
      public VectorizedRowBatch createValue() {
        VectorizedRowBatch outputBatch = null;
        try {
          outputBatch = rbCtx.createVectorizedRowBatch();
          internalValues = internalReader.createValue();
        } catch (HiveException e) {
          throw new RuntimeException("Error creating a batch", e);
        }
        return outputBatch;
      }

      @Override
      public long getPos() throws IOException {
        return internalReader.getPos();
      }

      @Override
      public void close() throws IOException {
        internalReader.close();
      }

      @Override
      public float getProgress() throws IOException {
        return internalReader.getProgress();
      }

    @Override
    public boolean next(NullWritable key, VectorizedRowBatch outputBatch)
        throws IOException {
      if (assigners != null) {
        assert(outputBatch.numCols == assigners.length);
      }
      outputBatch.reset();
      int maxSize = outputBatch.getMaxSize();
      try {
        while (outputBatch.size < maxSize) {
          if (false == internalReader.next(internalKey, internalValues)) {
            outputBatch.endOfFile = true;
            break;
          }
          Writable[] writables = internalValues.get();

          if (null == assigners) {
            // Normally we'd build the assigners from the rbCtx.rowOI, but with Parquet
            // we have a discrepancy between the metadata type (Eg. tinyint -> BYTE) and
            // the writable value (IntWritable). see Parquet's ETypeConverter class.
            assigners = VectorColumnAssignFactory.buildAssigners(outputBatch, writables);
          }

          for(int i = 0; i < writables.length; ++i) {
            assigners[i].assignObjectValue(writables[i], outputBatch.size);
          }
          ++outputBatch.size;
         }
      } catch (HiveException e) {
        throw new RuntimeException(e);
      }
      return outputBatch.size > 0;
    }
  }

  private final ParquetInputFormat<ArrayWritable> realInput;

  public VectorizedParquetInputFormat(ParquetInputFormat<ArrayWritable> realInput) {
    this.realInput = realInput;
  }

  @SuppressWarnings("unchecked")
  @Override
  public RecordReader<NullWritable, VectorizedRowBatch> getRecordReader(
      InputSplit split, JobConf conf, Reporter reporter) throws IOException {
    try {
      return (RecordReader<NullWritable, VectorizedRowBatch>)
        new VectorizedParquetRecordReader(realInput, (FileSplit) split, conf, reporter);
    } catch (final InterruptedException e) {
      throw new RuntimeException("Cannot create a VectorizedParquetRecordReader", e);
    }
  }

}
