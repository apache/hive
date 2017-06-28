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

import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.RecordReader;

import org.apache.parquet.hadoop.ParquetInputFormat;


/**
 *
 * A Parquet InputFormat for Hive (with the deprecated package mapred)
 *
 * NOTE: With HIVE-9235 we removed "implements VectorizedParquetInputFormat" since all data types
 *       are not currently supported.  Removing the interface turns off vectorization.
 */
public class MapredParquetInputFormat extends FileInputFormat<NullWritable, ArrayWritable>
  implements VectorizedInputFormatInterface {

  private static final Logger LOG = LoggerFactory.getLogger(MapredParquetInputFormat.class);

  private final ParquetInputFormat<ArrayWritable> realInput;

  private final transient VectorizedParquetInputFormat vectorizedSelf;

  public MapredParquetInputFormat() {
    this(new ParquetInputFormat<ArrayWritable>(DataWritableReadSupport.class));
  }

  protected MapredParquetInputFormat(final ParquetInputFormat<ArrayWritable> inputFormat) {
    this.realInput = inputFormat;
    vectorizedSelf = new VectorizedParquetInputFormat();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public org.apache.hadoop.mapred.RecordReader<NullWritable, ArrayWritable> getRecordReader(
      final org.apache.hadoop.mapred.InputSplit split,
      final org.apache.hadoop.mapred.JobConf job,
      final org.apache.hadoop.mapred.Reporter reporter
      ) throws IOException {
    try {
      if (Utilities.getUseVectorizedInputFileFormat(job)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Using vectorized record reader");
        }
        return (RecordReader) vectorizedSelf.getRecordReader(split, job, reporter);
      }
      else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Using row-mode record reader");
        }
        return new ParquetRecordReaderWrapper(realInput, split, job, reporter);
      }
    } catch (final InterruptedException e) {
      throw new RuntimeException("Cannot create a RecordReaderWrapper", e);
    }
  }
}
