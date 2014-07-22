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
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.RecordReader;

import parquet.hadoop.ParquetInputFormat;


/**
 *
 * A Parquet InputFormat for Hive (with the deprecated package mapred)
 *
 */
public class MapredParquetInputFormat extends FileInputFormat<Void, ArrayWritable>
    implements VectorizedInputFormatInterface {

  private static final Log LOG = LogFactory.getLog(MapredParquetInputFormat.class);

  private final ParquetInputFormat<ArrayWritable> realInput;

  private final transient VectorizedParquetInputFormat vectorizedSelf;

  public MapredParquetInputFormat() {
    this(new ParquetInputFormat<ArrayWritable>(DataWritableReadSupport.class));
  }

  protected MapredParquetInputFormat(final ParquetInputFormat<ArrayWritable> inputFormat) {
    this.realInput = inputFormat;
    vectorizedSelf = new VectorizedParquetInputFormat(inputFormat);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public org.apache.hadoop.mapred.RecordReader<Void, ArrayWritable> getRecordReader(
      final org.apache.hadoop.mapred.InputSplit split,
      final org.apache.hadoop.mapred.JobConf job,
      final org.apache.hadoop.mapred.Reporter reporter
      ) throws IOException {
    try {
      if (Utilities.isVectorMode(job)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Using vectorized record reader");
        }
        return (RecordReader) vectorizedSelf.getRecordReader(split, job, reporter);
      }
      else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Using row-mode record reader");
        }
        return (RecordReader<Void, ArrayWritable>)
          new ParquetRecordReaderWrapper(realInput, split, job, reporter);
      }
    } catch (final InterruptedException e) {
      throw new RuntimeException("Cannot create a RecordReaderWrapper", e);
    }
  }
}
