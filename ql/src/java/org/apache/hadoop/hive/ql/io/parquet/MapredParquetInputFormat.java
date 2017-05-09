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
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetTableUtils;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
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

    propagateParquetTimeZoneTablePorperty((FileSplit) split, job);

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

  /**
   * Tries to find the table belonging to the file path of the split.
   * If the table can be determined, the parquet timezone property will be propagated
   * to the job configuration to be used during reading.
   * If the table cannot be determined, then do nothing.
   * @param split file split being read
   * @param job configuration to set the timezone property on
   */
  private void propagateParquetTimeZoneTablePorperty(FileSplit split, JobConf job) {
    PartitionDesc part = null;
    Path filePath = split.getPath();
    try {
      MapWork mapWork = Utilities.getMapWork(job);
      if(mapWork != null) {
        LOG.debug("Trying to find partition in MapWork for path " + filePath);
        Map<Path, PartitionDesc> pathToPartitionInfo = mapWork.getPathToPartitionInfo();

        part = HiveFileFormatUtils
            .getPartitionDescFromPathRecursively(pathToPartitionInfo, filePath, null);
        LOG.debug("Partition found " + part);
      }
    } catch (AssertionError ae) {
      LOG.warn("Cannot get partition description from " + filePath
          + " because " + ae.getMessage());
      part = null;
    } catch (Exception e) {
      LOG.warn("Cannot get partition description from " + filePath
          + " because " + e.getMessage());
      part = null;
    }

    if (part != null && part.getTableDesc() != null) {
      ParquetTableUtils.setParquetTimeZoneIfAbsent(job, part.getTableDesc().getProperties());
    }
  }
}
