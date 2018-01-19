/*
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.io.DataCache;
import org.apache.hadoop.hive.common.io.FileMetadataCache;
import org.apache.hadoop.hive.ql.io.LlapCacheOnlyInputFormatInterface;
import org.apache.hadoop.hive.ql.io.parquet.vector.VectorizedParquetRecordReader;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * Vectorized input format for Parquet files
 */
public class VectorizedParquetInputFormat
  extends FileInputFormat<NullWritable, VectorizedRowBatch> 
  implements LlapCacheOnlyInputFormatInterface {

  private FileMetadataCache metadataCache = null;
  private DataCache dataCache = null;
  private Configuration cacheConf = null;

  public VectorizedParquetInputFormat() {
  }

  @Override
  public RecordReader<NullWritable, VectorizedRowBatch> getRecordReader(
    InputSplit inputSplit,
    JobConf jobConf,
    Reporter reporter) throws IOException {
    return new VectorizedParquetRecordReader(
        inputSplit, jobConf, metadataCache, dataCache, cacheConf);
  }

  @Override
  public void injectCaches(
      FileMetadataCache metadataCache, DataCache dataCache, Configuration cacheConf) {
    this.metadataCache = metadataCache;
    this.dataCache = dataCache;
    this.cacheConf = cacheConf;
  }
}
