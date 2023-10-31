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
package org.apache.hadoop.hive.ql.io.parquet.read;

import java.io.IOException;

import org.apache.hadoop.hive.ql.io.parquet.ParquetRecordReaderBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.StatsProvidingRecordReader;
import org.apache.hadoop.hive.ql.io.parquet.ProjectionPusher;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetInputSplit;
import org.apache.parquet.hadoop.util.ContextUtil;

public class ParquetRecordReaderWrapper extends ParquetRecordReaderBase
  implements RecordReader<NullWritable, ArrayWritable>, StatsProvidingRecordReader {
  public static final Logger LOG = LoggerFactory.getLogger(ParquetRecordReaderWrapper.class);

  private final long splitLen; // for getPos()

  private org.apache.hadoop.mapreduce.RecordReader<Void, ArrayWritable> realReader;
  // expect readReader return same Key & Value objects (common case)
  // this avoids extra serialization & deserialization of these objects
  private ArrayWritable valueObj = null;
  private boolean firstRecord = false;
  private boolean eof = false;

  public ParquetRecordReaderWrapper(
      final ParquetInputFormat<ArrayWritable> newInputFormat,
      final InputSplit oldSplit,
      final JobConf oldJobConf,
      final Reporter reporter)
          throws IOException, InterruptedException {
    this(newInputFormat, oldSplit, oldJobConf, reporter, new ProjectionPusher());
  }

  public ParquetRecordReaderWrapper(
      final ParquetInputFormat<ArrayWritable> newInputFormat,
      final InputSplit oldSplit,
      final JobConf oldJobConf,
      final Reporter reporter,
      final ProjectionPusher pusher)
          throws IOException, InterruptedException {
    this.splitLen = oldSplit.getLength();
    this.projectionPusher = pusher;
    this.serDeStats = new SerDeStats();

    jobConf = oldJobConf;
    final ParquetInputSplit split = getSplit(oldSplit, jobConf);

    TaskAttemptID taskAttemptID = TaskAttemptID.forName(jobConf.get(IOConstants.MAPRED_TASK_ID));
    if (taskAttemptID == null) {
      taskAttemptID = new TaskAttemptID();
    }

    // create a TaskInputOutputContext
    Configuration conf = jobConf;
    if (skipTimestampConversion ^ HiveConf.getBoolVar(
        conf, HiveConf.ConfVars.HIVE_PARQUET_TIMESTAMP_SKIP_CONVERSION)) {
      conf = new JobConf(jobConf);
      HiveConf.setBoolVar(conf,
        HiveConf.ConfVars.HIVE_PARQUET_TIMESTAMP_SKIP_CONVERSION, skipTimestampConversion);
    }

    final TaskAttemptContext taskContext = ContextUtil.newTaskAttemptContext(conf, taskAttemptID);
    if (split != null) {
      try {
        realReader = newInputFormat.createRecordReader(split, taskContext);
        realReader.initialize(split, taskContext);

        // read once to gain access to key and value objects
        if (realReader.nextKeyValue()) {
          firstRecord = true;
          valueObj = realReader.getCurrentValue();
        } else {
          eof = true;
        }
      } catch (final InterruptedException e) {
        throw new IOException(e);
      }
    } else {
      realReader = null;
      eof = true;
    }
    if (valueObj == null) { // Should initialize the value for createValue
      valueObj = new ArrayWritable(Writable.class, new Writable[schemaSize]);
    }
  }

  @Override
  public void close() throws IOException {
    if (realReader != null) {
      realReader.close();
    }
  }

  @Override
  public NullWritable createKey() {
    return null;
  }

  @Override
  public ArrayWritable createValue() {
    return valueObj;
  }

  @Override
  public long getPos() throws IOException {
    return (long) (splitLen * getProgress());
  }

  @Override
  public float getProgress() throws IOException {
    if (realReader == null) {
      return 1f;
    } else {
      try {
        return realReader.getProgress();
      } catch (final InterruptedException e) {
        throw new IOException(e);
      }
    }
  }

  @Override
  public boolean next(final NullWritable key, final ArrayWritable value) throws IOException {
    if (eof) {
      return false;
    }
    try {
      if (firstRecord) { // key & value are already read.
        firstRecord = false;
      } else if (!realReader.nextKeyValue()) {
        eof = true; // strictly not required, just for consistency
        return false;
      }

      final ArrayWritable tmpCurValue = realReader.getCurrentValue();
      if (value != tmpCurValue) {
        final Writable[] arrValue = value.get();
        final Writable[] arrCurrent = tmpCurValue.get();
        if (value != null && arrValue.length == arrCurrent.length) {
          System.arraycopy(arrCurrent, 0, arrValue, 0, arrCurrent.length);
        } else {
          if (arrValue.length != arrCurrent.length) {
            throw new IOException("DeprecatedParquetHiveInput : size of object differs. Value" +
              " size :  " + arrValue.length + ", Current Object size : " + arrCurrent.length);
          } else {
            throw new IOException("DeprecatedParquetHiveInput can not support RecordReaders that" +
              " don't return same key & value & value is null");
          }
        }
      }
      return true;
    } catch (final InterruptedException e) {
      throw new IOException(e);
    }
  }
}
