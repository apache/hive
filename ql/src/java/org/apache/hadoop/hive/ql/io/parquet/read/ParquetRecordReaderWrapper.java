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
package org.apache.hadoop.hive.ql.io.parquet.read;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.ProjectionPusher;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.ParquetInputSplit;
import parquet.hadoop.api.ReadSupport.ReadContext;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.FileMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.hadoop.util.ContextUtil;
import parquet.schema.MessageTypeParser;

public class ParquetRecordReaderWrapper  implements RecordReader<Void, ArrayWritable> {
  public static final Log LOG = LogFactory.getLog(ParquetRecordReaderWrapper.class);

  private final long splitLen; // for getPos()

  private org.apache.hadoop.mapreduce.RecordReader<Void, ArrayWritable> realReader;
  // expect readReader return same Key & Value objects (common case)
  // this avoids extra serialization & deserialization of these objects
  private ArrayWritable valueObj = null;
  private boolean firstRecord = false;
  private boolean eof = false;
  private int schemaSize;

  private final ProjectionPusher projectionPusher;

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

    final ParquetInputSplit split = getSplit(oldSplit, oldJobConf);

    TaskAttemptID taskAttemptID = TaskAttemptID.forName(oldJobConf.get(IOConstants.MAPRED_TASK_ID));
    if (taskAttemptID == null) {
      taskAttemptID = new TaskAttemptID();
    }

    // create a TaskInputOutputContext
    final TaskAttemptContext taskContext = ContextUtil.newTaskAttemptContext(oldJobConf, taskAttemptID);

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
      if (valueObj == null) { // Should initialize the value for createValue
        valueObj = new ArrayWritable(Writable.class, new Writable[schemaSize]);
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (realReader != null) {
      realReader.close();
    }
  }

  @Override
  public Void createKey() {
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
  public boolean next(final Void key, final ArrayWritable value) throws IOException {
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

  /**
   * gets a ParquetInputSplit corresponding to a split given by Hive
   *
   * @param oldSplit The split given by Hive
   * @param conf The JobConf of the Hive job
   * @return a ParquetInputSplit corresponding to the oldSplit
   * @throws IOException if the config cannot be enhanced or if the footer cannot be read from the file
   */
  protected ParquetInputSplit getSplit(
      final InputSplit oldSplit,
      final JobConf conf
      ) throws IOException {
    ParquetInputSplit split;
    if (oldSplit instanceof FileSplit) {
      final Path finalPath = ((FileSplit) oldSplit).getPath();
      final JobConf cloneJob = projectionPusher.pushProjectionsAndFilters(conf, finalPath.getParent());

      final ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(cloneJob, finalPath);
      final List<BlockMetaData> blocks = parquetMetadata.getBlocks();
      final FileMetaData fileMetaData = parquetMetadata.getFileMetaData();

      final ReadContext readContext = new DataWritableReadSupport()
          .init(cloneJob, fileMetaData.getKeyValueMetaData(), fileMetaData.getSchema());
      schemaSize = MessageTypeParser.parseMessageType(readContext.getReadSupportMetadata()
          .get(DataWritableReadSupport.HIVE_SCHEMA_KEY)).getFieldCount();
      final List<BlockMetaData> splitGroup = new ArrayList<BlockMetaData>();
      final long splitStart = ((FileSplit) oldSplit).getStart();
      final long splitLength = ((FileSplit) oldSplit).getLength();
      for (final BlockMetaData block : blocks) {
        final long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
        if (firstDataPage >= splitStart && firstDataPage < splitStart + splitLength) {
          splitGroup.add(block);
        }
      }
      if (splitGroup.isEmpty()) {
        LOG.warn("Skipping split, could not find row group in: " + (FileSplit) oldSplit);
        split = null;
      } else {
        split = new ParquetInputSplit(finalPath,
                splitStart,
                splitLength,
                ((FileSplit) oldSplit).getLocations(),
                splitGroup,
                readContext.getRequestedSchema().toString(),
                fileMetaData.getSchema().toString(),
                fileMetaData.getKeyValueMetaData(),
                readContext.getReadSupportMetadata());
      }
    } else {
      throw new IllegalArgumentException("Unknown split type: " + oldSplit);
    }
    return split;
  }
}
