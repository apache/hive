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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.InputFormatChecker;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * A MapReduce/Hive input format for ORC files.
 */
public class VectorizedOrcInputFormat extends FileInputFormat<NullWritable, VectorizedRowBatch>
    implements InputFormatChecker, VectorizedInputFormatInterface {

  static class VectorizedOrcRecordReader
      implements RecordReader<NullWritable, VectorizedRowBatch> {
    private final org.apache.hadoop.hive.ql.io.orc.RecordReader reader;
    private final long offset;
    private final long length;
    private float progress = 0.0f;
    private VectorizedRowBatchCtx rbCtx;
    private boolean addPartitionCols = true;

    VectorizedOrcRecordReader(Reader file, Configuration conf,
        FileSplit fileSplit) throws IOException {
      List<OrcProto.Type> types = file.getTypes();
      Reader.Options options = new Reader.Options();
      this.offset = fileSplit.getStart();
      this.length = fileSplit.getLength();
      options.range(offset, length);
      OrcInputFormat.setIncludedColumns(options, types, conf, true);
      OrcInputFormat.setSearchArgument(options, types, conf, true);

      this.reader = file.rowsOptions(options);
      try {
        rbCtx = new VectorizedRowBatchCtx();
        rbCtx.init(conf, fileSplit);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean next(NullWritable key, VectorizedRowBatch value) throws IOException {

      if (!reader.hasNext()) {
        return false;
      }
      try {
        // Check and update partition cols if necessary. Ideally, this should be done
        // in CreateValue as the partition is constant per split. But since Hive uses
        // CombineHiveRecordReader and
        // as this does not call CreateValue for each new RecordReader it creates, this check is
        // required in next()
        if (addPartitionCols) {
          rbCtx.addPartitionColsToBatch(value);
          addPartitionCols = false;
        }
        reader.nextBatch(value);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      progress = reader.getProgress();
      return true;
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
      return offset + (long) (progress * length);
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }

    @Override
    public float getProgress() throws IOException {
      return progress;
    }
  }

  public VectorizedOrcInputFormat() {
    // just set a really small lower bound
    setMinSplitSize(16 * 1024);
  }

  @Override
  public RecordReader<NullWritable, VectorizedRowBatch>
      getRecordReader(InputSplit inputSplit, JobConf conf,
          Reporter reporter) throws IOException {
    FileSplit fSplit = (FileSplit)inputSplit;
    reporter.setStatus(fSplit.toString());

    Path path = fSplit.getPath();

    OrcFile.ReaderOptions opts = OrcFile.readerOptions(conf);
    if(fSplit instanceof OrcSplit){
      OrcSplit orcSplit = (OrcSplit) fSplit;
      if (orcSplit.hasFooter()) {
        opts.fileMetaInfo(orcSplit.getFileMetaInfo());
      }
    }
    Reader reader = OrcFile.createReader(path, opts);
    return new VectorizedOrcRecordReader(reader, conf, fSplit);
  }

  @Override
  public boolean validateInput(FileSystem fs, HiveConf conf,
      ArrayList<FileStatus> files
      ) throws IOException {
    if (files.size() <= 0) {
      return false;
    }
    for (FileStatus file : files) {
      try {
        OrcFile.createReader(file.getPath(),
            OrcFile.readerOptions(conf).filesystem(fs));
      } catch (IOException e) {
        return false;
      }
    }
    return true;
  }
}
