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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/** An InputFormat for ORC files. Keys are meaningless,
 * value is the OrcStruct object */
public class OrcNewInputFormat extends InputFormat<NullWritable, OrcStruct>{
  private static final PerfLogger perfLogger = PerfLogger.getPerfLogger();
  private static final String CLASS_NAME = ReaderImpl.class.getName();

  @Override
  public RecordReader<NullWritable, OrcStruct> createRecordReader(
      InputSplit inputSplit, TaskAttemptContext context)
      throws IOException, InterruptedException {
    FileSplit fileSplit = (FileSplit) inputSplit;
    Path path = fileSplit.getPath();
    Configuration conf = ShimLoader.getHadoopShims()
        .getConfiguration(context);
    return new OrcRecordReader(OrcFile.createReader(path,
                                                   OrcFile.readerOptions(conf)),
        ShimLoader.getHadoopShims().getConfiguration(context),
        fileSplit.getStart(), fileSplit.getLength());
  }

  private static class OrcRecordReader
    extends RecordReader<NullWritable, OrcStruct> {
    private final org.apache.hadoop.hive.ql.io.orc.RecordReader reader;
    private final int numColumns;
    OrcStruct value;
    private float progress = 0.0f;

    OrcRecordReader(Reader file, Configuration conf,
                    long offset, long length) throws IOException {
      List<OrcProto.Type> types = file.getTypes();
      numColumns = (types.size() == 0) ? 0 : types.get(0).getSubtypesCount();
      value = new OrcStruct(numColumns);
      this.reader = OrcInputFormat.createReaderFromFile(file, conf, offset,
          length);
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }


    @Override
    public NullWritable getCurrentKey() throws IOException,
        InterruptedException {
      return NullWritable.get();
    }


    @Override
    public OrcStruct getCurrentValue() throws IOException,
        InterruptedException {
      return value;
    }


    @Override
    public float getProgress() throws IOException, InterruptedException {
      return progress;
    }


    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
    }


    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (reader.hasNext()) {
        reader.next(value);
        progress = reader.getProgress();
        return true;
      } else {
        return false;
      }
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext)
      throws IOException, InterruptedException {
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.ORC_GET_SPLITS);
    List<OrcSplit> splits =
        OrcInputFormat.generateSplitsInfo(ShimLoader.getHadoopShims()
        .getConfiguration(jobContext));
    List<InputSplit> result = new ArrayList<InputSplit>(splits.size());
    for(OrcSplit split: splits) {
      result.add(new OrcNewSplit(split));
    }
    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.ORC_GET_SPLITS);
    return result;
  }

}
