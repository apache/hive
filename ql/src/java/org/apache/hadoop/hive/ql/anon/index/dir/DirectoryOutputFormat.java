/*
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

package org.apache.hadoop.hive.ql.anon.index.dir;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.anon.ex.AnonIndexException;
import org.apache.hadoop.hive.ql.anon.btree.KeyValueStruct;
import org.apache.hadoop.hive.ql.anon.btree.KeyWritable;
import org.apache.hadoop.hive.ql.anon.btree.ValueWritable;
import org.apache.hadoop.hive.ql.anon.index.RawDataEntry;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

import static org.apache.hadoop.hive.ql.anon.index.Converters.convert;

public class DirectoryOutputFormat implements HiveOutputFormat<KeyWritable, ValueWritable>
{
  private static final Logger LOG = LoggerFactory.getLogger(DirectoryOutputFormat.class);
  private DirectoryIndex directoryIndex;

  @Override
  public RecordWriter<KeyWritable, ValueWritable> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress) throws IOException {
    return null;
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {

  }

  @Override
  public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath, Class<? extends Writable> valueClass,
                                                           boolean isCompressed, Properties tableProperties, Progressable progress) throws IOException {

    directoryIndex = new DirectoryIndex(jc);
    return new BtreeRecWriter(finalOutPath, jc, directoryIndex);
  }

  private static class BtreeRecWriter implements FileSinkOperator.RecordWriter {

    private final OutputStream os;
    private final DirectoryIndex index;

    BtreeRecWriter(Path path, JobConf jc, DirectoryIndex index) throws IOException {

      this.index = index;
      FileSystem fs = path.getFileSystem(jc);
      os = fs.create(path);
    }

    @Override
    public void write(final Writable w) throws IOException {
      if (!(w instanceof KeyValueStruct)) {
        throw new AnonIndexException("output format");
      }

      KeyValueStruct struct = (KeyValueStruct) w;
      WritableComparable wkey = struct.getKey();
      Writable wvalue = struct.getValue();

      byte[] key = convert(wkey);
      byte[] value = convert(wvalue);
      RawDataEntry dataEntry = new RawDataEntry(key, value);
      index.addEntry(dataEntry);
    }

    @Override
    public void close(boolean abort) throws IOException {
      index.save(os);
      os.close();
    }

  }

}
