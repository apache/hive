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

package org.apache.hadoop.hive.ql.anon.btree;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.anon.ex.BtreeException;
import org.apache.hadoop.hive.ql.anon.index.PageManager;
import org.apache.hadoop.hive.ql.anon.index.api.BtreeDataEntry;
import org.apache.hadoop.hive.ql.anon.index.impl.BtreeDataEntryImpl;
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

public class BtreeOutputFormat implements HiveOutputFormat<KeyWritable, ValueWritable>
{
  private static final Logger LOG = LoggerFactory.getLogger(BtreeOutputFormat.class);
  private PageManager pageManager;

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

    pageManager = new PageManager(jc);
    return new BtreeRecWriter(finalOutPath, jc, pageManager);
  }

  private static class BtreeRecWriter implements FileSinkOperator.RecordWriter {

    private final OutputStream os;
    private final PageManager pageManager;

    BtreeRecWriter(Path path, JobConf jc, PageManager pageManager) throws IOException {

      this.pageManager = pageManager;
      FileSystem fs = path.getFileSystem(jc);
      os = fs.create(path);
    }

    @Override
    public void write(final Writable w) throws IOException {
      if (!(w instanceof KeyValueStruct)) {
        throw new BtreeException("output format");
      }

      KeyValueStruct struct = (KeyValueStruct) w;
      WritableComparable key = struct.getKey();
      Writable value = struct.getValue();

      BtreeDataEntry dataEntry = new BtreeDataEntryImpl(key, value);
      pageManager.addDataEntry(dataEntry);
    }

    @Override
    public void close(boolean abort) throws IOException {
      pageManager.save(os);
      os.close();
    }

  }

}
