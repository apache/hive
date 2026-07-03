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

package org.apache.hadoop.hive.ql.anon.perf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hive.ql.anon.utils.FileUtils;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.serde2.io.ParquetHiveRecord;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TestParquetRdWr {

  private static final Logger LOG = LoggerFactory.getLogger(TestParquetRdWr.class);

  @Test
  public void test() throws IOException, InterruptedException {

    JobConf conf = new JobConf();
    Path path = new Path("/opt/hive/t_overwrite_parquet/000000_0");
    path = new Path("/opt/hive/t3.p");

    RecordReader<NullWritable, ArrayWritable> reader = FileUtils.getReader(path, conf);
    NullWritable key = reader.createKey();
    ArrayWritable value = reader.createValue();
    while (reader.next(key, value)) {
      LOG.info("{}", value);
    }
    reader.close();
  }

  @Test
  public void testWrite() throws IOException {
    JobConf jobConf = new JobConf();

    Path path = new Path("/opt/hive/t3.p");
    FileUtils.deleteIfExists(path, jobConf);

    Configuration conf = new Configuration();
    conf.set(IOConstants.COLUMNS, "i,j,k,s");
    conf.set(IOConstants.COLUMNS_TYPES, "int:int:bigint:string");

    StructObjectInspector soi = FileUtils.getOI(conf);
    FileSinkOperator.RecordWriter writer = FileUtils.getWriter(path, conf);

    ParquetHiveRecord value = new ParquetHiveRecord();
    value.inspector = soi;

    ArrayWritable aw = new ArrayWritable(Writable.class);
    for (int i = 0; i < 3; i++) {
      Writable[] objects = new Writable[4];
      objects[0] = new IntWritable(i);
      objects[1] = new IntWritable(i + 10);
      objects[2] = new LongWritable(i + 100);
      objects[3] = new Text("t" + i);
      aw.set(objects);
      value.value = aw;
      writer.write(value);
    }
    writer.close(false);
  }

}
