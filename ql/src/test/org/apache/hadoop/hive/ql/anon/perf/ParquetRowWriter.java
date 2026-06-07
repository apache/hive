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
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

import java.io.IOException;


public class ParquetRowWriter implements RowWriter {

  private final FileSinkOperator.RecordWriter writer;
  private final ParquetHiveRecord record;
  private final ArrayWritable aw = new ArrayWritable(Writable.class);
  private final Writable[] row = new Writable[3];

  public ParquetRowWriter(final TestContext ctx, final Path tblFilePath, final Configuration conf) throws IOException {
    conf.set(IOConstants.COLUMNS, "m,o,b");
    conf.set(IOConstants.COLUMNS_TYPES, getTypes(ctx));

    StructObjectInspector soi = FileUtils.getOI(conf);
    writer = FileUtils.getWriter(tblFilePath, conf);
    record = new ParquetHiveRecord();
    record.inspector = soi;
  }

  @Override
  public void addRow(Writable row) throws IOException {
    record.value = row;
    writer.write(record);
  }

  public void close() throws IOException {
    writer.close(false);
  }

  public Writable createRow(final Writable msgId, final Writable offset, final Writable body) {
    row[0] = msgId;
    row[1] = offset;
    row[2] = body;
    aw.set(row);
    return aw;
  }

  private String getTypes(final TestContext ctx) {
    switch (ctx.internalFormat) {
      case JSON:
      case XML:
        return "int,bigint,string";
      case AVRO:
      case MSGPACK:
      case PROTOBUF:
        return "int,bigint,binary";
      default:
        throw new RuntimeException("unsupported type: " + ctx.internalFormat);
    }
  }
}
