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

import java.io.IOException;
import org.apache.crunch.types.orc.OrcUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.ColumnInternalFormat;
import org.apache.hadoop.hive.ql.anon.utils.FileUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
public class OrcRowWriter implements RowWriter {

  private final TypeInfo typeInfo;
  private final Writer writer;

  public OrcRowWriter(final TestContext ctx, final Path tblFilePath, final Configuration conf) {
    final String orcStruct = getStruct(ctx.internalFormat);
    final TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(orcStruct);
    final ObjectInspector oi = OrcStruct.createObjectInspector(typeInfo);

    this.typeInfo = typeInfo;
    writer = FileUtils.getWriter(tblFilePath, conf, oi);
  }

  @Override
  public void addRow(Writable row) throws IOException {
    writer.addRow(row);
  }

  public void close() throws IOException {
    writer.writeIntermediateFooter();
    writer.close();
  }

  public Writable createRow(final Writable msgId, final Writable offset, final Writable body) {
    return OrcUtils.createOrcStruct(typeInfo, msgId, offset, body);
  }

  private String getStruct(final ColumnInternalFormat internalFormat) {
    switch (internalFormat) {
      case JSON:
      case XML:
        return "struct<m:int, o:bigint, b:string>";
      case MSGPACK:
      case PROTOBUF:
      case AVRO:
        return "struct<m:int, o:bigint, b:binary>";
      default:
        throw new RuntimeException("Unrecognized internal format: " + internalFormat);
    }
  }

}
