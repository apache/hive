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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.metastore.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Wrapper for {@link org.apache.hadoop.hive.metastore.api.Table} that makes it writable
 */
class TableWritable implements Writable {
  static final private Log LOG = LogFactory.getLog(TableWritable.class.getName());
  final Table table;

  TableWritable() {
    this.table = new Table();
  }

  TableWritable(Table table) {
    this.table = table;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    HBaseUtils.writeStr(out, table.getTableName());
    HBaseUtils.writeStr(out, table.getDbName());
    HBaseUtils.writeStr(out, table.getOwner());
    out.writeInt(table.getCreateTime());
    out.writeInt(table.getLastAccessTime());
    out.writeInt(table.getRetention());
    new StorageDescriptorWritable(table.getSd()).write(out);
    HBaseUtils.writeFieldSchemaList(out, table.getPartitionKeys());
    HBaseUtils.writeStrStrMap(out, table.getParameters());
    HBaseUtils.writeStr(out, table.getViewOriginalText());
    HBaseUtils.writeStr(out, table.getViewExpandedText());
    HBaseUtils.writeStr(out, table.getTableType());
    HBaseUtils.writePrivileges(out, table.getPrivileges());
    out.writeBoolean(table.isTemporary());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    table.setTableName(HBaseUtils.readStr(in));
    table.setDbName(HBaseUtils.readStr(in));
    table.setOwner(HBaseUtils.readStr(in));
    table.setCreateTime(in.readInt());
    table.setLastAccessTime(in.readInt());
    table.setRetention(in.readInt());
    StorageDescriptorWritable sdw = new StorageDescriptorWritable();
    sdw.readFields(in);
    table.setSd(sdw.sd);
    table.setPartitionKeys(HBaseUtils.readFieldSchemaList(in));
    table.setParameters(HBaseUtils.readStrStrMap(in));
    table.setViewOriginalText(HBaseUtils.readStr(in));
    table.setViewExpandedText(HBaseUtils.readStr(in));
    table.setTableType(HBaseUtils.readStr(in));
    table.setPrivileges(HBaseUtils.readPrivileges(in));
    table.setTemporary(in.readBoolean());
  }
}
