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

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Wrapper for {@link org.apache.hadoop.hive.metastore.api.Table} that makes it writable
 */
class PartitionWritable implements Writable {
  final Partition part;

  PartitionWritable() {
    this.part = new Partition();
  }

  PartitionWritable(Partition part) {
    this.part = part;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    HBaseUtils.writeStrList(out, part.getValues());
    // TODO should be able to avoid dbname and tablename since they're in the key
    HBaseUtils.writeStr(out, part.getDbName());
    HBaseUtils.writeStr(out, part.getTableName());
    out.writeInt(part.getCreateTime());
    out.writeInt(part.getLastAccessTime());
    new StorageDescriptorWritable(part.getSd()).write(out);
    HBaseUtils.writeStrStrMap(out, part.getParameters());
    HBaseUtils.writePrivileges(out, part.getPrivileges());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    part.setValues(HBaseUtils.readStrList(in));
    part.setDbName(HBaseUtils.readStr(in));
    part.setTableName(HBaseUtils.readStr(in));
    part.setCreateTime(in.readInt());
    part.setLastAccessTime(in.readInt());
    StorageDescriptorWritable sdw = new StorageDescriptorWritable();
    sdw.readFields(in);
    part.setSd(sdw.sd);
    part.setParameters(HBaseUtils.readStrStrMap(in));
    part.setPrivileges(HBaseUtils.readPrivileges(in));
  }
}
