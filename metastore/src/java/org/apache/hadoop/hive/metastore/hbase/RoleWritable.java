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

import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Wrapper for {@link org.apache.hadoop.hive.metastore.api.Table} that makes it writable
 */
class RoleWritable implements Writable {
  final Role role;

  RoleWritable() {
    this.role = new Role();
  }

  RoleWritable(Role role) {
    this.role = role;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    HBaseUtils.writeStr(out, role.getRoleName());
    out.writeInt(role.getCreateTime());
    HBaseUtils.writeStr(out, role.getOwnerName());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    role.setRoleName(HBaseUtils.readStr(in));
    role.setCreateTime(in.readInt());
    role.setOwnerName(HBaseUtils.readStr(in));
  }
}