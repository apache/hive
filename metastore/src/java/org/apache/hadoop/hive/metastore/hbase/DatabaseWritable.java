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

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Wrapper for {@link org.apache.hadoop.hive.metastore.api.Database} that makes it writable
 */
class DatabaseWritable implements Writable {
  final Database db;

  DatabaseWritable() {
    this.db = new Database();
  }

  DatabaseWritable(Database db) {
    this.db = db;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    HBaseUtils.writeStr(out, db.getName());
    HBaseUtils.writeStr(out, db.getDescription());
    HBaseUtils.writeStr(out, db.getLocationUri());
    HBaseUtils.writeStrStrMap(out, db.getParameters());
    HBaseUtils.writePrivileges(out, db.getPrivileges());
    HBaseUtils.writeStr(out, db.getOwnerName());
    HBaseUtils.writePrincipalType(out, db.getOwnerType());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    db.setName(HBaseUtils.readStr(in));
    db.setDescription(HBaseUtils.readStr(in));
    db.setLocationUri(HBaseUtils.readStr(in));
    db.setParameters(HBaseUtils.readStrStrMap(in));
    db.setPrivileges(HBaseUtils.readPrivileges(in));
    db.setOwnerName(HBaseUtils.readStr(in));
    db.setOwnerType(HBaseUtils.readPrincipalType(in));
  }
}
