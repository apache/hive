/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.client.builder;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.Collections;
import java.util.List;

/**
 * A builder for {@link HiveObjectRef}.  Unlike most builders (which allow a gradual building up
 * of the values) this gives a number of methods that take the object to be referenced and then
 * build the appropriate reference.  This is intended primarily for use with
 * {@link HiveObjectPrivilegeBuilder}
 */
public class HiveObjectRefBuilder {
  private HiveObjectType objectType;
  private String dbName, objectName, columnName;
  private List<String> partValues;

  public HiveObjectRef buildGlobalReference() {
    return new HiveObjectRef(HiveObjectType.GLOBAL, null, null, Collections.emptyList(), null);
  }

  public HiveObjectRef buildDatabaseReference(Database db) {
    return new
        HiveObjectRef(HiveObjectType.DATABASE, db.getName(), null, Collections.emptyList(), null);
  }

  public HiveObjectRef buildTableReference(Table table) {
    return new HiveObjectRef(HiveObjectType.TABLE, table.getDbName(), table.getTableName(),
        Collections.emptyList(), null);
  }

  public HiveObjectRef buildPartitionReference(Partition part) {
    return new HiveObjectRef(HiveObjectType.PARTITION, part.getDbName(), part.getTableName(),
        part.getValues(), null);
  }

  public HiveObjectRef buildColumnReference(Table table, String columnName) {
    return new HiveObjectRef(HiveObjectType.COLUMN, table.getDbName(), table.getTableName(),
        Collections.emptyList(), columnName);
  }

  public HiveObjectRef buildPartitionColumnReference(Table table, String columnName,
                                                     List<String> partValues) {
    return new HiveObjectRef(HiveObjectType.COLUMN, table.getDbName(), table.getTableName(),
        partValues, columnName);
  }
}
