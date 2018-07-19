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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.metastore.messaging;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.Map;

public abstract class AlterPartitionMessage extends EventMessage {

  protected AlterPartitionMessage() {
    super(EventType.ALTER_PARTITION);
  }

  public abstract String getTable();

  public abstract String getTableType();

  public abstract boolean getIsTruncateOp();

  public abstract Map<String,String> getKeyValues();

  public abstract Table getTableObj() throws Exception;

  public abstract Partition getPtnObjBefore() throws Exception;

  public abstract Partition getPtnObjAfter() throws Exception;
  @Override
  public EventMessage checkValid() {
    if (getTable() == null) throw new IllegalStateException("Table name unset.");
    if (getKeyValues() == null) throw new IllegalStateException("Partition values unset");
    try {
      if (getTableObj() == null){
        throw new IllegalStateException("Table object not set.");
      }
      if (getPtnObjAfter() == null){
        throw new IllegalStateException("Partition object(after) not set.");
      }
      if (getPtnObjBefore() == null){
        throw new IllegalStateException("Partition object(before) not set.");
      }
    } catch (Exception e) {
      if (! (e instanceof IllegalStateException)){
        throw new IllegalStateException("Event not set up correctly",e);
      } else {
        throw (IllegalStateException) e;
      }
    }
    return super.checkValid();
  }
}

