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

import org.apache.hadoop.hive.metastore.api.Table;

public abstract class AlterTableMessage extends EventMessage {

  protected AlterTableMessage() {
    super(EventType.ALTER_TABLE);
  }

  public abstract String getTable();

  public abstract String getTableType();

  public abstract boolean getIsTruncateOp();

  public abstract Table getTableObjBefore() throws Exception;

  public abstract Table getTableObjAfter() throws Exception;

  @Override
  public EventMessage checkValid() {
    if (getTable() == null) throw new IllegalStateException("Table name unset.");
    try {
      if (getTableObjAfter() == null){
        throw new IllegalStateException("Table object(after) not set.");
      }
      if (getTableObjBefore() == null){
        throw new IllegalStateException("Table object(before) not set.");
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

  public abstract Long getWriteId();
}
