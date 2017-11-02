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

public abstract class DropIndexMessage extends EventMessage {

  public abstract String getIndexName();
  public abstract String getOrigTableName();
  public abstract String getIndexTableName();

  protected DropIndexMessage() {
    super(EventType.DROP_INDEX);
  }

  @Override
  public EventMessage checkValid() {
    if (getIndexName() == null){
      throw new IllegalStateException("Index name unset.");
    }
    if (getOrigTableName() == null){
      throw new IllegalStateException("Index original table name unset.");
    }
    // NOTE: we do not do a not-null check on getIndexTableName,
    // since, per the index design wiki, it can actually be null.

    return super.checkValid();
  }

}