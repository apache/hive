/* * Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.hadoop.hive.metastore.messaging;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import java.util.List;

/**
 * HCat message sent when an ACID write is done.
 */
public abstract class AcidWriteMessage extends EventMessage {

  protected AcidWriteMessage() {
    super(EventType.ACID_WRITE);
  }

  public abstract Long getTxnId();

  public abstract String getTable();

  public abstract Long getWriteId();

  public abstract String getPartition();

  public abstract List<String> getFiles();

  public abstract Table getTableObj() throws Exception;

  public abstract Partition getPartitionObj() throws Exception;

  public abstract String getTableObjStr();

  public abstract String getPartitionObjStr();
}
