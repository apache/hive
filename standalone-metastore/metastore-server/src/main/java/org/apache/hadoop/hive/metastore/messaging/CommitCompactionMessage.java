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

import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.WriteEventInfo;

import java.util.List;

/**
 * Message sent when a compaction commit is done.
 */
public abstract class CommitCompactionMessage extends EventMessage {

  protected CommitCompactionMessage() {
    super(EventType.COMMIT_COMPACTION);
  }

  public abstract Long getTxnId();

  public abstract Long getCompactionId();

  public abstract  CompactionType getType();

  public abstract String getDbname();

  public abstract String getTableName();

  public abstract String getPartName();
}
