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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore.events;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import java.util.List;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class AllocWriteIdEvent extends ListenerEvent {

  private final List<Long> txnIds;
  private final String tableName;

  public AllocWriteIdEvent(List<Long> txnIds, String tableName, IHMSHandler handler) {
    super(true, handler);
    this.txnIds = txnIds;
    this.tableName = tableName;
  }

  public List<Long> getTxnIds() {
    return txnIds;
  }

  public String getTableName() {
    return tableName;
  }
}
