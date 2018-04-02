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
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.tools.SQLGenerator;
import java.sql.Connection;
import java.util.List;

/**
 * OpenTxnEvent
 * Event generated for open transaction event.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class OpenTxnEvent extends ListenerEvent {
  private List<Long> txnIds;
  Connection connection;
  SQLGenerator sqlGenerator;

  /**
   * @param txnIds List of unique identification for the transaction just opened.
   * @param handler handler that is firing the event
   */
  public OpenTxnEvent(List<Long> txnIds, IHMSHandler handler) {
    super(true, handler);
    this.txnIds = Lists.newArrayList(txnIds);
    this.connection = null;
    this.sqlGenerator = null;
  }

  /**
   * @param txnIds List of unique identification for the transaction just opened.
   * @param connection connection to execute direct SQL statement within same transaction
   * @param sqlGenerator generates db specific SQL query
   */
  public OpenTxnEvent(List<Long> txnIds, Connection connection, SQLGenerator sqlGenerator) {
    super(true, null);
    this.txnIds = Lists.newArrayList(txnIds);
    this.connection = connection;
    this.sqlGenerator = sqlGenerator;
  }

  /**
   * @return List<Long> txnIds
   */
  public List<Long> getTxnIds() {
    return txnIds;
  }

  /**
   * @return Connection connection - used only by DbNotificationListener
   */
  public Connection getConnection() {
    return connection;
  }

  /**
   * @return SQLGenerator sqlGenerator - used only by DbNotificationListener
   */
  public SQLGenerator getSqlGenerator() {
    return sqlGenerator;
  }
}
