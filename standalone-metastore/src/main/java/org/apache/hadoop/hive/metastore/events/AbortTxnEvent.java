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
import org.apache.hadoop.hive.metastore.tools.SQLGenerator;
import java.sql.Connection;

/**
 * AbortTxnEvent
 * Event generated for roll backing a transaction
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class AbortTxnEvent extends ListenerEvent {

  private final Long txnId;
  Connection connection;
  SQLGenerator sqlGenerator;

  /**
   *
   * @param transactionId Unique identification for the transaction that got rolledback.
   * @param handler handler that is firing the event
   */
  public AbortTxnEvent(Long transactionId, IHMSHandler handler) {
    super(true, handler);
    txnId = transactionId;
    connection = null;
    sqlGenerator = null;
  }

  /**
   * @param transactionId Unique identification for the transaction just got aborted.
   * @param connection connection to execute direct SQL statement within same transaction
   * @param sqlGenerator generates db specific SQL query
   */
  public AbortTxnEvent(Long transactionId, Connection connection, SQLGenerator sqlGenerator) {
    super(true, null);
    this.txnId = transactionId;
    this.connection = connection;
    this.sqlGenerator = sqlGenerator;
  }

  /**
   * @return Long txnId
   */
  public Long getTxnId() {
    return txnId;
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
