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

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jdo.PersistenceManager;
import javax.jdo.datastore.JDOConnection;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

class DirectSqlBase {

  private static final Logger LOG = LoggerFactory.getLogger(DirectSqlBase.class.getName());

  protected final PersistenceManager pm;
  protected final DatabaseProduct dbType;
  protected final int maxBatchSize;

  DirectSqlBase(PersistenceManager pm, DatabaseProduct dbType, int batchSize) {
    this.pm = pm;
    this.dbType = dbType;
    this.maxBatchSize = batchSize;
  }

  protected void updateWithStatement(ThrowableConsumer<PreparedStatement> consumer, String query)
      throws MetaException {
    JDOConnection jdoConn = pm.getDataStoreConnection();
    boolean doTrace = LOG.isDebugEnabled();
    long start = doTrace ? System.nanoTime() : 0;
    try (PreparedStatement statement =
         ((Connection) jdoConn.getNativeConnection()).prepareStatement(query)) {
      consumer.accept(statement);
      MetastoreDirectSqlUtils.timingTrace(doTrace, query, start, doTrace ? System.nanoTime() : 0);
    } catch (SQLException e) {
      LOG.error("Failed to execute update query: " + query, e);
      throw new MetaException("Unable to execute update due to: " + e.getMessage());
    } finally {
      closeDbConn(jdoConn);
    }
  }

  protected interface ThrowableConsumer<T> {
    void accept(T t) throws SQLException, MetaException;
  }

  protected void closeDbConn(JDOConnection jdoConn) {
    try {
      if (jdoConn != null) {
        jdoConn.close();
      }
    } catch (Exception e) {
      LOG.warn("Failed to close db connection", e);
    }
  }
}
