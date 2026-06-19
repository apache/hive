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
package org.apache.hadoop.hive.metastore.txn.jdbc.functions;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Statement;

public class AcquireTxnLockFunction implements TransactionalFunction<Void> {

  private static final Logger LOG = LoggerFactory.getLogger(AcquireTxnLockFunction.class.getName());
  
  private final boolean shared;

  public AcquireTxnLockFunction(boolean shared) {
    this.shared = shared;
  }

  @Override
  public Void execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    String sqlStmt = jdbcResource.getSqlGenerator().createTxnLockStatement(shared);
    jdbcResource.getJdbcTemplate().getJdbcTemplate().execute((Statement stmt) -> {
      stmt.execute(sqlStmt);
      return null;
    });
    LOG.debug("TXN lock locked by '{}' in mode {}", JavaUtils.hostname(), shared);
    return null;
  }
}
