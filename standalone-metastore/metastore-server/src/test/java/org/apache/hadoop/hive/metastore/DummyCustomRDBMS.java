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

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dummy custom database product - companion class to enable testing by TestTxnUtils
 */
public class DummyCustomRDBMS extends DatabaseProduct {
  static final private Logger LOG = LoggerFactory.getLogger(DummyCustomRDBMS.class.getName());
  
  public DummyCustomRDBMS() {
    LOG.info("Instantiating custom RDBMS");
  }
  @Override
  public boolean isDeadlock(SQLException e) {
    return true;
  }
  @Override
  public boolean needsInBatching() {
    return true;
  }
  @Override
  public boolean hasJoinOperationOrderBug() {
    return true;
  }
  @Override
  public String getHiveSchemaPostfix() {
    return "DummyPostfix";
  }
  @Override
  protected String toDate(String tableValue) {
    return "DummyDate";
  }
  @Override
  public  String getPrepareTxnStmt() {
    return "DummyPrepare";
  }
  @Override
  public String getMillisAfterEpochFn() {
    return "DummyFn";
  }
  @Override
  public String getDBTime() throws MetaException {
    return super.getDBTime();
  }
  @Override
  public String isWithinCheckInterval(String expr, long intervalInSeconds) {
    return "DummyIsWithin";
  }
  @Override
  public String addForUpdateClause(String selectStatement) {
    return selectStatement + " for update";
  }
  @Override
  public String addLimitClause(int numRows, String noSelectsqlQuery) {
    return "limit " + numRows;
  }
  @Override
  public String lockTable(String txnLockTable, boolean shared) {
    return "DummyLock";
  }
  @Override
  public List<String> getResetTxnSequenceStmts() {
    return Arrays.asList(new String[]{"DummyStmt"});
  }
  @Override
  public String getTruncateStatement(String name) {
    return super.getTruncateStatement(name);
  }
  @Override
  public boolean supportsGetGeneratedKeys() {
    return true;
  }
  @Override
  public boolean isDuplicateKeyError(Throwable t) {
   return true;
  }
  @Override
  public List<String> createInsertValuesStmt(String tblColumns, List<String> rows,
      List<Integer> rowsCountInStmts, Configuration conf) {
    return Arrays.asList(new String[]{"DummyStmt"});
  }
  @Override
  public String addEscapeCharacters(String s) {
    return s;
  }
  @Override
  public Map<String, String> getDataSourceProperties() {
    return null;
  }
  @Override
  public Configuration getConf() {
    myConf.set("DummyKey", "DummyValue");
    return myConf;
  }
}
