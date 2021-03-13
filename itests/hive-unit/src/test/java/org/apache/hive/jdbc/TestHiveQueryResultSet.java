/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.jdbc;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.junit.Test;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.thrift.EmbeddedThriftBinaryCLIService;
import org.apache.hive.service.rpc.thrift.TFetchResultsReq;
import org.apache.hive.service.rpc.thrift.TFetchResultsResp;
import org.apache.hive.service.rpc.thrift.TRowSet;
import org.apache.hive.service.rpc.thrift.TStatus;
import org.apache.hive.service.rpc.thrift.TStatusCode;
import org.apache.thrift.TException;

import static org.apache.hive.jdbc.EmbeddedCLIServicePortal.EMBEDDED_CLISERVICE;
import static org.apache.hive.jdbc.Utils.URL_PREFIX;
import static org.apache.hive.service.rpc.thrift.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10;
import static org.junit.Assert.assertEquals;

public class TestHiveQueryResultSet {
  private static final String EMIT_EMPTY_ROWS = "hive.test.emit.empty.rows";
  private static final String EMIT_NUM_ROWS = "hive.test.emit.num.rows";

  // Create subclass of EmbeddedThriftBinaryCLIService
  public static class MyThriftBinaryCLIService extends EmbeddedThriftBinaryCLIService {
    private TStatus success = new TStatus(TStatusCode.SUCCESS_STATUS);
    private boolean emitEmptyRows;
    private int numRows;
    private int position;
    private int emptyRowPos;

    @Override
    public synchronized void init(HiveConf hiveConf) {
      hiveConf.setBoolean("hive.support.concurrency", false);
      this.emitEmptyRows = hiveConf.getBoolean(EMIT_EMPTY_ROWS, false);
      this.numRows = hiveConf.getInt(EMIT_NUM_ROWS, 10);
      this.emptyRowPos = (numRows / 2);
      this.position = 0;
      super.init(hiveConf);
    }

    @Override
    public TFetchResultsResp FetchResults(TFetchResultsReq req) throws TException {
      position ++;
      TFetchResultsResp resp = new TFetchResultsResp();
      resp.setStatus(success);
      if (position > numRows) {
        resp.setHasMoreRows(false);
        resp.setResults(new TRowSet());
        return resp;
      }

      resp.setResults(getTRowSet());
      if (emitEmptyRows) {
        resp.setHasMoreRows(true);
        if (position == emptyRowPos) {
          resp.setResults(new TRowSet());
        }
      } else {
        resp.setHasMoreRows(false);
      }
      return resp;
    }

    private TRowSet getTRowSet() {
      Schema schema = new Schema();
      FieldSchema fieldSchema = new FieldSchema();
      fieldSchema.setName("_col0");
      fieldSchema.setType("string");
      schema.addToFieldSchemas(fieldSchema);
      TableSchema tableSchema = new TableSchema(schema);
      RowSet rowSet = RowSetFactory.create(tableSchema, HIVE_CLI_SERVICE_PROTOCOL_V10, false);
      String moreRows = "more rows";
      rowSet.addRow(new String[]{moreRows});
      return rowSet.toTRowSet();
    }
  }

  @Test
  public void testHasMoreRows() throws Exception {
    // 9 row + 1 empty row, ResultSet.next return 9 times
    verifyRowNum(true, 10);
    // 10 row, ResultSet.next return 10 times
    verifyRowNum(false, 10);
    // 4 row + 1 empty row, ResultSet.next return 4 times
    verifyRowNum(true, 5);
    // 5 row, ResultSet.next return 5 times
    verifyRowNum(false, 5);
  }

  private void verifyRowNum(boolean emitEmptyRows, int numRows) throws Exception {
    Properties properties = new Properties();
    properties.put("hiveconf:" + EMIT_EMPTY_ROWS, emitEmptyRows + "");
    properties.put("hiveconf:" + EMIT_NUM_ROWS, numRows + "");
    properties.put("hiveconf:" + EMBEDDED_CLISERVICE, MyThriftBinaryCLIService.class.getName());
    try (HiveConnection connection = new HiveConnection(URL_PREFIX, properties);
        Statement statement = connection.createStatement()) {
      boolean hasResult = statement.execute("select 'more rows'");
      if (hasResult) {
        int total = 0;
        try (ResultSet resultSet
            = statement.getResultSet()) {
          while (resultSet.next()) {
            String res = resultSet.getString(1);
            assertEquals("more rows", res);
            total ++;
          }
          int expectedNum = emitEmptyRows ? (numRows - 1) : numRows;
          assertEquals(expectedNum, total);
        }
      } else {
        throw new RuntimeException("The query should generate a result set");
      }
    }
  }

}
