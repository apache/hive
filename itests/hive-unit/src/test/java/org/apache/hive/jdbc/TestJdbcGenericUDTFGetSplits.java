/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.jdbc;

import org.apache.hadoop.hive.llap.FieldDesc;
import org.apache.hadoop.hive.llap.LlapBaseInputFormat;
import org.apache.hadoop.hive.llap.LlapInputSplit;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * TestJdbcGenericUDTFGetSplits.
 */
public class TestJdbcGenericUDTFGetSplits extends AbstractTestJdbcGenericUDTFGetSplits {

  @Test(timeout = 200000)
  public void testGenericUDTFOrderBySplitCount1() throws Exception {
    super.testGenericUDTFOrderBySplitCount1("get_splits", new int[]{10, 1, 0, 1, 10});
  }

  @Test
  public void testDecimalPrecisionAndScale() throws Exception {
    try (Statement stmt = hs2Conn.createStatement()) {
      stmt.execute("CREATE TABLE decimal_test_table(decimal_col DECIMAL(6,2))");
      stmt.execute("INSERT INTO decimal_test_table VALUES(2507.92)");

      ResultSet rs = stmt.executeQuery("SELECT * FROM decimal_test_table");
      assertTrue(rs.next());
      rs.close();

      String url = miniHS2.getJdbcURL();
      String user = System.getProperty("user.name");
      String pwd = user;
      String handleId = UUID.randomUUID().toString();
      String sql = "SELECT avg(decimal_col)/3 FROM decimal_test_table";

      // make request through llap-ext-client
      JobConf job = new JobConf(conf);
      job.set(LlapBaseInputFormat.URL_KEY, url);
      job.set(LlapBaseInputFormat.USER_KEY, user);
      job.set(LlapBaseInputFormat.PWD_KEY, pwd);
      job.set(LlapBaseInputFormat.QUERY_KEY, sql);
      job.set(LlapBaseInputFormat.HANDLE_ID, handleId);

      LlapBaseInputFormat llapBaseInputFormat = new LlapBaseInputFormat();
      //schema split
      LlapInputSplit schemaSplit = (LlapInputSplit) llapBaseInputFormat.getSplits(job, 0)[0];
      assertNotNull(schemaSplit);
      FieldDesc fieldDesc = schemaSplit.getSchema().getColumns().get(0);
      DecimalTypeInfo type = (DecimalTypeInfo) fieldDesc.getTypeInfo();
      assertEquals(38, type.getPrecision());
      assertEquals(24, type.scale());

      LlapBaseInputFormat.close(handleId);
    }
  }

}