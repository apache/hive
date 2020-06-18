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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;

import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.impala.catalog.CatalogMetastoreTestBase;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This test is similar to TestMetastoreExpr but instead of using a embedded HMS it talks
 * to the HMS service exposed from Catalog at port {@code}
 */
public class TestCatalogMetastoreExpr extends CatalogMetastoreTestBase {
  private static HiveMetaStoreClient client;

  @BeforeClass
  public static void createHMSClient() throws Exception {
    HiveConf hiveConf = new HiveConf(TestCatalogMetastoreExpr.class);
    hiveConf.set(ConfVars.METASTOREURIS.varname,
        "thrift://" + CATALOGD_HOST + ":" + CATALOGD_PORT);
    //TODO remove this once we fix CDPD-13660
    hiveConf.set(ConfVars.METASTORE_EXECUTE_SET_UGI.varname, "false");
    client = new HiveMetaStoreClient(hiveConf);
  }

  @AfterClass
  public static void closeClient() {
    if (client == null) {
      return;
    }
    client.close();
  }

  @Test
  public void testPartitionExpr() throws Exception {
    String dbName = "functional";
    String tblName = "alltypes";
    assertNotNull("Table does not exist " + dbName + "." + tblName,
        client.getTable(dbName, tblName));

    TestMetastoreExpr.ExprBuilder e = new TestMetastoreExpr.ExprBuilder(tblName);

    // table has 24 partitions for the each month of years 2009 and 2010
    // 2009 > month
    checkExpr(12, dbName, tblName, e.intCol("year").val(2010).pred(">", 2).build());
    // 12 > month
    checkExpr(22, dbName, tblName, e.intCol("month").val(12).pred(">", 2).build());
    // 9 > month
    checkExpr(16, dbName, tblName, e.intCol("month").val(9).pred(">", 2).build());
    // year > 2010
    checkExpr(0, dbName, tblName, e.val(2010).intCol("year").pred(">", 2).build());
    // month > 10
    checkExpr(4, dbName, tblName, e.val(10).intCol("month").pred(">", 2).build());
    // 2010 >= year
    checkExpr(24, dbName, tblName, e.intCol("year").val(2010).pred(">=", 2).build());
    // 0 >= month
    checkExpr(0, dbName, tblName, e.intCol("month").val(0).pred(">=", 2).build());
    // 12 >= month
    checkExpr(24, dbName, tblName, e.intCol("month").val(12).pred(">=", 2).build());
    // year >= 2010
    checkExpr(12, dbName, tblName, e.val(2010).intCol("year").pred(">=", 2).build());
    // month >= 9
    checkExpr(8, dbName, tblName, e.val(9).intCol("month").pred(">=", 2).build());
    // 1 < month
    checkExpr(22, dbName, tblName, e.intCol("month").val(1).pred("<", 2).build());
    // 0 < month
    checkExpr(24, dbName, tblName, e.intCol("month").val(0).pred("<", 2).build());
    // 2008 < year
    checkExpr(24, dbName, tblName, e.intCol("year").val(2008).pred("<", 2).build());
    // year < 2010
    checkExpr(12, dbName, tblName, e.val(2010).intCol("year").pred("<", 2).build());
    // month <= 4
    checkExpr(8, dbName, tblName, e.val(4).intCol("month").pred("<=", 2).build());
    // month <= 0
    checkExpr(0, dbName, tblName, e.val(0).intCol("month").pred("<=", 2).build());
    // year <= 2010
    checkExpr(24, dbName, tblName, e.val(2010).intCol("year").pred("<=", 2).build());
    // month <= 5
    checkExpr(10, dbName, tblName, e.val(5).intCol("month").pred("<=", 2).build());
    // (3 < month) and (year >= 2010)
    checkExpr(9, dbName, tblName, e.val(2010).intCol("year").pred(">=", 2)
        .intCol("month").val(3).pred("<", 2).pred("and", 2).build());
    //month BETWEEN 4 AND 7 (inclusive)
    checkExpr(8, dbName, tblName,
        e.val(7).val(4).intCol("month").val(false).pred("between", 4).build());
    //month BETWEEN 7 AND 4 (inclusive)
    checkExpr(0, dbName, tblName,
        e.val(4).val(7).intCol("month").val(false).pred("between", 4).build());
    // month is not between 4,7
    checkExpr(16, dbName, tblName,
        e.val(7).val(4).intCol("month").val(true).pred("between", 4).build());
    // month is not between 4,7
    checkExpr(16, dbName, tblName,
        e.val(7).val(4).intCol("month").val(true).pred("between", 4).build());
    // year = 2009 and 7 < month
    checkExpr(5, dbName, tblName, e.val(2009).intCol("year").pred("=", 2)
        .intCol("month").val(7).pred("<", 2).pred("and", 2).build());

    String strPartitionKeyTbl = "stringpartitionkey";
    // this table has one partition with key 'partition1' and another partition with
    // key '2009..'
    //(instr(string_col, 'partition') > 0)
    checkExpr(1, dbName, strPartitionKeyTbl, e.val(0).val("partition").strCol("string_col")
        .fn("instr", TypeInfoFactory.intTypeInfo, 2).pred(">", 2).build());
    //(instr(string_col, '2009') > 0)
    checkExpr(1, dbName, strPartitionKeyTbl, e.val(0).val("2009").strCol("string_col")
        .fn("instr", TypeInfoFactory.intTypeInfo, 2).pred(">", 2).build());
    checkExpr(0, dbName, strPartitionKeyTbl,
        e.strCol("string_col").pred("isnull", 1).build());
    checkExpr(2, dbName, strPartitionKeyTbl,
        e.strCol("string_col").pred("isnotnull", 1).build());
    checkExpr(2, dbName, strPartitionKeyTbl,
        e.intCol("string_col").pred("isnotnull", 1).build());

    // Cannot deserialize => throw the specific exception.
    try {
      client.listPartitionsByExpr(dbName, tblName,
          new byte[]{'f', 'o', 'o'}, null, (short) -1, new ArrayList<Partition>());
      // in remote mode this API throws an MetaException
      fail("Should have thrown MetaException");
    } catch (MetaException ignore) {
    }

    // Invalid expression => throw some exception, but not incompatible metastore.
    try {
      checkExpr(-1, dbName, tblName, e.val(31).intCol("p3").pred(">", 2).build());
      fail("Should have thrown");
    } catch (IMetaStoreClient.IncompatibleMetastoreException ignore) {
      fail("Should not have thrown IncompatibleMetastoreException");
    } catch (Exception ignore) {
    }
  }

  public void checkExpr(int numParts,
      String dbName, String tblName, ExprNodeGenericFuncDesc expr) throws Exception {
    List<Partition> parts = new ArrayList<Partition>();
    client.listPartitionsByExpr(dbName, tblName,
        SerializationUtilities.serializeExpressionToKryo(expr), null, (short) -1, parts);
    Assert.assertEquals("Partition check failed: " + expr.getExprString(), numParts,
        parts.size());
  }
}
