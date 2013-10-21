/**
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Stack;

import junit.framework.TestCase;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;

import com.google.common.collect.Lists;

/**
 * Tests hive metastore expression support. This should be moved in metastore module
 * as soon as we are able to use ql from metastore server (requires splitting metastore
 * server and client).
 * This is a "companion" test to test to TestHiveMetaStore#testPartitionFilter; thus,
 * it doesn't test all the edge cases of the filter (if classes were merged, perhaps the
 * filter test could be rolled into it); assumption is that they use the same path in SQL/JDO.
 */
public class TestMetastoreExpr extends TestCase {
  protected static HiveMetaStoreClient client;

  @Override
  protected void tearDown() throws Exception {
    try {
      super.tearDown();
      client.close();
    } catch (Throwable e) {
      System.err.println("Unable to close metastore");
      System.err.println(StringUtils.stringifyException(e));
      throw new Exception(e);
    }
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    try {
      client = new HiveMetaStoreClient(new HiveConf(this.getClass()), null);
    } catch (Throwable e) {
      System.err.println("Unable to open the metastore");
      System.err.println(StringUtils.stringifyException(e));
      throw new Exception(e);
    }
  }

  private static void silentDropDatabase(String dbName) throws MetaException, TException {
    try {
      for (String tableName : client.getTables(dbName, "*")) {
        client.dropTable(dbName, tableName);
      }
      client.dropDatabase(dbName);
    } catch (NoSuchObjectException e) {
    } catch (InvalidOperationException e) {
    }
  }

  public void testPartitionExpr() throws Exception {
    String dbName = "filterdb";
    String tblName = "filtertbl";

    silentDropDatabase(dbName);
    Database db = new Database();
    db.setName(dbName);
    client.createDatabase(db);

    ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
    cols.add(new FieldSchema("c1", serdeConstants.STRING_TYPE_NAME, ""));
    cols.add(new FieldSchema("c2", serdeConstants.INT_TYPE_NAME, ""));
    ArrayList<FieldSchema> partCols = Lists.newArrayList(
        new FieldSchema("p1", serdeConstants.STRING_TYPE_NAME, ""),
        new FieldSchema("p2", serdeConstants.INT_TYPE_NAME, ""));

    Table tbl = new Table();
    tbl.setDbName(dbName);
    tbl.setTableName(tblName);
    addSd(cols, tbl);

    tbl.setPartitionKeys(partCols);
    client.createTable(tbl);
    tbl = client.getTable(dbName, tblName);

    addPartition(client, tbl, Lists.newArrayList("p11", "32"), "part1");
    addPartition(client, tbl, Lists.newArrayList("p12", "32"), "part2");
    addPartition(client, tbl, Lists.newArrayList("p13", "31"), "part3");
    addPartition(client, tbl, Lists.newArrayList("p14", "-33"), "part4");

    ExprBuilder e = new ExprBuilder(tblName);

    checkExpr(3, dbName, tblName, e.val(0).intCol("p2").pred(">", 2).build());
    checkExpr(3, dbName, tblName, e.intCol("p2").val(0).pred("<", 2).build());
    checkExpr(1, dbName, tblName, e.intCol("p2").val(0).pred(">", 2).build());
    checkExpr(2, dbName, tblName, e.val(31).intCol("p2").pred("<=", 2).build());
    checkExpr(3, dbName, tblName, e.val("p11").strCol("p1").pred(">", 2).build());
    checkExpr(1, dbName, tblName, e.val("p11").strCol("p1").pred(">", 2)
        .intCol("p2").val(31).pred("<", 2).pred("and", 2).build());
    checkExpr(3, dbName, tblName,
        e.val(32).val(31).intCol("p2").val(false).pred("between", 4).build());

    // Apply isnull and instr (not supported by pushdown) via name filtering.
    checkExpr(4, dbName, tblName, e.val("p").strCol("p1")
        .fn("instr", TypeInfoFactory.intTypeInfo, 2).val(0).pred("<=", 2).build());
    checkExpr(0, dbName, tblName, e.intCol("p2").pred("isnull", 1).build());

    // Cannot deserialize => throw the specific exception.
    try {
      client.listPartitionsByExpr(dbName, tblName,
          new byte[] { 'f', 'o', 'o' }, null, (short)-1, new ArrayList<Partition>());
      fail("Should have thrown IncompatibleMetastoreException");
    } catch (IMetaStoreClient.IncompatibleMetastoreException ex) {
    }

    // Invalid expression => throw some exception, but not incompatible metastore.
    try {
      checkExpr(-1, dbName, tblName, e.val(31).intCol("p3").pred(">", 2).build());
      fail("Should have thrown");
    } catch (IMetaStoreClient.IncompatibleMetastoreException ex) {
      fail("Should not have thrown IncompatibleMetastoreException");
    } catch (Exception ex) {
    }
  }

  public void checkExpr(int numParts,
      String dbName, String tblName, ExprNodeGenericFuncDesc expr) throws Exception {
    List<Partition> parts = new ArrayList<Partition>();
    client.listPartitionsByExpr(
        dbName, tblName, Utilities.serializeExpressionToKryo(expr), null, (short)-1, parts);
    assertEquals("Partition check failed: " + expr.getExprString(), numParts, parts.size());
  }

  private static class ExprBuilder {
    private final String tblName;
    private final Stack<ExprNodeDesc> stack = new Stack<ExprNodeDesc>();

    public ExprBuilder(String tblName) {
      this.tblName = tblName;
    }

    public ExprNodeGenericFuncDesc build() throws Exception {
      if (stack.size() != 1) {
        throw new Exception("Bad test: " + stack.size());
      }
      return (ExprNodeGenericFuncDesc)stack.pop();
    }

    public ExprBuilder pred(String name, int args) {
      return fn(name, TypeInfoFactory.booleanTypeInfo, args);
    }

    private ExprBuilder fn(String name, TypeInfo ti, int args) {
      List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
      for (int i = 0; i < args; ++i) {
        children.add(stack.pop());
      }
      stack.push(new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
          FunctionRegistry.getFunctionInfo(name).getGenericUDF(), children));
      return this;
    }

    public ExprBuilder strCol(String col) {
      return colInternal(TypeInfoFactory.stringTypeInfo, col, true);
    }
    public ExprBuilder intCol(String col) {
      return colInternal(TypeInfoFactory.intTypeInfo, col, true);
    }
    private ExprBuilder colInternal(TypeInfo ti, String col, boolean part) {
      stack.push(new ExprNodeColumnDesc(ti, col, tblName, part));
      return this;
    }

    public ExprBuilder val(String val) {
      return valInternal(TypeInfoFactory.stringTypeInfo, val);
    }
    public ExprBuilder val(int val) {
      return valInternal(TypeInfoFactory.intTypeInfo, val);
    }
    public ExprBuilder val(boolean val) {
      return valInternal(TypeInfoFactory.booleanTypeInfo, val);
    }
    private ExprBuilder valInternal(TypeInfo ti, Object val) {
      stack.push(new ExprNodeConstantDesc(ti, val));
      return this;
    }
  }

  private void addSd(ArrayList<FieldSchema> cols, Table tbl) {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(cols);
    sd.setCompressed(false);
    sd.setNumBuckets(1);
    sd.setParameters(new HashMap<String, String>());
    sd.setBucketCols(new ArrayList<String>());
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setName(tbl.getTableName());
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    sd.getSerdeInfo().getParameters()
        .put(serdeConstants.SERIALIZATION_FORMAT, "1");
    sd.setSortCols(new ArrayList<Order>());
    tbl.setSd(sd);
  }

  private void addPartition(HiveMetaStoreClient client, Table table,
      List<String> vals, String location) throws InvalidObjectException,
        AlreadyExistsException, MetaException, TException {

    Partition part = new Partition();
    part.setDbName(table.getDbName());
    part.setTableName(table.getTableName());
    part.setValues(vals);
    part.setParameters(new HashMap<String, String>());
    part.setSd(table.getSd());
    part.getSd().setSerdeInfo(table.getSd().getSerdeInfo());
    part.getSd().setLocation(table.getSd().getLocation() + location);

    client.add_partition(part);
  }
}
