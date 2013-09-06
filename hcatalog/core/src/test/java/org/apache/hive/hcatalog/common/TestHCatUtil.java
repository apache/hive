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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.junit.Assert;
import org.junit.Test;

public class TestHCatUtil {

  @Test
  public void testFsPermissionOperation() {

    HashMap<String, Integer> permsCode = new HashMap<String, Integer>();

    for (int i = 0; i < 8; i++) {
      for (int j = 0; j < 8; j++) {
        for (int k = 0; k < 8; k++) {
          StringBuilder sb = new StringBuilder();
          sb.append("0");
          sb.append(i);
          sb.append(j);
          sb.append(k);
          Integer code = (((i * 8) + j) * 8) + k;
          String perms = (new FsPermission(Short.decode(sb.toString()))).toString();
          if (permsCode.containsKey(perms)) {
            Assert.assertEquals("permissions(" + perms + ") mapped to multiple codes", code, permsCode.get(perms));
          }
          permsCode.put(perms, code);
          assertFsPermissionTransformationIsGood(perms);
        }
      }
    }
  }

  private void assertFsPermissionTransformationIsGood(String perms) {
    Assert.assertEquals(perms, FsPermission.valueOf("-" + perms).toString());
  }

  @Test
  public void testValidateMorePermissive() {
    assertConsistentFsPermissionBehaviour(FsAction.ALL, true, true, true, true, true, true, true, true);
    assertConsistentFsPermissionBehaviour(FsAction.READ, false, true, false, true, false, false, false, false);
    assertConsistentFsPermissionBehaviour(FsAction.WRITE, false, true, false, false, true, false, false, false);
    assertConsistentFsPermissionBehaviour(FsAction.EXECUTE, false, true, true, false, false, false, false, false);
    assertConsistentFsPermissionBehaviour(FsAction.READ_EXECUTE, false, true, true, true, false, true, false, false);
    assertConsistentFsPermissionBehaviour(FsAction.READ_WRITE, false, true, false, true, true, false, true, false);
    assertConsistentFsPermissionBehaviour(FsAction.WRITE_EXECUTE, false, true, true, false, true, false, false, true);
    assertConsistentFsPermissionBehaviour(FsAction.NONE, false, true, false, false, false, false, false, false);
  }


  private void assertConsistentFsPermissionBehaviour(
      FsAction base, boolean versusAll, boolean versusNone,
      boolean versusX, boolean versusR, boolean versusW,
      boolean versusRX, boolean versusRW, boolean versusWX) {

    Assert.assertTrue(versusAll == HCatUtil.validateMorePermissive(base, FsAction.ALL));
    Assert.assertTrue(versusX == HCatUtil.validateMorePermissive(base, FsAction.EXECUTE));
    Assert.assertTrue(versusNone == HCatUtil.validateMorePermissive(base, FsAction.NONE));
    Assert.assertTrue(versusR == HCatUtil.validateMorePermissive(base, FsAction.READ));
    Assert.assertTrue(versusRX == HCatUtil.validateMorePermissive(base, FsAction.READ_EXECUTE));
    Assert.assertTrue(versusRW == HCatUtil.validateMorePermissive(base, FsAction.READ_WRITE));
    Assert.assertTrue(versusW == HCatUtil.validateMorePermissive(base, FsAction.WRITE));
    Assert.assertTrue(versusWX == HCatUtil.validateMorePermissive(base, FsAction.WRITE_EXECUTE));
  }

  @Test
  public void testExecutePermissionsCheck() {
    Assert.assertTrue(HCatUtil.validateExecuteBitPresentIfReadOrWrite(FsAction.ALL));
    Assert.assertTrue(HCatUtil.validateExecuteBitPresentIfReadOrWrite(FsAction.NONE));
    Assert.assertTrue(HCatUtil.validateExecuteBitPresentIfReadOrWrite(FsAction.EXECUTE));
    Assert.assertTrue(HCatUtil.validateExecuteBitPresentIfReadOrWrite(FsAction.READ_EXECUTE));
    Assert.assertTrue(HCatUtil.validateExecuteBitPresentIfReadOrWrite(FsAction.WRITE_EXECUTE));

    Assert.assertFalse(HCatUtil.validateExecuteBitPresentIfReadOrWrite(FsAction.READ));
    Assert.assertFalse(HCatUtil.validateExecuteBitPresentIfReadOrWrite(FsAction.WRITE));
    Assert.assertFalse(HCatUtil.validateExecuteBitPresentIfReadOrWrite(FsAction.READ_WRITE));

  }

  @Test
  public void testGetTableSchemaWithPtnColsApi() throws IOException {
    // Check the schema of a table with one field & no partition keys.
    StorageDescriptor sd = new StorageDescriptor(
        Lists.newArrayList(new FieldSchema("username", serdeConstants.STRING_TYPE_NAME, null)),
        "location", "org.apache.hadoop.mapred.TextInputFormat",
        "org.apache.hadoop.mapred.TextOutputFormat", false, -1, new SerDeInfo(),
        new ArrayList<String>(), new ArrayList<Order>(), new HashMap<String, String>());
    org.apache.hadoop.hive.metastore.api.Table apiTable =
        new org.apache.hadoop.hive.metastore.api.Table("test_tblname", "test_dbname", "test_owner",
            0, 0, 0, sd, new ArrayList<FieldSchema>(), new HashMap<String, String>(),
            "viewOriginalText", "viewExpandedText", TableType.EXTERNAL_TABLE.name());
    Table table = new Table(apiTable);

    List<HCatFieldSchema> expectedHCatSchema =
        Lists.newArrayList(new HCatFieldSchema("username", HCatFieldSchema.Type.STRING, null));

    Assert.assertEquals(new HCatSchema(expectedHCatSchema),
        HCatUtil.getTableSchemaWithPtnCols(table));

    // Add a partition key & ensure its reflected in the schema.
    List<FieldSchema> partitionKeys =
        Lists.newArrayList(new FieldSchema("dt", serdeConstants.STRING_TYPE_NAME, null));
    table.getTTable().setPartitionKeys(partitionKeys);
    expectedHCatSchema.add(new HCatFieldSchema("dt", HCatFieldSchema.Type.STRING, null));
    Assert.assertEquals(new HCatSchema(expectedHCatSchema),
        HCatUtil.getTableSchemaWithPtnCols(table));
  }

  /**
   * Hive represents tables in two ways:
   * <ul>
   *   <li>org.apache.hadoop.hive.metastore.api.Table - exactly whats stored in the metastore</li>
   *   <li>org.apache.hadoop.hive.ql.metadata.Table - adds business logic over api.Table</li>
   * </ul>
   * Here we check SerDe-reported fields are included in the table schema.
   */
  @Test
  public void testGetTableSchemaWithPtnColsSerDeReportedFields() throws IOException {
    Map<String, String> parameters = Maps.newHashMap();
    parameters.put(serdeConstants.SERIALIZATION_CLASS,
        "org.apache.hadoop.hive.serde2.thrift.test.IntString");
    parameters.put(serdeConstants.SERIALIZATION_FORMAT, "org.apache.thrift.protocol.TBinaryProtocol");

    SerDeInfo serDeInfo = new SerDeInfo(null,
        "org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer", parameters);

    // StorageDescriptor has an empty list of fields - SerDe will report them.
    StorageDescriptor sd = new StorageDescriptor(new ArrayList<FieldSchema>(), "location",
        "org.apache.hadoop.mapred.TextInputFormat", "org.apache.hadoop.mapred.TextOutputFormat",
        false, -1, serDeInfo, new ArrayList<String>(), new ArrayList<Order>(),
        new HashMap<String, String>());

    org.apache.hadoop.hive.metastore.api.Table apiTable =
        new org.apache.hadoop.hive.metastore.api.Table("test_tblname", "test_dbname", "test_owner",
            0, 0, 0, sd, new ArrayList<FieldSchema>(), new HashMap<String, String>(),
            "viewOriginalText", "viewExpandedText", TableType.EXTERNAL_TABLE.name());
    Table table = new Table(apiTable);

    List<HCatFieldSchema> expectedHCatSchema = Lists.newArrayList(
        new HCatFieldSchema("myint", HCatFieldSchema.Type.INT, null),
        new HCatFieldSchema("mystring", HCatFieldSchema.Type.STRING, null),
        new HCatFieldSchema("underscore_int", HCatFieldSchema.Type.INT, null));

    Assert.assertEquals(new HCatSchema(expectedHCatSchema),
        HCatUtil.getTableSchemaWithPtnCols(table));
  }
}
