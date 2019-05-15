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
package org.apache.hadoop.hive.ql.security;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.security.authorization.HDFSPermissionPolicyProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveResourceACLs;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test cases for privilege synchronizer for storage based authorizer
 */
public class TestHDFSPermissionPolicyProvider {
  private static MiniDFSCluster mDfs;
  private static IMetaStoreClient client;
  private static Configuration conf;
  private static String defaultTbl1Loc, defaultTbl2Loc, db1Loc, db1Tbl1Loc;

  @BeforeClass
  public static void setup() throws Exception {
    mDfs = new MiniDFSCluster.Builder(new Configuration()).numDataNodes(1).format(true).build();
    conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://" + mDfs.getNameNode().getHostAndPort());
    String warehouseLocation = "hdfs://" + mDfs.getNameNode().getHostAndPort()
        + MetastoreConf.ConfVars.WAREHOUSE.getDefaultVal();
    conf.set(MetastoreConf.ConfVars.WAREHOUSE.getVarname(), warehouseLocation);
    conf.set(MetastoreConf.ConfVars.AUTO_CREATE_ALL.getVarname(), "true");
    conf.set(MetastoreConf.ConfVars.SCHEMA_VERIFICATION.getVarname(), "false");
    client = Hive.get(conf, TestHDFSPermissionPolicyProvider.class).getMSC();

    try {
      client.dropTable("default", "tbl1");
    } catch (Exception e) {
    }
    try {
      client.dropTable("default", "tbl2");
    } catch (Exception e) {
    }
    try {
      client.dropTable("db1", "tbl1");
    } catch (Exception e) {
    }
    try {
      client.dropDatabase("db1");
    } catch (Exception e) {
    }

    defaultTbl1Loc = warehouseLocation + "/tbl1";
    defaultTbl2Loc = warehouseLocation + "/tbl2";
    db1Loc = warehouseLocation + "/db1";
    db1Tbl1Loc = warehouseLocation + "/db1/tbl1";

    int now = (int)System.currentTimeMillis() / 1000;
    FieldSchema col1 = new FieldSchema("col1", "int", "no comment");
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(col1);
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    StorageDescriptor sd =
        new StorageDescriptor(cols, defaultTbl1Loc, "input", "output", false, 0, serde, null, null,
            new HashMap<String, String>());
    Table tbl =
        new Table("tbl1", "default", "foo", now, now, 0, sd, null,
            new HashMap<String, String>(), null, null, TableType.MANAGED_TABLE.toString());
    client.createTable(tbl);

    sd = new StorageDescriptor(cols, defaultTbl2Loc, "input", "output", false, 0, serde,
        null, null, new HashMap<String, String>());
    tbl = new Table("tbl2", "default", "foo", now, now, 0, sd, null,
            new HashMap<String, String>(), null, null, TableType.MANAGED_TABLE.toString());
    client.createTable(tbl);

    Database db = new Database("db1", "no description", db1Loc, new HashMap<String, String>());
    client.createDatabase(db);

    sd = new StorageDescriptor(cols, db1Tbl1Loc, "input", "output", false, 0, serde, null, null,
            new HashMap<String, String>());
    tbl = new Table("tbl1", "db1", "foo", now, now, 0, sd, null,
            new HashMap<String, String>(), null, null, TableType.MANAGED_TABLE.toString());
    client.createTable(tbl);
  }

  @Test
  public void testPolicyProvider() throws Exception {
    HDFSPermissionPolicyProvider policyProvider = new HDFSPermissionPolicyProvider(conf);
    FileSystem fs = FileSystem.get(conf);
    fs.setOwner(new Path(defaultTbl1Loc), "user1", "group1");
    fs.setOwner(new Path(defaultTbl2Loc), "user1", "group1");
    fs.setOwner(new Path(db1Loc), "user1", "group1");
    fs.setOwner(new Path(db1Tbl1Loc), "user1", "group1");
    fs.setPermission(new Path(defaultTbl1Loc), new FsPermission("444")); // r--r--r--
    HiveResourceACLs acls = policyProvider.getResourceACLs(
        new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW, "default", "tbl1"));
    assertEquals(acls.getUserPermissions().size(), 1);
    assertTrue(acls.getUserPermissions().keySet().contains("user1"));
    assertEquals(acls.getGroupPermissions().size(), 2);
    assertTrue(acls.getGroupPermissions().keySet().contains("group1"));
    assertTrue(acls.getGroupPermissions().keySet().contains("public"));

    fs.setPermission(new Path(defaultTbl1Loc), new FsPermission("440")); // r--r-----
    acls = policyProvider.getResourceACLs(
        new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW, "default", "tbl1"));
    assertEquals(acls.getUserPermissions().size(), 1);
    assertEquals(acls.getUserPermissions().keySet().iterator().next(), "user1");
    assertEquals(acls.getGroupPermissions().size(), 1);
    assertTrue(acls.getGroupPermissions().keySet().contains("group1"));

    fs.setPermission(new Path(defaultTbl1Loc), new FsPermission("404")); // r-----r--
    acls = policyProvider.getResourceACLs(
        new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW, "default", "tbl1"));
    assertEquals(acls.getUserPermissions().size(), 1);
    assertTrue(acls.getUserPermissions().keySet().contains("user1"));
    assertEquals(acls.getGroupPermissions().size(), 1);
    assertTrue(acls.getGroupPermissions().keySet().contains("public"));

    fs.setPermission(new Path(defaultTbl1Loc), new FsPermission("400")); // r--------
    acls = policyProvider.getResourceACLs(
        new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW, "default", "tbl1"));
    assertEquals(acls.getUserPermissions().size(), 1);
    assertTrue(acls.getUserPermissions().keySet().contains("user1"));
    assertEquals(acls.getGroupPermissions().size(), 0);

    fs.setPermission(new Path(defaultTbl1Loc), new FsPermission("004")); // ------r--
    fs.setPermission(new Path(defaultTbl2Loc), new FsPermission("777")); // rwxrwxrwx
    acls = policyProvider.getResourceACLs(
        new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW, "default", "tbl1"));
    assertEquals(acls.getUserPermissions().size(), 0);
    assertEquals(acls.getGroupPermissions().size(), 1);
    assertTrue(acls.getGroupPermissions().keySet().contains("public"));
    acls = policyProvider.getResourceACLs(
        new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW, "default", "tbl2"));
    assertEquals(acls.getUserPermissions().size(), 1);
    assertTrue(acls.getUserPermissions().keySet().contains("user1"));
    assertEquals(acls.getGroupPermissions().size(), 2);
    assertTrue(acls.getGroupPermissions().keySet().contains("group1"));
    assertTrue(acls.getGroupPermissions().keySet().contains("public"));

    fs.setPermission(new Path(db1Loc), new FsPermission("400")); // ------r--
    fs.delete(new Path(db1Tbl1Loc), true);
    acls = policyProvider.getResourceACLs(
        new HivePrivilegeObject(HivePrivilegeObjectType.DATABASE, "db1", null));
    assertEquals(acls.getUserPermissions().size(), 1);
    assertTrue(acls.getUserPermissions().keySet().contains("user1"));
    assertEquals(acls.getGroupPermissions().size(), 0);
    acls = policyProvider.getResourceACLs(
        new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW, "db1", "tbl1"));
    assertEquals(acls.getUserPermissions().size(), 1);
    assertTrue(acls.getUserPermissions().keySet().contains("user1"));
    assertEquals(acls.getGroupPermissions().size(), 0);

  }
}
