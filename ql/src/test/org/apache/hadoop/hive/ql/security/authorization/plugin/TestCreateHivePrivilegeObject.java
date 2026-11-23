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
package org.apache.hadoop.hive.ql.security.authorization.plugin;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivObjectActionType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.filtercontext.DatabaseFilterContext;
import org.junit.Assert;
import org.junit.Test;

/**
 * ADD JAR and other session commands authorize a COMMAND_PARAMS object.
 * RangerHiveAuthorizer.getAccessType() switches on the action type, so it must not be null.
 */
public class TestCreateHivePrivilegeObject {

  @Test
  public void testCommandParamsActionTypeIsOther() {
    HivePrivilegeObject commandObj =
        HivePrivilegeObject.createHivePrivilegeObject(Arrays.asList("jar", "/tmp/a.jar"));
    Assert.assertEquals(HivePrivilegeObjectType.COMMAND_PARAMS, commandObj.getType());
    Assert.assertEquals(HivePrivObjectActionType.OTHER, commandObj.getActionType());
    Assert.assertEquals(Arrays.asList("jar", "/tmp/a.jar"), commandObj.getCommandParams());
  }

  @Test
  public void testNullActionTypeDefaultsToOther() {
    HivePrivilegeObject scheduled = HivePrivilegeObject.forScheduledQuery("user", "ns", "job");
    Assert.assertEquals(HivePrivObjectActionType.OTHER, scheduled.getActionType());

    HivePrivilegeObject explicitNull = HivePrivilegeObject.builder(HivePrivilegeObjectType.DFS_URI)
        .objectName("hdfs://path")
        .actionType(null)
        .build();
    Assert.assertEquals(HivePrivObjectActionType.OTHER, explicitNull.getActionType());

    HivePrivilegeObject legacy = new HivePrivilegeObject(HivePrivilegeObjectType.DFS_URI, null, "hdfs://path");
    Assert.assertEquals(HivePrivObjectActionType.OTHER, legacy.getActionType());

    HivePrivilegeObject insert = HivePrivilegeObject.builder(HivePrivilegeObjectType.TABLE_OR_VIEW)
        .dbName("db")
        .objectName("tbl")
        .actionType(HivePrivObjectActionType.INSERT)
        .build();
    Assert.assertEquals(HivePrivObjectActionType.INSERT, insert.getActionType());
  }

  @Test
  public void testBuilderDefaults() {
    HivePrivilegeObject obj = HivePrivilegeObject.builder(HivePrivilegeObjectType.TABLE_OR_VIEW)
        .dbName("db")
        .objectName("tbl")
        .build();
    Assert.assertEquals(Warehouse.DEFAULT_CATALOG_NAME, obj.getCatName());
    Assert.assertEquals("db", obj.getDbname());
    Assert.assertEquals("tbl", obj.getObjectName());
    Assert.assertEquals(HivePrivObjectActionType.OTHER, obj.getActionType());
    Assert.assertEquals("Object [type=TABLE_OR_VIEW, name=hive.db.tbl]", obj.toString());

    HivePrivilegeObject blankCat = HivePrivilegeObject.builder(HivePrivilegeObjectType.TABLE_OR_VIEW)
        .catName("")
        .dbName("db")
        .objectName("tbl")
        .build();
    Assert.assertEquals(Warehouse.DEFAULT_CATALOG_NAME, blankCat.getCatName());

    HivePrivilegeObject noDb = HivePrivilegeObject.builder(HivePrivilegeObjectType.FUNCTION)
        .objectName("f")
        .build();
    Assert.assertEquals("Object [type=FUNCTION, name=f]", noDb.toString());
  }

  @Test
  public void testTablePrivilegeLookupNullCatalogMatchesDefault() {
    HivePrivilegeObject defaultCat = HivePrivilegeObject.builder(HivePrivilegeObjectType.TABLE_OR_VIEW)
        .dbName("db")
        .objectName("t1")
        .build();
    HivePrivilegeObject otherCat = HivePrivilegeObject.builder(HivePrivilegeObjectType.TABLE_OR_VIEW)
        .catName("cat1")
        .dbName("db")
        .objectName("t2")
        .build();
    HivePrivilegeObjectUtils.TablePrivilegeLookup lookup =
        new HivePrivilegeObjectUtils.TablePrivilegeLookup(Arrays.asList(defaultCat, otherCat), "hive");
    Assert.assertSame(defaultCat, lookup.lookup("hive", "db", "t1"));
    Assert.assertSame(defaultCat, lookup.lookup(null, "db", "t1"));
    Assert.assertNull(lookup.lookup("cat1", "db", "t1"));
    Assert.assertSame(otherCat, lookup.lookup("cat1", "db", "t2"));
    Assert.assertNull(lookup.lookup(null, "db", "t2"));
  }

  @Test
  public void testDatabaseFilterContextSetsDbNameAndCatalog() {
    List<HivePrivilegeObject> objs =
        new DatabaseFilterContext("cat1", Arrays.asList("db1")).getAuthzContext().getInputHObjs();
    Assert.assertEquals(1, objs.size());
    Assert.assertEquals(HivePrivilegeObjectType.DATABASE, objs.get(0).getType());
    Assert.assertEquals("cat1", objs.get(0).getCatName());
    Assert.assertEquals("db1", objs.get(0).getDbname());
    Assert.assertEquals("db1", objs.get(0).getObjectName());
  }
}
