/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.common;

import org.junit.Assert;
import org.junit.Test;

public class TestTableName {
  @Test
  public void fullName() {
    TableName name = new TableName("cat", "db", "t");
    Assert.assertEquals("cat", name.getCat());
    Assert.assertEquals("db", name.getDb());
    Assert.assertEquals("t", name.getTable());
    Assert.assertEquals("cat.db.t", name.toString());
    Assert.assertEquals("db.t", name.getDbTable());
  }

  @Test
  public void fullNameWithMetaTable() {
    TableName name = new TableName("cat", "db", "t", "meta");
    Assert.assertEquals("cat", name.getCat());
    Assert.assertEquals("db", name.getDb());
    Assert.assertEquals("t", name.getTable());
    Assert.assertEquals("meta", name.getTableMetaRef());
    Assert.assertEquals("cat.db.t", name.toString());
    Assert.assertEquals("db.t", name.getDbTable());
  }

  @Test
  public void fromString() {
    TableName name = TableName.fromString("cat.db.tab", null, null, null);
    Assert.assertEquals("cat", name.getCat());
    Assert.assertEquals("db", name.getDb());
    Assert.assertEquals("tab", name.getTable());

    name = TableName.fromString("db.tab", "cat", null, null);
    Assert.assertEquals("cat", name.getCat());
    Assert.assertEquals("db", name.getDb());
    Assert.assertEquals("tab", name.getTable());

    name = TableName.fromString("tab", "cat", "db", null);
    Assert.assertEquals("cat", name.getCat());
    Assert.assertEquals("db", name.getDb());
    Assert.assertEquals("tab", name.getTable());

    name = TableName.fromString("tab", "cat", "db", "metatable");
    Assert.assertEquals("cat", name.getCat());
    Assert.assertEquals("db", name.getDb());
    Assert.assertEquals("tab", name.getTable());
    Assert.assertEquals("metatable", name.getTableMetaRef());

    try {
      TableName.fromString(null, null, null, null);
      Assert.fail("Name can't be null");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testNotEmptyDbTable() {
    TableName name = new TableName("cat", "db", "t");
    Assert.assertEquals("db.t", name.getNotEmptyDbTable());

    name = new TableName("cat", null, "t");
    Assert.assertEquals("t", name.getNotEmptyDbTable());

    name = new TableName("cat", "", "t");
    Assert.assertEquals("t", name.getNotEmptyDbTable());
  }
}
