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
  public void fullname() {
    TableName name = new TableName("cat", "db", "t");
    Assert.assertEquals("cat", name.getCat());
    Assert.assertEquals("db", name.getDb());
    Assert.assertEquals("t", name.getTable());
    Assert.assertEquals("cat.db.t", name.toString());
    Assert.assertEquals("db.t", name.getDbTable());
  }

  @Test
  public void fromString() {
    TableName name = TableName.fromString("cat.db.tab", null, null);
    Assert.assertEquals("cat", name.getCat());
    Assert.assertEquals("db", name.getDb());
    Assert.assertEquals("tab", name.getTable());

    name = TableName.fromString("db.tab", "cat", null);
    Assert.assertEquals("cat", name.getCat());
    Assert.assertEquals("db", name.getDb());
    Assert.assertEquals("tab", name.getTable());

    name = TableName.fromString("tab", "cat", "db");
    Assert.assertEquals("cat", name.getCat());
    Assert.assertEquals("db", name.getDb());
    Assert.assertEquals("tab", name.getTable());
  }
}
