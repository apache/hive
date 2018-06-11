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

public class TestDatabaseName {

  @Test
  public void differentFromConf() {
    String cat = "cat";
    String db = "db";
    DatabaseName dbName = new DatabaseName(cat, db);
    Assert.assertEquals(cat, dbName.getCat());
    Assert.assertEquals(db, dbName.getDb());
    Assert.assertEquals("cat.db", dbName.toString());
  }

  @Test
  public void fromString() {
    DatabaseName dbName = DatabaseName.fromString("cat.db", null);
    Assert.assertEquals("cat", dbName.getCat());
    Assert.assertEquals("db", dbName.getDb());
    dbName = DatabaseName.fromString("db", "cat");
    Assert.assertEquals("cat", dbName.getCat());
    Assert.assertEquals("db", dbName.getDb());
  }
}
