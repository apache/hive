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

package org.apache.hive.hplsql;

import org.junit.Assert;
import org.junit.Test;

public class TestColumnMap {

  @Test
  public void testShouldReturnColumnWhenOnlyColumnNameIsPassed() {
    ColumnMap columnMap = new ColumnMap();
    Var var = new Var("Value");
    Column column1 = new Column("tab.col1", "string", var);
    Column column2 = new Column("schema.tab.col2", "string", var);
    columnMap.add(column1);
    columnMap.add(column2);
    Assert.assertEquals(column1, columnMap.get("col1"));
    Assert.assertEquals(column2, columnMap.get("col2"));
  }
}
