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

package org.apache.hadoop.hive.ql.anon.simple;

import org.apache.hadoop.hive.ql.anon.BaseTest;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.List;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestStoredAsOrcAsSelectFromTable extends BaseTest {

  @BeforeAll
  public void setup() throws CommandProcessorException {
    tblName = "t_orc_as_select";
    prepTemp();
  }

  public void prepTemp() throws CommandProcessorException {
    execute("drop table if exists temp_orc");
    execute("create table if not exists temp_orc(a int, b int)");
    execute("truncate table temp_orc");
    execute("insert into table temp_orc values(1, 2),(3, 4)");
  }

  @Order(1)
  @Test
  public void create() throws CommandProcessorException {
    execute("drop table if exists %s");
    execute("create table %s stored as orc as select a, b from temp_orc");
  }

  @Order(2)
  @Test
  public void testSelect() throws CommandProcessorException {
    List<Object> lst = execute("select * from %s");
    Assertions.assertEquals(2, lst.size());
  }
}
