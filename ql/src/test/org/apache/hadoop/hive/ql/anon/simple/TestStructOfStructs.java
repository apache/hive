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
import org.junit.jupiter.api.Test;

import java.util.List;

public class TestStructOfStructs extends BaseTest {

  @BeforeAll
  public void setup() throws CommandProcessorException {
    tblName = "t_struct_of_structs";
    create();
    truncate();
    insert();
  }

  public void create() throws CommandProcessorException {
    execute("drop table if exists %s");
    execute("create table if not exists %s(" +
      "s struct<s1:struct<b:string>, s2:struct<m:int,o:bigint,f:string>, s3:struct<p:int>>) " +
      "stored as orc");
  }

  public void insert() throws CommandProcessorException {
    execute("insert into table %s values (named_struct(" +
      "'s1', named_struct('b', 'body'), " +
      "'s2', named_struct('m', 1, 'o', 2L, 'f', 'file'), " +
      "'s3', named_struct('p', 99))" +
      ")");
  }

  @Test
  public void testSelect() throws CommandProcessorException {
    List<Object> lst = execute("select * from %s as t");
    Assertions.assertEquals(1, lst.size());
  }
}
