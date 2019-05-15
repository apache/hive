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
package org.apache.hadoop.hive.ql.plan;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Before;
import org.junit.Test;

public class TestCreateMacroDesc {
  private String name;
  private List<String> colNames;
  private List<TypeInfo> colTypes;
  private ExprNodeConstantDesc bodyDesc;
  @Before
  public void setup() throws Exception {
    name = "fixed_number";
    colNames = new ArrayList<String>();
    colTypes = new ArrayList<TypeInfo>();
    colNames.add("x");
    colTypes.add(TypeInfoFactory.intTypeInfo);
    colNames.add("y");
    colTypes.add(TypeInfoFactory.intTypeInfo);
    bodyDesc = new ExprNodeConstantDesc(1);
  }
  @Test
  public void testCreateMacroDesc() throws Exception {
    CreateMacroDesc desc = new CreateMacroDesc(name, colNames, colTypes, bodyDesc);
    Assert.assertEquals(name, desc.getMacroName());
    Assert.assertEquals(bodyDesc, desc.getBody());
    Assert.assertEquals(colNames, desc.getColNames());
    Assert.assertEquals(colTypes, desc.getColTypes());
  }
}
