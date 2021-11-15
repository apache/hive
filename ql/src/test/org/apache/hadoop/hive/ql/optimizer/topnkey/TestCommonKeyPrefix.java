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
package org.apache.hadoop.hive.ql.optimizer.topnkey;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Test;

/**
 * Tests for CommonKeyPrefix.
 */
public class TestCommonKeyPrefix {

  @Test
  public void testmapWhenNoKeysExists() {
    // when
    CommonKeyPrefix commonPrefix = CommonKeyPrefix.map(
            new ArrayList<>(0), "", "", new ArrayList<>(0), new HashMap<>(0), "", "");
    // then
    assertThat(commonPrefix.isEmpty(), is(true));
    assertThat(commonPrefix.size(), is(0));
    assertThat(commonPrefix.getMappedOrder(), is(""));
    assertThat(commonPrefix.getMappedNullOrder(), is(""));
    assertThat(commonPrefix.getMappedColumns().isEmpty(), is(true));
  }

  @Test
  public void testmapWhenAllKeysMatch() {
    // given
    ExprNodeColumnDesc childCol0 = exprNodeColumnDesc("_col0");
    ExprNodeColumnDesc childCol1 = exprNodeColumnDesc("_col1");
    ExprNodeColumnDesc parentCol0 = exprNodeColumnDesc("KEY._col0");
    ExprNodeColumnDesc parentCol1 = exprNodeColumnDesc("KEY._col1");
    Map<String, ExprNodeDesc> exprNodeDescMap = new HashMap<>();
    exprNodeDescMap.put("_col0", parentCol0);
    exprNodeDescMap.put("_col1", parentCol1);

    // when
    CommonKeyPrefix commonPrefix = CommonKeyPrefix.map(
            asList(childCol0, childCol1), "++", "aa", asList(parentCol0, parentCol1), exprNodeDescMap, "++", "aa");

    // then
    assertThat(commonPrefix.isEmpty(), is(false));
    assertThat(commonPrefix.size(), is(2));
    assertThat(commonPrefix.getMappedOrder(), is("++"));
    assertThat(commonPrefix.getMappedNullOrder(), is("aa"));
    assertThat(commonPrefix.getMappedColumns().get(0), is(parentCol0));
    assertThat(commonPrefix.getMappedColumns().get(1), is(parentCol1));
  }

  private ExprNodeColumnDesc exprNodeColumnDesc(String colName) {
    ExprNodeColumnDesc exprNodeColumnDesc = new ExprNodeColumnDesc();
    exprNodeColumnDesc.setColumn(colName);
    exprNodeColumnDesc.setTypeInfo(TypeInfoFactory.intTypeInfo);
    return exprNodeColumnDesc;
  }

  @Test
  public void testmapWhenOnlyFirstKeyMatchFromTwo() {
    // given
    ExprNodeColumnDesc childCol0 = exprNodeColumnDesc("_col0");
    ExprNodeColumnDesc differentChildCol = exprNodeColumnDesc("_col2");
    ExprNodeColumnDesc parentCol0 = exprNodeColumnDesc("KEY._col0");
    ExprNodeColumnDesc parentCol1 = exprNodeColumnDesc("KEY._col1");
    Map<String, ExprNodeDesc> exprNodeDescMap = new HashMap<>();
    exprNodeDescMap.put("_col0", parentCol0);
    exprNodeDescMap.put("_col1", parentCol1);

    // when
    CommonKeyPrefix commonPrefix = CommonKeyPrefix.map(
            asList(childCol0, differentChildCol), "++", "aa",
            asList(parentCol0, parentCol1), exprNodeDescMap, "++", "aa");

    // then
    assertThat(commonPrefix.isEmpty(), is(false));
    assertThat(commonPrefix.size(), is(1));
    assertThat(commonPrefix.getMappedOrder(), is("+"));
    assertThat(commonPrefix.getMappedColumns().get(0), is(parentCol0));
  }

  @Test
  public void testmapWhenAllColumnsMatchButOrderMismatch() {
    // given
    ExprNodeColumnDesc childCol0 = exprNodeColumnDesc("_col0");
    ExprNodeColumnDesc childCol1 = exprNodeColumnDesc("_col1");
    ExprNodeColumnDesc parentCol0 = exprNodeColumnDesc("KEY._col0");
    ExprNodeColumnDesc parentCol1 = exprNodeColumnDesc("KEY._col1");
    Map<String, ExprNodeDesc> exprNodeDescMap = new HashMap<>();
    exprNodeDescMap.put("_col0", parentCol0);
    exprNodeDescMap.put("_col1", parentCol1);

    // when
    CommonKeyPrefix commonPrefix = CommonKeyPrefix.map(
            asList(childCol0, childCol1), "+-", "aa", asList(parentCol0, parentCol1), exprNodeDescMap, "++", "aa");

    // then
    assertThat(commonPrefix.isEmpty(), is(false));
    assertThat(commonPrefix.size(), is(1));
    assertThat(commonPrefix.getMappedOrder(), is("+"));
    assertThat(commonPrefix.getMappedNullOrder(), is("a"));
    assertThat(commonPrefix.getMappedColumns().get(0), is(parentCol0));

    // when
    commonPrefix = CommonKeyPrefix.map(
            asList(childCol0, childCol1), "-+", "aa", asList(parentCol0, parentCol1), exprNodeDescMap, "++", "aa");

    // then
    assertThat(commonPrefix.isEmpty(), is(true));
  }

  @Test
  public void testmapWhenAllColumnsMatchButNullOrderMismatch() {
    // given
    ExprNodeColumnDesc childCol0 = exprNodeColumnDesc("_col0");
    ExprNodeColumnDesc childCol1 = exprNodeColumnDesc("_col1");
    ExprNodeColumnDesc parentCol0 = exprNodeColumnDesc("KEY._col0");
    ExprNodeColumnDesc parentCol1 = exprNodeColumnDesc("KEY._col1");
    Map<String, ExprNodeDesc> exprNodeDescMap = new HashMap<>();
    exprNodeDescMap.put("_col0", parentCol0);
    exprNodeDescMap.put("_col1", parentCol1);

    // when
    CommonKeyPrefix commonPrefix = CommonKeyPrefix.map(
            asList(childCol0, childCol1), "++", "az", asList(parentCol0, parentCol1), exprNodeDescMap, "++", "aa");

    // then
    assertThat(commonPrefix.isEmpty(), is(false));
    assertThat(commonPrefix.size(), is(1));
    assertThat(commonPrefix.getMappedOrder(), is("+"));
    assertThat(commonPrefix.getMappedNullOrder(), is("a"));
    assertThat(commonPrefix.getMappedColumns().get(0), is(parentCol0));

    // when
    commonPrefix = CommonKeyPrefix.map(
            asList(childCol0, childCol1), "++", "za", asList(parentCol0, parentCol1), exprNodeDescMap, "++", "aa");

    // then
    assertThat(commonPrefix.isEmpty(), is(true));
  }

  @Test
  public void testmapWhenKeyCountsMismatch() {
    // given
    ExprNodeColumnDesc childCol0 = exprNodeColumnDesc("_col0");
    ExprNodeColumnDesc childCol1 = exprNodeColumnDesc("_col1");
    ExprNodeColumnDesc parentCol0 = exprNodeColumnDesc("KEY._col0");
    Map<String, ExprNodeDesc> exprNodeDescMap = new HashMap<>();
    exprNodeDescMap.put("_col0", parentCol0);

    // when
    CommonKeyPrefix commonPrefix = CommonKeyPrefix.map(
            asList(childCol0, childCol1), "++", "aa", singletonList(parentCol0), exprNodeDescMap, "++", "aa");

    // then
    assertThat(commonPrefix.isEmpty(), is(false));
    assertThat(commonPrefix.size(), is(1));
    assertThat(commonPrefix.getMappedOrder(), is("+"));
    assertThat(commonPrefix.getMappedColumns().get(0), is(parentCol0));
  }
}
