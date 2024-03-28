/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.hooks;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.optimizer.calcite.CommonTableExpressionEmptySuggester;
import org.apache.hadoop.hive.ql.optimizer.calcite.CommonTableExpressionIdentitySuggester;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestCommonTableExpressionAutoTuningHook {

  private final String query;
  private final String suggester;

  public TestCommonTableExpressionAutoTuningHook(final String query, final String suggester) {
    this.query = query;
    this.suggester = suggester;
  }

  @Test
  public void testBeforeCompileSetsConf() {
    CommonTableExpressionAutoTuningHook h = new CommonTableExpressionAutoTuningHook();
    HiveConf c = new HiveConf();
    h.beforeCompile(new QueryLifeTimeHookContextImpl.Builder().withHiveConf(c).withCommand(query).build());
    assertEquals(query, suggester, c.getVar(HiveConf.ConfVars.HIVE_CTE_SUGGESTER_CLASS));
  }

  @Parameterized.Parameters(name = "query={0}, suggester={1}")
  public static List<Object[]> queries() {
    List<Object[]> queries = new ArrayList<>();
    String idSuggester = CommonTableExpressionIdentitySuggester.class.getCanonicalName();
    String emptySuggester = CommonTableExpressionEmptySuggester.class.getCanonicalName();
    queries.add(new Object[] { "SELECT * FROM table", idSuggester });
    queries.add(new Object[] { "SELECT * FROM withtable as wt", idSuggester });
    queries.add(new Object[] { "WITH q1 AS (SELECT * FROM table)", emptySuggester });
    queries.add(new Object[] { "with q1 AS (SELECT * FROM table)", emptySuggester });
    queries.add(new Object[] { "WITH\n q1 AS (SELECT * FROM table)", emptySuggester });
    queries.add(new Object[] { "\nWITH\n q1 AS (SELECT * FROM table)", emptySuggester });
    queries.add(new Object[] { "\nWITH q1 AS (SELECT * FROM table)", emptySuggester });
    return queries;
  }
}
