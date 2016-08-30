/**
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

package org.apache.hadoop.hive.ql.exec;

import static org.junit.Assert.assertEquals;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.plan.ExplainWork;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.junit.Ignore;
import org.junit.Test;

public class TestExplainTask {

  public static class DummyExplainDesc<K, V> extends TableScanDesc {
    private static final long serialVersionUID = 1L;
    private Map<K, V> explainResult;

    public DummyExplainDesc(Map<K, V> explainResult) {
      this.explainResult = explainResult;
    }

    @Explain(displayName = "test", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
    public Map<K, V> explainMethod() {
      return explainResult;
    }
  }

  public static class DummyOperator extends TableScanOperator {
    private static final long serialVersionUID = 1L;

    public DummyOperator(TableScanDesc conf) {
      super();
      setConf(conf);
    }

  }

  @Test
  public void testExplainDoesSortTopLevelMapEntries() throws Exception {
    LinkedHashMap<String, String> explainMap1 = new LinkedHashMap<>();
    explainMap1.put("/k1", "v");
    explainMap1.put("k3", "v");
    explainMap1.put("hdfs:///k2", "v");
    explainMap1.put("hdfs:///k1", "v");

    LinkedHashMap<String, String> explainMap2 = new LinkedHashMap<>();
    explainMap2.put("hdfs:///k1", "v");
    explainMap2.put("hdfs:///k2", "v");
    explainMap2.put("/k1", "v");
    explainMap2.put("k3", "v");

    String result1 = explainToString(explainMap1);
    String result2 = explainToString(explainMap2);

    assertEquals("both maps should be ordered, regardless of input order", result1, result2);
  }

  @Test
  public void testExplainDoesSortPathAsStrings() throws Exception {
    LinkedHashMap<String, String> explainMap1 = new LinkedHashMap<>();
    explainMap1.put("/k1", "v");
    explainMap1.put("k3", "v");
    explainMap1.put("hdfs:/k2", "v");
    explainMap1.put("hdfs:/k1", "v");

    LinkedHashMap<Path, String> explainMap2 = new LinkedHashMap<>();
    explainMap2.put(new Path("hdfs:/k1"), "v");
    explainMap2.put(new Path("hdfs:/k2"), "v");
    explainMap2.put(new Path("/k1"), "v");
    explainMap2.put(new Path("k3"), "v");

    String result1 = explainToString(explainMap1);
    String result2 = explainToString(explainMap2);

    assertEquals("both maps should be sorted the same way", result1, result2);
  }

  @Test
  public void testExplainDoesSortMapValues() throws Exception {
    LinkedHashMap<String, String> explainMap1Val = new LinkedHashMap<>();
    explainMap1Val.put("a", "v");
    explainMap1Val.put("b", "v");

    LinkedHashMap<String, Map<String, String>> explainMap1 = new LinkedHashMap<>();
    explainMap1.put("k", explainMap1Val);

    LinkedHashMap<String, String> explainMap2Val = new LinkedHashMap<>();
    explainMap2Val.put("b", "v");
    explainMap2Val.put("a", "v");

    LinkedHashMap<String, Map<String, String>> explainMap2 = new LinkedHashMap<>();
    explainMap2.put("k", explainMap2Val);

    String result1 = explainToString(explainMap1);
    String result2 = explainToString(explainMap2);

    assertEquals("both maps should be sorted the same way", result1, result2);
  }

  private <K, V> String explainToString(Map<K, V> explainMap) throws Exception {
    ExplainWork work = new ExplainWork();
    ParseContext pCtx = new ParseContext();
    HashMap<String, TableScanOperator> topOps = new HashMap<>();
    TableScanOperator scanOp = new DummyOperator(new DummyExplainDesc<K, V>(explainMap));
    topOps.put("sample", scanOp);
    pCtx.setTopOps(topOps);
    work.setParseContext(pCtx);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    work.setConfig(new ExplainConfiguration());
    new ExplainTask().getJSONLogicalPlan(new PrintStream(baos), work);
    baos.close();
    return baos.toString();
  }

}
