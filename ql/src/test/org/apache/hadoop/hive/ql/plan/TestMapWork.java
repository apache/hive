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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.mapred.JobConf;

import org.junit.Test;

import com.google.common.collect.Lists;

public class TestMapWork {
  @Test
  public void testGetAndSetConsistency() {
    MapWork mw = new MapWork();
    Map<Path, List<String>> pathToAliases = new LinkedHashMap<>();
    pathToAliases.put(new Path("p0"), Lists.newArrayList("a1", "a2"));
    mw.setPathToAliases(pathToAliases);

    Map<Path, List<String>> pta = mw.getPathToAliases();
    assertEquals(pathToAliases, pta);

  }

  @Test
  public void testPath() {
    Path p1 = new Path("hdfs://asd/asd");
    Path p2 = new Path("hdfs://asd/asd/");

    assertEquals(p1, p2);
  }

  @Test
  public void testDeriveLlapSetsCacheAffinityForTextInputFormat() {
    MapWork mapWork = new MapWork();
    PartitionDesc partitionDesc = new PartitionDesc();
    partitionDesc.setInputFileFormatClass(org.apache.hadoop.mapred.TextInputFormat.class);
    mapWork.addPathToPartitionInfo(new Path("/tmp"), partitionDesc);
    Configuration conf = new Configuration(false);
    HiveConf.setVar(conf, HiveConf.ConfVars.LLAP_IO_ENABLED, "true");
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.LLAP_IO_NONVECTOR_WRAPPER_ENABLED, true);

    mapWork.deriveLlap(conf, false);

    assertTrue("Cache affinity should be set for TextInputFormat, as LLAP serde cache would use it",
        mapWork.getCacheAffinity());
  }

  @Test
  public void testConfigureJobConfPropagatesTableCreateTime() {
    // Given a table with a realistic create time
    String dbName = "test_db";
    String tableName = "test_table";
    int createTime = 1770653453;

    Table table = new Table(dbName, tableName);
    table.setCreateTime(createTime);

    // And a TableScanOperator configured for that table
    TableScanDesc tsDesc = new TableScanDesc(table);
    CompilationOpContext cCtx = new CompilationOpContext();
    TableScanOperator tsOp = new TableScanOperator(cCtx);
    tsOp.setConf(tsDesc);

    // And a MapWork that uses this TableScanOperator as a root
    MapWork mapWork = new MapWork();
    mapWork.getAliasToWork().put("t", tsOp);

    JobConf jobConf = new JobConf();

    // When configuring the job from the MapWork
    mapWork.configureJobConf(jobConf);

    // Then the table's create time should be present in the JobConf
    String fullTableName = TableName.getDbTable(dbName, tableName);
    assertEquals(
        createTime,
        Utilities.getTableCreateTime(jobConf, fullTableName));
  }
}
