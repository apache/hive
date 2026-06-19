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

package org.apache.hadoop.hive.ql.exec;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.parse.type.ExprNodeTypeCheck;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.JobConf;


import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * TestPlan.
 *
 */
public class TestPlan {

  @Test
  public void testPlan() throws Exception {

    final String f1 = "#affiliations";
    final String f2 = "friends[0].friendid";

    try {
      // initialize a complete map reduce configuration
      ExprNodeDesc expr1 = new ExprNodeColumnDesc(
          TypeInfoFactory.stringTypeInfo, f1, "", false);
      ExprNodeDesc expr2 = new ExprNodeColumnDesc(
          TypeInfoFactory.stringTypeInfo, f2, "", false);
      ExprNodeDesc filterExpr = ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
          .getFuncExprNodeDesc("==", expr1, expr2);

      FilterDesc filterCtx = new FilterDesc(filterExpr, false);
      Operator<FilterDesc> op = OperatorFactory.get(new CompilationOpContext(), FilterDesc.class);
      op.setConf(filterCtx);

      ArrayList<String> aliasList = new ArrayList<String>();
      aliasList.add("a");
      Map<Path, List<String>> pa = new LinkedHashMap<>();
      pa.put(new Path("/tmp/testfolder"), aliasList);

      TableDesc tblDesc = Utilities.defaultTd;
      PartitionDesc partDesc = new PartitionDesc(tblDesc, null);
      LinkedHashMap<Path, PartitionDesc> pt = new LinkedHashMap<>();
      pt.put(new Path("/tmp/testfolder"), partDesc);

      LinkedHashMap<String, Operator<? extends OperatorDesc>> ao =
          new LinkedHashMap<String, Operator<? extends OperatorDesc>>();
      ao.put("a", op);

      MapredWork mrwork = new MapredWork();
      mrwork.getMapWork().setPathToAliases(pa);
      mrwork.getMapWork().setPathToPartitionInfo(pt);
      mrwork.getMapWork().setAliasToWork(ao);

      JobConf job = new JobConf(TestPlan.class);
      // serialize the configuration once ..
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      SerializationUtilities.serializePlan(mrwork, baos);
      baos.close();
      String v1 = baos.toString();

      // store into configuration

      job.set("fs.default.name", "file:///");
      Utilities.setMapRedWork(job, mrwork, new Path(System.getProperty("java.io.tmpdir") + File.separator +
          System.getProperty("user.name") + File.separator + "hive"));
      MapredWork mrwork2 = Utilities.getMapRedWork(job);
      Utilities.clearWork(job);

      // over here we should have some checks of the deserialized object against
      // the orginal object
      // System.out.println(v1);

      // serialize again
      baos.reset();
      SerializationUtilities.serializePlan(mrwork2, baos);
      baos.close();

      // verify that the two are equal
      assertEquals(v1, baos.toString());

    } catch (Exception excp) {
      excp.printStackTrace();
      throw excp;
    }
    System.out.println("Serialization/Deserialization of plan successful");
  }
}
