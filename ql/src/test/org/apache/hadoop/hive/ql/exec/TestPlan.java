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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import junit.framework.TestCase;

import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.JobConf;

/**
 * TestPlan.
 *
 */
public class TestPlan extends TestCase {

  public void testPlan() throws Exception {

    final String F1 = "#affiliations";
    final String F2 = "friends[0].friendid";

    try {
      // initialize a complete map reduce configuration
      ExprNodeDesc expr1 = new ExprNodeColumnDesc(
          TypeInfoFactory.stringTypeInfo, F1, "", false);
      ExprNodeDesc expr2 = new ExprNodeColumnDesc(
          TypeInfoFactory.stringTypeInfo, F2, "", false);
      ExprNodeDesc filterExpr = TypeCheckProcFactory.DefaultExprProcessor
          .getFuncExprNodeDesc("==", expr1, expr2);

      FilterDesc filterCtx = new FilterDesc(filterExpr, false);
      Operator<FilterDesc> op = OperatorFactory.get(FilterDesc.class);
      op.setConf(filterCtx);

      ArrayList<String> aliasList = new ArrayList<String>();
      aliasList.add("a");
      LinkedHashMap<String, ArrayList<String>> pa = new LinkedHashMap<String, ArrayList<String>>();
      pa.put("/tmp/testfolder", aliasList);

      TableDesc tblDesc = Utilities.defaultTd;
      PartitionDesc partDesc = new PartitionDesc(tblDesc, null);
      LinkedHashMap<String, PartitionDesc> pt = new LinkedHashMap<String, PartitionDesc>();
      pt.put("/tmp/testfolder", partDesc);

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
      Utilities.serializePlan(mrwork, baos, job);
      baos.close();
      String v1 = baos.toString();

      // store into configuration

      job.set("fs.default.name", "file:///");
      Utilities.setMapRedWork(job, mrwork, System.getProperty("java.io.tmpdir") + File.separator +
        System.getProperty("user.name") + File.separator + "hive");
      MapredWork mrwork2 = Utilities.getMapRedWork(job);
      Utilities.clearWork(job);

      // over here we should have some checks of the deserialized object against
      // the orginal object
      // System.out.println(v1);

      // serialize again
      baos.reset();
      Utilities.serializePlan(mrwork2, baos, job);
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
