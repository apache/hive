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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import junit.framework.TestCase;

import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.filterDesc;
import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.plan.partitionDesc;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.JobConf;

public class TestPlan extends TestCase {

  public void testPlan() throws Exception {

    final String F1 = "#affiliations";
    final String F2 = "friends[0].friendid";

    try {
      // initialize a complete map reduce configuration
      exprNodeDesc expr1 = new exprNodeColumnDesc(
          TypeInfoFactory.stringTypeInfo, F1, "", false);
      exprNodeDesc expr2 = new exprNodeColumnDesc(
          TypeInfoFactory.stringTypeInfo, F2, "", false);
      exprNodeDesc filterExpr = TypeCheckProcFactory.DefaultExprProcessor
          .getFuncExprNodeDesc("==", expr1, expr2);

      filterDesc filterCtx = new filterDesc(filterExpr, false);
      Operator<filterDesc> op = OperatorFactory.get(filterDesc.class);
      op.setConf(filterCtx);

      ArrayList<String> aliasList = new ArrayList<String>();
      aliasList.add("a");
      LinkedHashMap<String, ArrayList<String>> pa = new LinkedHashMap<String, ArrayList<String>>();
      pa.put("/tmp/testfolder", aliasList);

      tableDesc tblDesc = Utilities.defaultTd;
      partitionDesc partDesc = new partitionDesc(tblDesc, null);
      LinkedHashMap<String, partitionDesc> pt = new LinkedHashMap<String, partitionDesc>();
      pt.put("/tmp/testfolder", partDesc);

      LinkedHashMap<String, Operator<? extends Serializable>> ao = new LinkedHashMap<String, Operator<? extends Serializable>>();
      ao.put("a", op);

      mapredWork mrwork = new mapredWork();
      mrwork.setPathToAliases(pa);
      mrwork.setPathToPartitionInfo(pt);
      mrwork.setAliasToWork(ao);

      // serialize the configuration once ..
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      Utilities.serializeMapRedWork(mrwork, baos);
      baos.close();
      String v1 = baos.toString();

      // store into configuration
      JobConf job = new JobConf(TestPlan.class);
      job.set("fs.default.name", "file:///");
      Utilities.setMapRedWork(job, mrwork);
      mapredWork mrwork2 = Utilities.getMapRedWork(job);
      Utilities.clearMapRedWork(job);

      // over here we should have some checks of the deserialized object against
      // the orginal object
      // System.out.println(v1);

      // serialize again
      baos.reset();
      Utilities.serializeMapRedWork(mrwork2, baos);
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
