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

package org.apache.hadoop.hive.ql.optimizer;

import static org.junit.Assert.assertEquals;
import java.util.ArrayList;

import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.optimizer.BucketVersionPopulator.InfoType;
import org.apache.hadoop.hive.ql.optimizer.BucketVersionPopulator.OperatorBucketingVersionInfo;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.junit.Test;

public class TestBucketVersionPopulator {

  CompilationOpContext cCtx = new CompilationOpContext();

  @Test
  public void testVersionInfoSort() {

    ArrayList<OperatorBucketingVersionInfo> arr = new ArrayList<>();
    OperatorBucketingVersionInfo opt2 = buildInfo(InfoType.OPTIONAL, 2);
    OperatorBucketingVersionInfo opt1 = buildInfo(InfoType.OPTIONAL, 1);
    OperatorBucketingVersionInfo mand2 = buildInfo(InfoType.MANDATORY, 2);
    OperatorBucketingVersionInfo mand1 = buildInfo(InfoType.MANDATORY, 1);
    arr.add(opt2);
    arr.add(opt1);
    arr.add(mand2);
    arr.add(mand1);
    arr.sort(OperatorBucketingVersionInfo.MANDATORY_FIRST);

    assertEquals(arr.get(0), mand2);
    assertEquals(arr.get(1), mand1);
    assertEquals(arr.get(2), opt2);
    assertEquals(arr.get(3), opt1);
  }

  private OperatorBucketingVersionInfo buildInfo(InfoType optional, int version) {
    return new OperatorBucketingVersionInfo(getOp(), optional, version);
  }

  private Operator<? extends OperatorDesc> getOp() {
    ExprNodeDesc pred = new ExprNodeConstantDesc(1);
    FilterDesc fd = new FilterDesc(pred, true);
    Operator<? extends OperatorDesc> op = OperatorFactory.get(cCtx, fd);
    return op;
  }

}
