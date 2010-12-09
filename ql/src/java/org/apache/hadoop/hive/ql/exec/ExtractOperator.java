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

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExtractDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;

/**
 * Extract operator implementation Extracts a subobject and passes that on.
 **/
public class ExtractOperator extends Operator<ExtractDesc> implements
    Serializable {
  private static final long serialVersionUID = 1L;
  protected transient ExprNodeEvaluator eval;

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    eval = ExprNodeEvaluatorFactory.get(conf.getCol());
    outputObjInspector = eval.initialize(inputObjInspectors[0]);
    initializeChildren(hconf);
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {
    forward(eval.evaluate(row), outputObjInspector);
  }

  @Override
  public OperatorType getType() {
    return OperatorType.EXTRACT;
  }
}
