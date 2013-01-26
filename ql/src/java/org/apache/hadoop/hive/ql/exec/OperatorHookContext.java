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

public class OperatorHookContext {
  private String operatorName;
  private String operatorId;
  private Object currentRow;
  private Operator operator;
  public OperatorHookContext(Operator op, Object row) {
    this(op.getName(), op.getIdentifier(), row);
    this.operator = op;
  }

  private OperatorHookContext(String opName, String opId, Object row) {
    operatorName = opName;
    operatorId = opId;
    currentRow = row;
  }

  public Operator getOperator() {
    return operator;
  }

  public String getOperatorName() {
    return operatorName;
  }

  public String getOperatorId() {
    return operatorId;
  }

  public Object getCurrentRow() {
    return currentRow;
  }

  @Override
  public String toString() {
    return  "operatorName= " + this.getOperatorName() +
      ", id=" + this.getOperatorId();

  }
}
