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

package org.apache.hadoop.hive.ql.plan.ptf;


import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.plan.Explain;

public abstract class PTFInputDef {
  private String expressionTreeString;
  private ShapeDetails outputShape;
  private String alias;

  public String getExpressionTreeString() {
    return expressionTreeString;
  }

  public void setExpressionTreeString(String expressionTreeString) {
    this.expressionTreeString = expressionTreeString;
  }

  public ShapeDetails getOutputShape() {
    return outputShape;
  }

  @Explain(displayName = "output shape")
  public String getOutputShapeExplain() {
    RowSchema schema = outputShape.getRr().getRowSchema();
    return StringUtils.join(schema.getSignature(), ", ");
  }

  public void setOutputShape(ShapeDetails outputShape) {
    this.outputShape = outputShape;
  }

  @Explain(displayName = "input alias")
  public String getAlias() {
    return alias;
  }
  public void setAlias(String alias) {
    this.alias = alias;
  }

  public abstract PTFInputDef getInput();
}