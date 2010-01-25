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

package org.apache.hadoop.hive.ql.plan;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

public class AggregationDesc implements java.io.Serializable {
  private static final long serialVersionUID = 1L;
  private String genericUDAFName;

  /**
   * In case genericUDAFEvaluator is Serializable, we will serialize the object.
   * 
   * In case genericUDAFEvaluator does not implement Serializable, Java will
   * remember the class of genericUDAFEvaluator and creates a new instance when
   * deserialized. This is exactly what we want.
   */
  private GenericUDAFEvaluator genericUDAFEvaluator;
  private java.util.ArrayList<ExprNodeDesc> parameters;
  private boolean distinct;
  private GenericUDAFEvaluator.Mode mode;

  public AggregationDesc() {
  }

  public AggregationDesc(final String genericUDAFName,
      final GenericUDAFEvaluator genericUDAFEvaluator,
      final java.util.ArrayList<ExprNodeDesc> parameters,
      final boolean distinct, final GenericUDAFEvaluator.Mode mode) {
    this.genericUDAFName = genericUDAFName;
    this.genericUDAFEvaluator = genericUDAFEvaluator;
    this.parameters = parameters;
    this.distinct = distinct;
    this.mode = mode;
  }

  public void setGenericUDAFName(final String genericUDAFName) {
    this.genericUDAFName = genericUDAFName;
  }

  public String getGenericUDAFName() {
    return genericUDAFName;
  }

  public void setGenericUDAFEvaluator(
      final GenericUDAFEvaluator genericUDAFEvaluator) {
    this.genericUDAFEvaluator = genericUDAFEvaluator;
  }

  public GenericUDAFEvaluator getGenericUDAFEvaluator() {
    return genericUDAFEvaluator;
  }

  public java.util.ArrayList<ExprNodeDesc> getParameters() {
    return parameters;
  }

  public void setParameters(final java.util.ArrayList<ExprNodeDesc> parameters) {
    this.parameters = parameters;
  }

  public boolean getDistinct() {
    return distinct;
  }

  public void setDistinct(final boolean distinct) {
    this.distinct = distinct;
  }

  public void setMode(final GenericUDAFEvaluator.Mode mode) {
    this.mode = mode;
  }

  public GenericUDAFEvaluator.Mode getMode() {
    return mode;
  }

  @Explain(displayName = "expr")
  public String getExprString() {
    StringBuilder sb = new StringBuilder();
    sb.append(genericUDAFName);
    sb.append("(");
    if (distinct) {
      sb.append("DISTINCT ");
    }
    boolean first = true;
    for (ExprNodeDesc exp : parameters) {
      if (first) {
        first = false;
      } else {
        sb.append(", ");
      }
      sb.append(exp.getExprString());
    }
    sb.append(")");
    return sb.toString();
  }
}
