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

import java.io.Externalizable;
import java.io.Serializable;

import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.ql.exec.PTFUtils;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * AggregationDesc.
 *
 */
public class AggregationDesc implements java.io.Serializable {

  static {
    PTFUtils.makeTransient(AggregationDesc.class, "genericUDAFEvaluator");
  }

  private static final long serialVersionUID = 1L;
  private String genericUDAFName;

  private java.util.ArrayList<ExprNodeDesc> parameters;
  private boolean distinct;
  private GenericUDAFEvaluator.Mode mode;

  // used for GenericUDAFEvaluator
  private String genericUDAFEvaluatorClassName;
  // used for GenericUDAFBridgeEvaluator
  private GenericUDAFEvaluator genericUDAFWritableEvaluator;

  private transient GenericUDAFEvaluator genericUDAFEvaluator;

  public AggregationDesc() {
  }

  public AggregationDesc(final String genericUDAFName,
      final GenericUDAFEvaluator genericUDAFEvaluator,
      final java.util.ArrayList<ExprNodeDesc> parameters,
      final boolean distinct, final GenericUDAFEvaluator.Mode mode) {
    this.genericUDAFName = genericUDAFName;
    this.parameters = parameters;
    this.distinct = distinct;
    this.mode = mode;
    setGenericUDAFEvaluator(genericUDAFEvaluator);
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
    if (genericUDAFEvaluator instanceof Serializable ||
        genericUDAFEvaluator instanceof Externalizable) {
      this.genericUDAFWritableEvaluator = genericUDAFEvaluator;
    } else {
      this.genericUDAFEvaluatorClassName = genericUDAFEvaluator.getClass().getName();
    }
  }

  public GenericUDAFEvaluator getGenericUDAFEvaluator() {
    if (genericUDAFEvaluator != null) {
      return genericUDAFEvaluator;
    }
    if (genericUDAFWritableEvaluator != null) {
      return genericUDAFEvaluator = genericUDAFWritableEvaluator;
    }
    try {
      return genericUDAFEvaluator =
          ReflectionUtils.newInstance(Class.forName(genericUDAFEvaluatorClassName, true,
          JavaUtils.getClassLoader()).asSubclass(GenericUDAFEvaluator.class), null);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public String getGenericUDAFEvaluatorClassName() {
    return genericUDAFEvaluatorClassName;
  }

  public void setGenericUDAFEvaluatorClassName(String genericUDAFEvaluatorClassName) {
    this.genericUDAFEvaluatorClassName = genericUDAFEvaluatorClassName;
  }

  public GenericUDAFEvaluator getGenericUDAFWritableEvaluator() {
    return genericUDAFWritableEvaluator;
  }

  public void setGenericUDAFWritableEvaluator(GenericUDAFEvaluator genericUDAFWritableEvaluator) {
    this.genericUDAFWritableEvaluator = genericUDAFWritableEvaluator;
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
