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

import java.io.Serializable;

import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.util.ReflectionUtils;

public class aggregationDesc implements java.io.Serializable {
  private static final long serialVersionUID = 1L;
  private String genericUDAFName;
  private Class<? extends GenericUDAFEvaluator> genericUDAFEvaluatorClass;
  private GenericUDAFEvaluator genericUDAFEvaluator;
  private java.util.ArrayList<exprNodeDesc> parameters;
  private boolean distinct;
  private GenericUDAFEvaluator.Mode mode;
  public aggregationDesc() {}
  public aggregationDesc(
    final String genericUDAFName,
    final GenericUDAFEvaluator genericUDAFEvaluator,
    final java.util.ArrayList<exprNodeDesc> parameters,
    final boolean distinct,
    final GenericUDAFEvaluator.Mode mode) {
    this.genericUDAFName = genericUDAFName;
    if (genericUDAFEvaluator instanceof Serializable) {
      this.genericUDAFEvaluator = genericUDAFEvaluator;
      this.genericUDAFEvaluatorClass = null;
    } else {
      this.genericUDAFEvaluator = null;
      this.genericUDAFEvaluatorClass = genericUDAFEvaluator.getClass();
    }
    this.parameters = parameters;
    this.distinct = distinct;
    this.mode = mode;
  }
  public GenericUDAFEvaluator createGenericUDAFEvaluator() {
    return (genericUDAFEvaluator != null) ? genericUDAFEvaluator
        : (GenericUDAFEvaluator)ReflectionUtils.newInstance(genericUDAFEvaluatorClass, null);
  }
  public void setGenericUDAFName(final String genericUDAFName) {
    this.genericUDAFName = genericUDAFName;
  }
  public String getGenericUDAFName() {
    return genericUDAFName;
  }
  public void setGenericUDAFEvaluator(final GenericUDAFEvaluator genericUDAFEvaluator) {
    this.genericUDAFEvaluator = genericUDAFEvaluator;
  }
  public GenericUDAFEvaluator getGenericUDAFEvaluator() {
    return genericUDAFEvaluator;
  }
  public void setGenericUDAFEvaluatorClass(final Class<? extends GenericUDAFEvaluator> genericUDAFEvaluatorClass) {
    this.genericUDAFEvaluatorClass = genericUDAFEvaluatorClass;
  }
  public Class<? extends GenericUDAFEvaluator>  getGenericUDAFEvaluatorClass() {
    return genericUDAFEvaluatorClass;
  }
  public java.util.ArrayList<exprNodeDesc> getParameters() {
    return this.parameters;
  }
  public void setParameters(final java.util.ArrayList<exprNodeDesc> parameters) {
    this.parameters=parameters;
  }
  public boolean getDistinct() {
    return this.distinct;
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
  
  @explain(displayName="expr")
  public String getExprString() {
    StringBuilder sb = new StringBuilder();
    sb.append(genericUDAFName);
    sb.append("(");
    if (distinct) {
      sb.append("DISTINCT ");
    }
    boolean first = true;
    for(exprNodeDesc exp: parameters) {
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
