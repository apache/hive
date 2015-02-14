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

package org.apache.hadoop.hive.ql.plan.ptf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

@Explain(displayName = "window function definition")
public class WindowFunctionDef extends WindowExpressionDef {
  String name;
  boolean isStar;
  boolean isDistinct;
  List<PTFExpressionDef> args;
  WindowFrameDef windowFrame;
  GenericUDAFEvaluator wFnEval;
  boolean pivotResult;

  @Explain(displayName = "name")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @Explain(displayName = "isStar", displayOnlyOnTrue = true)
  public boolean isStar() {
    return isStar;
  }

  public void setStar(boolean isStar) {
    this.isStar = isStar;
  }

  @Explain(displayName = "isDistinct", displayOnlyOnTrue = true)
  public boolean isDistinct() {
    return isDistinct;
  }

  public void setDistinct(boolean isDistinct) {
    this.isDistinct = isDistinct;
  }

  public List<PTFExpressionDef> getArgs() {
    return args;
  }

  public void setArgs(List<PTFExpressionDef> args) {
    this.args = args;
  }

  public void addArg(PTFExpressionDef arg) {
    args = args == null ? new ArrayList<PTFExpressionDef>() : args;
    args.add(arg);
  }

  @Explain(displayName = "arguments")
  public String getArgsExplain() {
    if (args == null) {
      return null;
    }
    StringBuilder builder = new StringBuilder();
    for (PTFExpressionDef expression : args) {
      if (builder.length() > 0) {
        builder.append(", ");
      }
      builder.append(expression.getExprNode().getExprString());
    }
    return builder.toString();
  }

  public WindowFrameDef getWindowFrame() {
    return windowFrame;
  }

  public void setWindowFrame(WindowFrameDef windowFrame) {
    this.windowFrame = windowFrame;
  }

  @Explain(displayName = "window frame")
  public String getWindowFrameExplain() {
    return windowFrame == null ? null : windowFrame.toString();
  }

  public GenericUDAFEvaluator getWFnEval() {
    return wFnEval;
  }

  public void setWFnEval(GenericUDAFEvaluator wFnEval) {
    this.wFnEval = wFnEval;
  }

  @Explain(displayName = "window function")
  public String getWFnEvalExplain() {
    return wFnEval == null ? null : wFnEval.getClass().getSimpleName();
  }

  @Explain(displayName = "isPivotResult", displayOnlyOnTrue = true)
  public boolean isPivotResult() {
    return pivotResult;
  }

  public void setPivotResult(boolean pivotResult) {
    this.pivotResult = pivotResult;
  }

}