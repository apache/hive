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

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hive.common.util.AnnotationUtils;

@SuppressWarnings("deprecation")
public class WindowFunctionInfo implements CommonFunctionInfo {
  boolean supportsWindow = true;
  boolean pivotResult = false;
  boolean impliesOrder = false;
  FunctionInfo fInfo;

  WindowFunctionInfo(FunctionInfo fInfo) {
    assert fInfo.isGenericUDAF();
    this.fInfo = fInfo;
    Class<? extends GenericUDAFResolver> wfnCls = fInfo.getGenericUDAFResolver().getClass();
    WindowFunctionDescription def =
          AnnotationUtils.getAnnotation(wfnCls, WindowFunctionDescription.class);
    if ( def != null) {
      supportsWindow = def.supportsWindow();
      pivotResult = def.pivotResult();
      impliesOrder = def.impliesOrder();
    }
  }

  public boolean isSupportsWindow() {
    return supportsWindow;
  }

  public boolean isPivotResult() {
    return pivotResult;
  }

  public boolean isImpliesOrder() {
    return impliesOrder;
  }
  public FunctionInfo getfInfo() {
    return fInfo;
  }

  @Override
  public Class<?> getFunctionClass() {
    return getfInfo().getFunctionClass();
  }
}
