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

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hive.common.util.AnnotationUtils;

@SuppressWarnings("deprecation")
public class WindowFunctionInfo extends FunctionInfo {

  private final boolean supportsWindow;
  private final boolean pivotResult;
  private final boolean impliesOrder;
  private final boolean orderedAggregate;
  private final boolean supportsNullTreatment;

  public WindowFunctionInfo(FunctionType functionType, String functionName,
      GenericUDAFResolver resolver, FunctionResource[] resources) {
    super(functionType, functionName, resolver, resources);
    WindowFunctionDescription def =
        AnnotationUtils.getAnnotation(resolver.getClass(), WindowFunctionDescription.class);
    supportsWindow = def == null ? true : def.supportsWindow();
    pivotResult = def == null ? false : def.pivotResult();
    impliesOrder = def == null ? false : def.impliesOrder();
    orderedAggregate = def == null ? false : def.orderedAggregate();
    supportsNullTreatment = def == null ? false : def.supportsNullTreatment();
  }

  public boolean isSupportsWindow() {
    return supportsWindow;
  }

  public boolean isPivotResult() {
    return pivotResult;
  }

  /**
   * Property for indicating that the function is a window function and an OVER clause is required when invoked.
   * example:
   * SELECT val, rank() OVER (ORDER BY val DESC) FROM t_table;
   * @return true if the function is a window function, false otherwise
   */
  public boolean isImpliesOrder() {
    return impliesOrder;
  }

  /**
   * Property for indicating that the function is an Ordered-Set Aggregate function.
   * A WITHIN GROUP clause is required when invoked.
   * example:
   * SELECT rank(1) WITHIN GROUP (ORDER BY val) FROM t_table;
   * @return true if the function is a an Ordered-Set Aggregate function, false otherwise
   */
  public boolean isOrderedAggregate() {
    return orderedAggregate;
  }

  public boolean isSupportsNullTreatment() {
    return supportsNullTreatment;
  }
}
