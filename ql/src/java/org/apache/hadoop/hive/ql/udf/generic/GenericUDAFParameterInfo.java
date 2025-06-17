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
package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * A callback interface used in conjunction with <pre>GenericUDAFResolver2</pre>
 * interface that allows for a more extensible and flexible means of
 * discovering the parameter types provided for UDAF invocation. Apart from
 * allowing the function implementation to discover the <pre>TypeInfo</pre> of
 * any types provided in the invocation, this also allows the implementation
 * to determine if the parameters were qualified using <pre>DISTINCT</pre>. If
 * no parameters were specified explicitly, it allows the function
 * implementation to test if the invocation used the wildcard syntax
 * such as <pre>FUNCTION(*)</pre>.
 * <p>
 * <b>Note:</b> The implementation of function does not have to handle the
 * actual <pre>DISTINCT</pre> or wildcard implementation. This information is
 * provided only to allow the function implementation to accept or reject
 * such invocations. For example - the implementation of <pre>COUNT</pre> UDAF
 * requires that the <pre>DISTINCT</pre> qualifier be supplied when more than
 * one parameters are specified in the invocation. The actual filtering of
 * data bound to parameter types for <pre>DISTINCT</pre> implementation is
 * handled by the framework and not the <pre>COUNT</pre> UDAF implementation.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface GenericUDAFParameterInfo {

  /**
   *
   * @return the parameter type list passed into the UDAF.
   */
  @Deprecated
  TypeInfo[] getParameters();

  /**
   *
   * @return getParameters() with types returned as ObjectInspectors.
   */
  ObjectInspector[] getParameterObjectInspectors();

  /**
   * Returns <pre>true</pre> if the UDAF invocation was qualified with
   * <pre>DISTINCT</pre> keyword. Note that this is provided for informational
   * purposes only and the function implementation is not expected to ensure
   * the distinct property for the parameter values. That is handled by the
   * framework.
   * @return <pre>true</pre> if the UDAF invocation was qualified with
   * <pre>DISTINCT</pre> keyword, <pre>false</pre> otherwise.
   */
  boolean isDistinct();

  /**
   * The flag to indicate if the UDAF invocation was from the windowing function
   * call or not.
   * @return <pre>true</pre> if the UDAF invocation was from the windowing function
   * call.
   */
  boolean isWindowing();
  /**
   * Returns <pre>true</pre> if the UDAF invocation was done via the wildcard
   * syntax <pre>FUNCTION(*)</pre>. Note that this is provided for informational
   * purposes only and the function implementation is not expected to ensure
   * the wildcard handling of the target relation. That is handled by the
   * framework.
   * @return <pre>true</pre> if the UDAF invocation was done with a wildcard
   * instead of explicit parameter list.
   */
  boolean isAllColumns();

  boolean respectNulls();
}
