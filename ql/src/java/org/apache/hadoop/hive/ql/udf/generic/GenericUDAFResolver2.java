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

import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * This interface extends the <tt>GenericUDAFResolver</tt> interface and
 * provides more flexibility in terms of discovering the parameter types
 * supplied to the UDAF. Implementations that extend this interface will
 * also have access to extra information such as the specification of the
 * <tt>DISTINCT</tt> qualifier or the invocation with the special wildcard
 * character.
 * <p>
 * <b>Note:</b> The implementation of function does not have to handle the
 * actual <tt>DISTINCT</tt> or wildcard implementation. This information is
 * provided only to allow the function implementation to accept or reject
 * such invocations. For example - the implementation of <tt>COUNT</tt> UDAF
 * requires that the <tt>DISTINCT</tt> qualifier be supplied when more than
 * one parameters are specified in the invocation. The actual filtering of
 * data bound to parameter types for <tt>DISTINCT</tt> implementation is
 * handled by the framework and not the <tt>COUNT</tt> UDAF implementation.
 */
@Deprecated
@SuppressWarnings("deprecation")
public interface GenericUDAFResolver2 extends GenericUDAFResolver {

  /**
   * Get the evaluator for the parameter types.
   *
   * The reason that this function returns an object instead of a class is
   * because it is possible that the object needs some configuration (that can
   * be serialized). In that case the class of the object has to implement the
   * Serializable interface. At execution time, we will deserialize the object
   * from the plan and use it to evaluate the aggregations.
   * <p>
   * If the class of the object does not implement Serializable, then we will
   * create a new instance of the class at execution time.
   * </p>
   *
   * @param info The parameter information that is applicable to the UDAF being
   *          invoked.
   * @throws SemanticException
   */
  GenericUDAFEvaluator getEvaluator(
      GenericUDAFParameterInfo info) throws SemanticException;
}
