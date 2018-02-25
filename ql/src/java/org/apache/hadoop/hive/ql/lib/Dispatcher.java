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

package org.apache.hadoop.hive.ql.lib;

import java.util.Stack;

import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Dispatcher interface for Operators Used in operator graph walking to dispatch
 * process/visitor functions for operators.
 */
public interface Dispatcher {

  /**
   * Dispatcher function.
   *
   * @param nd
   *          operator to process.
   * @param stack
   *          operator stack to process.
   * @param nodeOutputs
   *          The argument list of outputs from processing other nodes that are
   *          passed to this dispatcher from the walker.
   * @return Object The return object from the processing call.
   * @throws SemanticException
   */
  Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs)
      throws SemanticException;

}
