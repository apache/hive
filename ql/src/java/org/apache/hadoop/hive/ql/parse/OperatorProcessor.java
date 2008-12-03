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
package org.apache.hadoop.hive.ql.parse;

import java.io.Serializable;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.optimizer.OperatorProcessorContext;

/**
 * Base class for processing operators which is no-op. The specific processors can register their own context with
 * the dispatcher.
 */
public interface OperatorProcessor {
  
  /**
   * generic process for all ops that don't have specific implementations
   * @param op operator to process
   * @param opProcCtx operator processor context
   * @throws SemanticException
   */
  public void process(Operator<? extends Serializable> op, OperatorProcessorContext opProcCtx) 
    throws SemanticException;
}
