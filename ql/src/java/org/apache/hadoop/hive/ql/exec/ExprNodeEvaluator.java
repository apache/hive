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

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * ExprNodeEvaluator.
 *
 */
public abstract class ExprNodeEvaluator {

  /**
   * Initialize should be called once and only once. Return the ObjectInspector
   * for the return value, given the rowInspector.
   */
  public abstract ObjectInspector initialize(ObjectInspector rowInspector) throws HiveException;

  /**
   * Evaluate the expression given the row. This method should use the
   * rowInspector passed in from initialize to inspect the row object. The
   * return value will be inspected by the return value of initialize.
   */
  public abstract Object evaluate(Object row) throws HiveException;

}
