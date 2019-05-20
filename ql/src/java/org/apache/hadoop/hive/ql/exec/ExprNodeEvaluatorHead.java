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

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * Increases version number of each evaluations for correct caching
 */
public class ExprNodeEvaluatorHead extends ExprNodeEvaluator {

  private int counter;
  private final ExprNodeEvaluator referencing;

  public ExprNodeEvaluatorHead(ExprNodeEvaluator referencing) {
    super(referencing.getExpr(), referencing.getConf());
    this.referencing = referencing;
  }

  @Override
  public ObjectInspector initialize(ObjectInspector rowInspector) throws HiveException {
    return outputOI = referencing.initialize(rowInspector);
  }

  @Override
  protected Object _evaluate(Object row, int version) throws HiveException {
    return referencing.evaluate(row, next());
  }

  private int next() {
    return ++counter < 0 ? counter = 0 : counter;
  }
}
