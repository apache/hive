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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.WindowFunctionInfo;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.udf.ptf.WindowingTableFunction;

/**
 * A GenericUDAF mode that provides it results as a List to the
 * {@link WindowingTableFunction} (so it is a
 * {@link WindowFunctionInfo#isPivotResult()} return true) may support this
 * interface. If it does then the WindowingTableFunction will ask it for the
 * next Result after every aggregate call.
 */
public interface ISupportStreamingModeForWindowing {

  Object getNextResult(AggregationBuffer agg) throws HiveException;
  
  /*
   * for functions that don't support a Window, this provides the rows remaining to be 
   * added to output. Functions that return a Window can throw a UnsupportedException,
   * this method shouldn't be called. For Ranking fns return 0; lead/lag fns return the
   * lead/lag amt.
   */
  int getRowsRemainingAfterTerminate() throws HiveException;

  public static Object NULL_RESULT = new Object();
}
