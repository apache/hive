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

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

/**
 * Resolver for Numeric UDAFs like sum and avg. If the input argument is string or date,
 * the resolver returns the evaluator whose iterate function operates on doubles.
 */
public class NumericUDAFEvaluatorResolver extends DefaultUDAFEvaluatorResolver {

  /**
   * Constructor.
   */
  public NumericUDAFEvaluatorResolver(Class<? extends UDAF> udafClass) {
    super(udafClass);
  }
  
  /* (non-Javadoc)
   * @see org.apache.hadoop.hive.ql.exec.UDAFMethodResolver#getEvaluatorClass(java.util.List)
   */
  @Override
  public Class<? extends UDAFEvaluator> getEvaluatorClass(
      List<Class<?>> argClasses) throws AmbiguousMethodException {
    // Go through the argClasses and for any string, void or date time, start looking for doubles
    ArrayList<Class<?>> args = new ArrayList<Class<?>>();
    for(Class<?>arg: argClasses) {
      if (arg == Void.class || arg == String.class || arg == Date.class) {
        args.add(Double.class);
      }
      else {
        args.add(arg);
      }
    }
    
    return super.getEvaluatorClass(args);
  }
}
