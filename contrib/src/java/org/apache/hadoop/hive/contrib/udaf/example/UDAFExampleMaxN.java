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


package org.apache.hadoop.hive.contrib.udaf.example;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;

/**
 * Returns the max N double values.
 */
@Description(name = "example_max_n", value = "_FUNC_(expr) - Example UDAF that returns the max N double values")
public class UDAFExampleMaxN extends UDAF {

  /**
   * The evaluator for getting max N double values.
   */
  public static class UDAFMaxNEvaluator extends UDAFExampleMaxMinNUtil.Evaluator {

    @Override
    protected boolean getAscending() {
      return false;
    }
  }
  
}

