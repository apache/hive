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

package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.spark.SparkCompiler;

/**
 * TaskCompilerFactory is a factory class to choose the appropriate
 * TaskCompiler.
 */
public class TaskCompilerFactory {

  private TaskCompilerFactory() {
    // avoid instantiation
  }

  /**
   * Returns the appropriate compiler to translate the operator tree
   * into executable units.
   */
  public static TaskCompiler getCompiler(HiveConf conf, ParseContext parseContext) {
    if (HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")) {
      return new TezCompiler();
    } else if (HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("spark")) {
      return new SparkCompiler();
    } else {
      return new MapReduceCompiler();
    }
  }
}
