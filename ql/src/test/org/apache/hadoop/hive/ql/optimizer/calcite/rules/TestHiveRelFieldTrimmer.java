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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.junit.Test;

public class TestHiveRelFieldTrimmer {

  @Test
  public void testRegisterDefaultClassesNoException() throws Throwable {
    HiveRelFieldTrimmer trimmer = new HiveRelFieldTrimmer(false);
    trimmer.register(CalcitePlanner.HIVE_REL_NODE_CLASSES);
  }
}
