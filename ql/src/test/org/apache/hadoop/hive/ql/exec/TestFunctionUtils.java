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
import org.junit.Assert;
import org.junit.Test;

public class TestFunctionUtils {

  @Test
  public void testSplitQualifiedFunctionName() throws HiveException {

    String function1 = Registry.WINDOW_FUNC_PREFIX + "database1.function1";
    String function2 = "database2.function2";

    String[] output1 = FunctionUtils.splitQualifiedFunctionName(function1);
    Assert.assertEquals("database1", output1[0]);
    Assert.assertEquals("function1", output1[1]);

    String[] output2 = FunctionUtils.splitQualifiedFunctionName(function2);
    Assert.assertEquals("database2", output2[0]);
    Assert.assertEquals("function2", output2[1]);

  }

}