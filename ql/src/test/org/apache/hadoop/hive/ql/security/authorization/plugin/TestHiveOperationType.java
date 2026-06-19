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
package org.apache.hadoop.hive.ql.security.authorization.plugin;

import static org.junit.Assert.fail;

import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.junit.Test;

/**
 * Test HiveOperationType
 */
public class TestHiveOperationType {

  /**
   * test that all enums in {@link HiveOperation} match one in @{link HiveOperationType}
   */
  @Test
  public void checkHiveOperationTypeMatch(){
    for (HiveOperation op : HiveOperation.values()) {
      try {
        HiveOperationType.valueOf(op.name());
      } catch(IllegalArgumentException ex) {
        // if value is null or not found, exception would get thrown
        fail("Unable to find corresponding type in HiveOperationType for " + op + " : " +  ex );
      }
    }
  }

}
