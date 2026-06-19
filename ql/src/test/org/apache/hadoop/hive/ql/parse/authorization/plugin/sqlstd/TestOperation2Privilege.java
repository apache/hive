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
package org.apache.hadoop.hive.ql.parse.authorization.plugin.sqlstd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Set;

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.Operation2Privilege;
import org.junit.Test;

/**
 * Test HiveOperationType
 */
public class TestOperation2Privilege {

  /**
   * test that all enums in {@link HiveOperationType} match one map entry in
   * Operation2Privilege
   */
  @Test
  public void checkHiveOperationTypeMatch() {
    Set<HiveOperationType> operationMapKeys = Operation2Privilege.getOperationTypes();
    for (HiveOperationType operationType : HiveOperationType.values()) {
      if (!operationMapKeys.contains(operationType)) {
        fail("Unable to find corresponding entry in Operation2Privilege map for HiveOperationType "
            + operationType);
      }
    }
    assertEquals("Check if Operation2Privilege, HiveOperationType have same number of instances",
        operationMapKeys.size(), HiveOperationType.values().length);
  }

}
