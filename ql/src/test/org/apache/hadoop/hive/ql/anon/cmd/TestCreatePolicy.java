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

package org.apache.hadoop.hive.ql.anon.cmd;

import org.apache.hadoop.hive.ql.anon.BaseTest;
import org.apache.hadoop.hive.ql.anon.builders.CreateDataErasurePolicyStatementBuilder;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.hive.ql.anon.TestUtils.getTestPolicyDsl;

public class TestCreatePolicy extends BaseTest {

  @Override
  protected String[] managedErasurePolicies() {
    return new String[] { "ap1" };
  }

  @BeforeAll
  public void setup() throws CommandProcessorException {
  }

  @Test
  public void testCreatePolicy() throws CommandProcessorException, IOException {
    final String cmd = new CreateDataErasurePolicyStatementBuilder()
      .withPolicyName("ap1")
      .withPolicySource(getTestPolicyDsl())
      .withIfNotExists()
      .build();
    List<Object> lst = execute(cmd);
    Assertions.assertEquals(0, lst.size());
  }
}
