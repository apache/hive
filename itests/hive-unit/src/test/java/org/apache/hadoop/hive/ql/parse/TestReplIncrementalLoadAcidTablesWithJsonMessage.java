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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.Ignore;

import java.util.Collections;

@Ignore
public class TestReplIncrementalLoadAcidTablesWithJsonMessage
    extends TestReplicationScenariosIncrementalLoadAcidTables {

  @Rule
  public TestRule replV1BackwardCompat;

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    internalBeforeClassSetup(Collections.emptyMap(),
        TestReplIncrementalLoadAcidTablesWithJsonMessage.class);
  }

  @Before
  public void setup() throws Throwable {
    replV1BackwardCompat = primary.getReplivationV1CompatRule(Collections.emptyList());
    super.setup();
  }

}
