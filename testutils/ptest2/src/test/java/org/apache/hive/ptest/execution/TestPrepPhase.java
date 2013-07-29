/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.ptest.execution;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.approvaltests.Approvals;
import org.approvaltests.reporters.JunitReporter;
import org.approvaltests.reporters.UseReporter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


@UseReporter(JunitReporter.class)
public class TestPrepPhase extends AbstractTestPhase {
  private PrepPhase phase;

  @Before
  public void setup() throws Exception {
    initialize(getClass().getSimpleName());
    createHostExecutor();
  }
  @After
  public void teardown() {
    FileUtils.deleteQuietly(baseDir);
  }
  @Test
  public void testExecute() throws Exception {
    phase = new PrepPhase(hostExecutors, localCommandFactory,
        templateDefaults, baseDir, (File)null, logger);
    phase.execute();
    Approvals.verify(getExecutedCommands());
  }
}
