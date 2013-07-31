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

import static org.mockito.Mockito.when;

import java.util.List;

import junit.framework.Assert;

import org.apache.hive.ptest.execution.ssh.NonZeroExitCodeException;
import org.approvaltests.Approvals;
import org.approvaltests.reporters.JunitReporter;
import org.approvaltests.reporters.UseReporter;
import org.junit.Before;
import org.junit.Test;


@UseReporter(JunitReporter.class)
public class TestPhase extends AbstractTestPhase {

  private Phase phase;
  @Before
  public void setup() throws Exception {
    initialize(getClass().getSimpleName());
    createHostExecutor();
  }

  @Test(expected = NonZeroExitCodeException.class)
  public void testExecLocallyFails() throws Throwable {
    phase = new Phase(hostExecutors, localCommandFactory,
        templateDefaults, logger) {
      @Override
      public void execute() throws Exception {
        execLocally("local");
      }
    };
    when(localCommand.getExitCode()).thenReturn(1);
    phase.execute();
  }
  @Test
  public void testExecLocallySucceeds() throws Throwable {
    phase = new Phase(hostExecutors, localCommandFactory,
        templateDefaults, logger) {
      @Override
      public void execute() throws Exception {
        execLocally("local");
      }
    };
    phase.execute();
    List<String> commands = localCommandFactory.getCommands();
    Assert.assertEquals(1, commands.size());
    Assert.assertEquals("local", commands.get(0));
  }
  @Test
  public void testExecInstancesWithFailure() throws Throwable {
    sshCommandExecutor.putFailure("echo", Constants.EXIT_CODE_UNKNOWN);
    phase = new Phase(hostExecutors, localCommandFactory,
        templateDefaults, logger) {
      @Override
      public void execute() throws Exception {
        execInstances("echo");
      }
    };
    phase.execute();
    Approvals.verify(getExecutedCommands());
    Assert.assertEquals(1,  hostExecutor.remainingDrones());
  }
  @Test
  public void testExecHostsWithFailure() throws Throwable {
    sshCommandExecutor.putFailure("echo", Constants.EXIT_CODE_UNKNOWN);
    phase = new Phase(hostExecutors, localCommandFactory,
        templateDefaults, logger) {
      @Override
      public void execute() throws Exception {
        execHosts("echo");
      }
    };
    phase.execute();
    Approvals.verify(getExecutedCommands());
    Assert.assertEquals(1,  hostExecutor.remainingDrones());
  }
  @Test
  public void testRsyncFromLocalToRemoteInstancesWithFailureUnknown() throws Throwable {
    rsyncCommandExecutor.putFailure("local remote", Constants.EXIT_CODE_UNKNOWN);
    phase = new Phase(hostExecutors, localCommandFactory,
        templateDefaults, logger) {
      @Override
      public void execute() throws Exception {
        rsyncFromLocalToRemoteInstances("local", "remote");
      }
    };
    phase.execute();
    Approvals.verify(getExecutedCommands());
    Assert.assertEquals(1,  hostExecutor.remainingDrones());
  }
  @Test
  public void testRsyncFromLocalToRemoteInstancesWithFailureOne() throws Throwable {
    rsyncCommandExecutor.putFailure("local remote", 1);
    phase = new Phase(hostExecutors, localCommandFactory,
        templateDefaults, logger) {
      @Override
      public void execute() throws Exception {
        rsyncFromLocalToRemoteInstances("local", "remote");
      }
    };
    phase.execute();
    Approvals.verify(getExecutedCommands());
    Assert.assertEquals(1,  hostExecutor.remainingDrones());
  }
}
