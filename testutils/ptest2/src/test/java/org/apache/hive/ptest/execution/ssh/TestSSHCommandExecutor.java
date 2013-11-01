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
package org.apache.hive.ptest.execution.ssh;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import junit.framework.Assert;

import org.apache.hive.ptest.execution.Constants;
import org.apache.hive.ptest.execution.LocalCommand;
import org.apache.hive.ptest.execution.MockLocalCommandFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSSHCommandExecutor {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestSSHCommandExecutor.class);

  private MockLocalCommandFactory localCommandFactory;

  @Before
  public void setup() throws Exception {
    localCommandFactory = new MockLocalCommandFactory(LOG);
  }

  @Test
  public void testShutdownBeforeWaitFor() throws Exception {
    LocalCommand localCommand = mock(LocalCommand.class);
    localCommandFactory.setInstance(localCommand);
    SSHCommandExecutor executor = new SSHCommandExecutor(LOG, localCommandFactory);
    Assert.assertFalse(executor.isShutdown());
    executor.shutdownNow();
    SSHCommand command = new SSHCommand(executor, "privateKey", "user", "host", 1, "whoami");
    executor.execute(command);
    Assert.assertTrue(executor.isShutdown());
    Assert.assertEquals(Constants.EXIT_CODE_UNKNOWN, command.getExitCode());
    if(command.getException() != null) {
      throw new Exception("Unexpected exception during execution", command.getException());
    }
    verify(localCommand, times(1)).kill();
  }
  @Test
  public void testShutdownDuringWaitFor() throws Exception {
    LocalCommand localCommand = mock(LocalCommand.class);
    localCommandFactory.setInstance(localCommand);
    final SSHCommandExecutor executor = new SSHCommandExecutor(LOG, localCommandFactory);
    Assert.assertFalse(executor.isShutdown());
    when(localCommand.getExitCode()).thenAnswer(new Answer<Integer>() {
      @Override
      public Integer answer(InvocationOnMock invocation) throws Throwable {
        executor.shutdownNow();
        return Constants.EXIT_CODE_UNKNOWN;
      }
    });
    SSHCommand command = new SSHCommand(executor, "privateKey", "user", "host", 1, "whoami");
    executor.execute(command);
    Assert.assertTrue(executor.isShutdown());
    Assert.assertEquals(Constants.EXIT_CODE_UNKNOWN, command.getExitCode());
    if(command.getException() != null) {
      throw new Exception("Unexpected exception during execution", command.getException());
    }
    verify(localCommand, never()).kill();
  }
}