package org.apache.hive.service.cli.operation;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.session.HiveSession;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHiveCommandOperation {

  @Test
  public void testRunInternalSetsCorrectArgumentsForUntrimmedInput() throws HiveSQLException {

    HiveConf conf = new HiveConf(TestHiveCommandOperation.class);
    File tmpFile = new File("/dev/null");

    HiveSession mockHiveSession = mock(HiveSession.class);
    SessionState mockSessionState = mock(SessionState.class);
    CommandProcessor mockCommandProcessor = mock(CommandProcessor.class);

    ArgumentCaptor<String> commandArgsCaptor = ArgumentCaptor.forClass(String.class);

    when(mockHiveSession.getHiveConf()).thenReturn(conf);
    when(mockHiveSession.getSessionState()).thenReturn(mockSessionState);
    when(mockSessionState.getTmpOutputFile()).thenReturn(tmpFile);
    when(mockSessionState.getTmpErrOutputFile()).thenReturn(tmpFile);
    when(mockCommandProcessor.run(commandArgsCaptor.capture())).thenReturn(new CommandProcessorResponse(0));

    HiveCommandOperation hiveCommandOperation = new HiveCommandOperation(mockHiveSession,
           " set a = b", mockCommandProcessor, null);
    hiveCommandOperation.runInternal();

    String commandArgs = commandArgsCaptor.getValue();
    assertEquals("a = b", commandArgs);
  }
}
