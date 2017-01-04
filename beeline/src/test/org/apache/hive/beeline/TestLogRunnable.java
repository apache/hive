package org.apache.hive.beeline;

import org.apache.hadoop.hive.ql.log.InPlaceUpdate;
import org.apache.hadoop.hive.ql.log.ProgressMonitor;
import org.apache.hive.jdbc.HiveStatement;
import org.apache.hive.service.rpc.thrift.TJobExecutionStatus;
import org.apache.hive.service.rpc.thrift.TProgressUpdateResp;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.*;
import static org.powermock.api.mockito.PowerMockito.verifyNoMoreInteractions;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({InPlaceUpdate.class, Commands.LogRunnable.class})
@PowerMockIgnore("javax.management.*")
public class TestLogRunnable {

  @Mock
  private Commands commands;
  @Mock
  private HiveStatement hiveStatement;
  @Mock
  private InPlaceUpdate inplaceUpdate;
  private Commands.LogRunnable logRunnable;
  @Mock
  private BeeLine beeline;

  @Before
  public void setup() throws Exception {
    logRunnable = new Commands.LogRunnable(commands, hiveStatement, 0L);
    whenNew(InPlaceUpdate.class).withAnyArguments().thenReturn(inplaceUpdate);
    Whitebox.setInternalState(commands, "beeLine", beeline);
  }

  @Test
  public void shouldNotQueryLogWhenInPlaceUpdateAvailable() throws Exception {
    when(hiveStatement.hasMoreLogs()).thenReturn(true).thenReturn(false);
    TProgressUpdateResp response = new TProgressUpdateResp(
      Collections.<String>emptyList(), new ArrayList<List<String>>(),
      0.0D,
      TJobExecutionStatus.INITING,
      "", 0L);
    when(hiveStatement.getProgressResponse()).thenReturn(response);

    logRunnable.run();

    verify(hiveStatement, times(2)).hasMoreLogs();
    verify(hiveStatement).getProgressResponse();
    verifyNoMoreInteractions(hiveStatement);
    verify(inplaceUpdate).render(any(ProgressMonitor.class));
  }

  @Test
  public void shouldNotTryToRenderWhenJobStatusIsNotAvailable() throws Exception {
    when(hiveStatement.hasMoreLogs()).thenReturn(true).thenReturn(false);
    when(hiveStatement.getProgressResponse()).thenReturn(new TProgressUpdateResp(
      Collections.<String>emptyList(), Collections.<List<String>>emptyList(),
      0.0D,
      TJobExecutionStatus.NOT_AVAILABLE,
      "", 0L));

    logRunnable.run();

    verify(hiveStatement, times(2)).hasMoreLogs();
    verify(hiveStatement).getProgressResponse();
    verifyNoMoreInteractions(hiveStatement);
    verifyNew(InPlaceUpdate.class, never()).withArguments(Matchers.any());
  }
}