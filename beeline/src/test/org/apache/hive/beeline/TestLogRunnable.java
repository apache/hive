package org.apache.hive.beeline;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.log.InPlaceUpdate;
import org.apache.hadoop.hive.ql.log.ProgressMonitor;
import org.apache.hive.jdbc.HiveStatement;
import org.apache.hive.service.rpc.thrift.TJobExecutionStatus;
import org.apache.hive.service.rpc.thrift.TProgressUpdateResp;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.*;

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

  @Before
  public void setup() {
    mockStatic(InPlaceUpdate.class);
    when(InPlaceUpdate.canRenderInPlace(any(HiveConf.class))).thenReturn(true);
    logRunnable = new Commands.LogRunnable(commands, hiveStatement, 0L);
  }

  @After
  public void assertStatic() {
    verifyStatic();
    InPlaceUpdate.canRenderInPlace(any(HiveConf.class));
  }

  @Test
  public void shouldNotQueryLogWhenInPlaceUpdateAvailable() throws Exception {
    when(hiveStatement.hasMoreLogs()).thenReturn(true).thenReturn(false);
    when(hiveStatement.getProgressResponse()).thenReturn(new TProgressUpdateResp(
      Collections.<String>emptyList(), Collections.<List<String>>emptyList(),
      0.0D,
      TJobExecutionStatus.INITING,
      "", 0L));

    whenNew(InPlaceUpdate.class).withNoArguments().thenReturn(inplaceUpdate);

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
    Mockito.verifyZeroInteractions(inplaceUpdate);
  }
 }