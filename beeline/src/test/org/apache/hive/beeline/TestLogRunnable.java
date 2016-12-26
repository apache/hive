package org.apache.hive.beeline;

import org.apache.hive.beeline.display.InPlaceUpdate;
import org.apache.hive.beeline.display.Util;
import org.apache.hive.jdbc.HiveStatement;
import org.apache.hive.service.rpc.thrift.TJobExecutionStatus;
import org.apache.hive.service.rpc.thrift.TProgressUpdateResp;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
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
@PrepareForTest({Util.class, Commands.LogRunnable.class})
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
    mockStatic(Util.class);
    when(Util.canDisplayInPlace()).thenReturn(true);
    logRunnable = new Commands.LogRunnable(commands, hiveStatement);
  }

  @After
  public void assertStatic() {
    verifyStatic();
    Util.canDisplayInPlace();
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
    verify(inplaceUpdate).render(any(TProgressUpdateResp.class));
  }


}