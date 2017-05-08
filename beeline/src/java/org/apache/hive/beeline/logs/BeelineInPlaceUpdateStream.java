package org.apache.hive.beeline.logs;

import org.apache.hadoop.hive.common.log.InPlaceUpdate;
import org.apache.hadoop.hive.common.log.ProgressMonitor;
import org.apache.hive.jdbc.logs.InPlaceUpdateStream;
import org.apache.hive.service.rpc.thrift.TJobExecutionStatus;
import org.apache.hive.service.rpc.thrift.TProgressUpdateResp;

import java.io.PrintStream;
import java.util.List;

public class BeelineInPlaceUpdateStream implements InPlaceUpdateStream {
  private InPlaceUpdate inPlaceUpdate;
  private EventNotifier notifier;

  public BeelineInPlaceUpdateStream(PrintStream out, InPlaceUpdateStream.EventNotifier notifier) {
    this.inPlaceUpdate = new InPlaceUpdate(out);
    this.notifier = notifier;
  }

  @Override
  public void update(TProgressUpdateResp response) {
    if (response == null || response.getStatus().equals(TJobExecutionStatus.NOT_AVAILABLE)) {
      /*
        we set it to completed if there is nothing the server has to report
        for example, DDL statements
      */
      notifier.progressBarCompleted();
    } else if (notifier.isOperationLogUpdatedAtLeastOnce()) {
      /*
        try to render in place update progress bar only if the operations logs is update at least once
        as this will hopefully allow printing the metadata information like query id, application id
        etc. have to remove these notifiers when the operation logs get merged into GetOperationStatus
      */
      inPlaceUpdate.render(new ProgressMonitorWrapper(response));
    }
  }

  @Override
  public EventNotifier getEventNotifier() {
    return notifier;
  }

  static class ProgressMonitorWrapper implements ProgressMonitor {
    private TProgressUpdateResp response;

    ProgressMonitorWrapper(TProgressUpdateResp response) {
      this.response = response;
    }

    @Override
    public List<String> headers() {
      return response.getHeaderNames();
    }

    @Override
    public List<List<String>> rows() {
      return response.getRows();
    }

    @Override
    public String footerSummary() {
      return response.getFooterSummary();
    }

    @Override
    public long startTime() {
      return response.getStartTime();
    }

    @Override
    public String executionStatus() {
      throw new UnsupportedOperationException(
          "This should never be used for anything. All the required data is available via other methods"
      );
    }

    @Override
    public double progressedPercentage() {
      return response.getProgressedPercentage();
    }
  }
}
