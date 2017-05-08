package org.apache.hive.jdbc.logs;

import org.apache.hive.service.rpc.thrift.TProgressUpdateResp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface InPlaceUpdateStream {
  void update(TProgressUpdateResp response);

  InPlaceUpdateStream NO_OP = new InPlaceUpdateStream() {
    private final EventNotifier eventNotifier = new EventNotifier();
    @Override
    public void update(TProgressUpdateResp response) {

    }

    @Override
    public EventNotifier getEventNotifier() {
      return eventNotifier;
    }

  };

  EventNotifier getEventNotifier();

  class EventNotifier {
    public static final Logger LOG = LoggerFactory.getLogger(EventNotifier.class.getName());
    boolean isComplete = false;
    boolean isOperationLogUpdatedOnceAtLeast = false;

    public synchronized void progressBarCompleted() {
      LOG.debug("progress bar is complete");
      this.isComplete = true;
    }

    private synchronized boolean isProgressBarComplete() {
      return isComplete;

    }

    public synchronized void operationLogShowedToUser() {
      LOG.debug("operations log is shown to the user");
      isOperationLogUpdatedOnceAtLeast = true;
    }

    public synchronized boolean isOperationLogUpdatedAtLeastOnce() {
      return isOperationLogUpdatedOnceAtLeast;
    }

    public boolean canOutputOperationLogs() {
      return !isOperationLogUpdatedAtLeastOnce() || isProgressBarComplete();
    }
  }
}
