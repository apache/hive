package org.apache.hive.service.cli;

import org.apache.hive.service.rpc.thrift.TJobExecutionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TezProgressMonitorStatusMapper implements ProgressMonitorStatusMapper {
  private final Logger LOG = LoggerFactory.getLogger(TezProgressMonitorStatusMapper.class.getName());

  /**
   * These states are taken form DAGStatus.State, could not use that here directly as it was
   * optional dependency and did not want to include it just for the enum.
   */
  enum TezStatus {
    SUBMITTED, INITING, RUNNING, SUCCEEDED, KILLED, FAILED, ERROR

  }

  @Override
  public TJobExecutionStatus forStatus(String status) {
    try {
      TezStatus tezStatus = TezStatus.valueOf(status);
      switch (tezStatus) {
      case SUBMITTED:
      case INITING:
      case RUNNING:
        return TJobExecutionStatus.IN_PROGRESS;
      default:
        return TJobExecutionStatus.COMPLETE;
      }
    } catch (IllegalArgumentException ex) {
      LOG.debug("Could not understand the status from tez execution engine : " + status);
      return TJobExecutionStatus.NOT_AVAILABLE;
    }
  }
}
