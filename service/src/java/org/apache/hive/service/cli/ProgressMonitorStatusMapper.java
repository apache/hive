package org.apache.hive.service.cli;

import org.apache.hive.service.rpc.thrift.TJobExecutionStatus;

/**
 * This defines the mapping between the internal execution status of various engines and the
 * generic states that the progress monitor cares about. Theses are specified by TJobExecutionStatus
 */
public interface ProgressMonitorStatusMapper {

  ProgressMonitorStatusMapper DEFAULT = new ProgressMonitorStatusMapper() {
    @Override
    public TJobExecutionStatus forStatus(String status) {
      return TJobExecutionStatus.NOT_AVAILABLE;
    }
  };

  TJobExecutionStatus forStatus(String status);
}
