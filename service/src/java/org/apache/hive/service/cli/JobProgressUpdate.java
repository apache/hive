package org.apache.hive.service.cli;

import org.apache.hadoop.hive.ql.log.ProgressMonitor;

import java.util.List;

public class JobProgressUpdate {
  public final double progressedPercentage;
  public final JobExecutionStatus jobExecutionStatus;
  public final String footerSummary;
  public final long startTimeMillis;
  private final List<String> headers;
  private final List<List<String>> rows;


  JobProgressUpdate(ProgressMonitor monitor) {
    this(monitor.headers(), monitor.rows(), monitor.footerSummary(), monitor.progressedPercentage(),
      JobExecutionStatus.fromString(monitor.executionStatus()), monitor.startTime());
  }

  public JobProgressUpdate(List<String> headers, List<List<String>> rows, String footerSummary,
                           double progressedPercentage, JobExecutionStatus jobExecutionStatus,
                           long startTimeMillis) {
    this.progressedPercentage = progressedPercentage;
    this.jobExecutionStatus = jobExecutionStatus;
    this.footerSummary = footerSummary;
    this.startTimeMillis = startTimeMillis;
    this.headers = headers;
    this.rows = rows;
  }

  public List<String> headers() {
    return headers;
  }

  public List<List<String>> rows() {
    return rows;
  }
}
