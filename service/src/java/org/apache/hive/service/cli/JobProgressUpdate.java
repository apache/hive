package org.apache.hive.service.cli;

import org.apache.hadoop.hive.common.log.ProgressMonitor;

import java.util.List;

public class JobProgressUpdate {
  public final double progressedPercentage;
  public final String footerSummary;
  public final long startTimeMillis;
  private final List<String> headers;
  private final List<List<String>> rows;
  public final String status;


  JobProgressUpdate(ProgressMonitor monitor) {
    this(monitor.headers(), monitor.rows(), monitor.footerSummary(), monitor.progressedPercentage(),
        monitor.startTime(), monitor.executionStatus());
  }

  private JobProgressUpdate(List<String> headers, List<List<String>> rows, String footerSummary,
      double progressedPercentage, long startTimeMillis, String status) {
    this.progressedPercentage = progressedPercentage;
    this.footerSummary = footerSummary;
    this.startTimeMillis = startTimeMillis;
    this.headers = headers;
    this.rows = rows;
    this.status = status;
  }

  public List<String> headers() {
    return headers;
  }

  public List<List<String>> rows() {
    return rows;
  }
}
