package org.apache.hadoop.hive.common.log;

import java.util.Collections;
import java.util.List;

public interface ProgressMonitor {

  ProgressMonitor NULL = new ProgressMonitor() {
    @Override
    public List<String> headers() {
      return Collections.emptyList();
    }

    @Override
    public List<List<String>> rows() {
      return Collections.emptyList();
    }

    @Override
    public String footerSummary() {
      return "";
    }

    @Override
    public long startTime() {
      return 0;
    }

    @Override
    public String executionStatus() {
      return "";
    }

    @Override
    public double progressedPercentage() {
      return 0;
    }
  };

  List<String> headers();

  List<List<String>> rows();

  String footerSummary();

  long startTime();

  String executionStatus();

  double progressedPercentage();
}
