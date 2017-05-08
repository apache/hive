package org.apache.hadoop.hive.ql.exec.tez.monitoring;

import org.apache.hadoop.hive.ql.session.SessionState;

interface PrintSummary {
  void print(SessionState.LogHelper console);
}
