package org.apache.hadoop.hive.ql.exec.tez.monitoring;

import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.text.DecimalFormat;

import static org.apache.hadoop.hive.ql.exec.tez.monitoring.Constants.SEPARATOR;

class QueryExecutionBreakdownSummary implements PrintSummary {
  // Methods summary
  private static final String OPERATION_SUMMARY = "%-35s %9s";
  private static final String OPERATION = "OPERATION";
  private static final String DURATION = "DURATION";


  private DecimalFormat decimalFormat = new DecimalFormat("#0.00");
  private PerfLogger perfLogger;

  private final Long compileEndTime;
  private final Long dagSubmitStartTime;
  private final Long submitToRunningDuration;

  QueryExecutionBreakdownSummary(PerfLogger perfLogger) {
    this.perfLogger = perfLogger;
    this.compileEndTime = perfLogger.getEndTime(PerfLogger.COMPILE);
    this.dagSubmitStartTime = perfLogger.getStartTime(PerfLogger.TEZ_SUBMIT_DAG);
    this.submitToRunningDuration = perfLogger.getDuration(PerfLogger.TEZ_SUBMIT_TO_RUNNING);
  }

  private String formatNumber(long number) {
    return decimalFormat.format(number / 1000.0) + "s";
  }

  private String format(String value, long number) {
    return String.format(OPERATION_SUMMARY, value, formatNumber(number));
  }

  public void print(SessionState.LogHelper console) {
    console.printInfo("Query Execution Summary");

    String execBreakdownHeader = String.format(OPERATION_SUMMARY, OPERATION, DURATION);
    console.printInfo(SEPARATOR);
    console.printInfo(execBreakdownHeader);
    console.printInfo(SEPARATOR);

    // parse, analyze, optimize and compile
    long compile = compileEndTime - perfLogger.getStartTime(PerfLogger.COMPILE);
    console.printInfo(format("Compile Query", compile));

    // prepare plan for submission (building DAG, adding resources, creating scratch dirs etc.)
    long totalDAGPrep = dagSubmitStartTime - compileEndTime;
    console.printInfo(format("Prepare Plan", totalDAGPrep));

    // submit to accept dag (if session is closed, this will include re-opening of session time,
    // localizing files for AM, submitting DAG)
    long submitToAccept = perfLogger.getStartTime(PerfLogger.TEZ_RUN_DAG) - dagSubmitStartTime;
    console.printInfo(format("Submit Plan", submitToAccept));

    // accept to start dag (schedule wait time, resource wait time etc.)
    console.printInfo(format("Start DAG", submitToRunningDuration));

    // time to actually run the dag (actual dag runtime)
    final long startToEnd;
    if (submitToRunningDuration == 0) {
      startToEnd = perfLogger.getDuration(PerfLogger.TEZ_RUN_DAG);
    } else {
      startToEnd = perfLogger.getEndTime(PerfLogger.TEZ_RUN_DAG) -
          perfLogger.getEndTime(PerfLogger.TEZ_SUBMIT_TO_RUNNING);
    }
    console.printInfo(format("Run DAG", startToEnd));
    console.printInfo(SEPARATOR);
    console.printInfo("");
  }
}
