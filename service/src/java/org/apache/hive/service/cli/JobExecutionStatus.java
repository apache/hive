package org.apache.hive.service.cli;

public enum JobExecutionStatus {
  SUBMITTED((short) 0),

  INITING((short) 1),

  RUNNING((short) 2),

  SUCCEEDED((short) 3),

  KILLED((short) 4),

  FAILED((short) 5),

  ERROR((short) 6),

  NOT_AVAILABLE((short) 7);

  private final short executionStatusOrdinal;

  JobExecutionStatus(short executionStatusOrdinal) {
    this.executionStatusOrdinal = executionStatusOrdinal;
  }

  public short toExecutionStatus() {
    return executionStatusOrdinal;
  }

  public static JobExecutionStatus fromString(String input) {
    for (JobExecutionStatus status : values()) {
      if (status.name().equals(input))
        return status;
    }
    return NOT_AVAILABLE;
  }
}
