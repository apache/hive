package org.apache.hadoop.hive.ql;

public class CommandNeedRetryException extends Exception {
  public CommandNeedRetryException() {
    super();
  }

  public CommandNeedRetryException(String message) {
    super(message);
  }

  public CommandNeedRetryException(Throwable cause) {
    super(cause);
  }

  public CommandNeedRetryException(String message, Throwable cause) {
    super(message, cause);
  }
}
