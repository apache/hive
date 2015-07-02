package org.apache.hive.hcatalog.streaming.mutate.worker;

public class WorkerException extends Exception {

  private static final long serialVersionUID = 1L;

  WorkerException(String message, Throwable cause) {
    super(message, cause);
  }

  WorkerException(String message) {
    super(message);
  }

}
