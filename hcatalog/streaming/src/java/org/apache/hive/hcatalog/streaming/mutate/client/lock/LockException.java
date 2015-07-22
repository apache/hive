package org.apache.hive.hcatalog.streaming.mutate.client.lock;

public class LockException extends Exception {

  private static final long serialVersionUID = 1L;

  public LockException(String message) {
    super(message);
  }

  public LockException(String message, Throwable cause) {
    super(message, cause);
  }

}
