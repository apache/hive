package org.apache.hive.hcatalog.streaming.mutate.client;

public class ClientException extends Exception {

  private static final long serialVersionUID = 1L;

  ClientException(String message, Throwable cause) {
    super(message, cause);
  }

  ClientException(String message) {
    super(message);
  }

}
