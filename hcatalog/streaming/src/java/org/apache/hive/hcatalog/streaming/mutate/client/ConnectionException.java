package org.apache.hive.hcatalog.streaming.mutate.client;

public class ConnectionException extends ClientException {

  private static final long serialVersionUID = 1L;

  ConnectionException(String message, Throwable cause) {
    super(message, cause);
  }

  ConnectionException(String message) {
    super(message);
  }

}
