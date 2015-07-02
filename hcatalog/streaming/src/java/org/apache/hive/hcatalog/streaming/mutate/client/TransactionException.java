package org.apache.hive.hcatalog.streaming.mutate.client;

public class TransactionException extends ClientException {

  private static final long serialVersionUID = 1L;

  TransactionException(String message, Throwable cause) {
    super(message, cause);
  }

  TransactionException(String message) {
    super(message);
  }

}
