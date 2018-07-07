package org.apache.hadoop.hive.registry.common.exception;

import org.apache.hadoop.hive.registry.serdes.SerDesException;

/**
 * This is thrown when an exception is caused due to the Schema Registry.
 * This should be the default, unless the caught exception is retryable,
 * where {@link org.apache.hadoop.hive.registry.common.exception.RegistryRetryableException}
 * should be thrown instead.
 */
public class RegistryException extends SerDesException {

  public RegistryException() {
  }

  public RegistryException(String message) {
    super(message);
  }

  public RegistryException(String message, Throwable cause) {
    super(message, cause);
  }

  public RegistryException(Throwable cause) {
    super(cause);
  }

  public RegistryException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
