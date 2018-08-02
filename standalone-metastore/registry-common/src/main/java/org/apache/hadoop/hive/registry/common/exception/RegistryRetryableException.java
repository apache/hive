package org.apache.hadoop.hive.registry.common.exception;

/**
 * This is thrown when an exception is caused due to the Schema Registry,
 * and the exception is deemed retry-able, e.g. cause being IOException.
 */
public class RegistryRetryableException extends RegistryException implements RetryableException {

  public RegistryRetryableException() {
  }

  public RegistryRetryableException(String message) {
    super(message);
  }

  public RegistryRetryableException(String message, Throwable cause) {
    super(message, cause);
  }

  public RegistryRetryableException(Throwable cause) {
    super(cause);
  }

  public RegistryRetryableException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
