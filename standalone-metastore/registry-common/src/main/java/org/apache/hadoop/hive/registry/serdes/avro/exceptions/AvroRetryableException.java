package org.apache.hadoop.hive.registry.serdes.avro.exceptions;

import org.apache.hadoop.hive.registry.common.exception.RetryableException;

/**
 * This is thrown when an exception is caused due to Avro to binary (or vice versa) conversion,
 * and the exception is deemed retry-able, e.g. cause being IOException.
 */
public class AvroRetryableException extends AvroException implements RetryableException {

  public AvroRetryableException() {
  }

  public AvroRetryableException(String message) {
    super(message);
  }

  public AvroRetryableException(String message, Throwable cause) {
    super(message, cause);
  }

  public AvroRetryableException(Throwable cause) {
    super(cause);
  }

  public AvroRetryableException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

}
