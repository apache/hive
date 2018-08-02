package org.apache.hadoop.hive.registry.serdes.avro.exceptions;

import org.apache.hadoop.hive.registry.serdes.SerDesException;

/**
 * This is thrown when an exception is caused due to Avro to binary (or vice versa) conversion.
 * This should be the default, unless the caught exception is retryable,
 * where {@link org.apache.hadoop.hive.registry.serdes.avro.exceptions.AvroRetryableException}
 * should be thrown instead.
 */
public class AvroException extends SerDesException {

  public AvroException() {
  }

  public AvroException(String message) {
    super(message);
  }

  public AvroException(String message, Throwable cause) {
    super(message, cause);
  }

  public AvroException(Throwable cause) {
    super(cause);
  }

  public AvroException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}

