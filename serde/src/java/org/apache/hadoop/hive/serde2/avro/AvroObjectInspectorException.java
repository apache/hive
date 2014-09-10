package org.apache.hadoop.hive.serde2.avro;

/**
 * Exception for the {@link AvroLazyObjectInspector}
 * */
public class AvroObjectInspectorException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public AvroObjectInspectorException() {
    super();
  }

  public AvroObjectInspectorException(String message) {
    super(message);
  }

  public AvroObjectInspectorException(Throwable cause) {
    super(cause);
  }

  public AvroObjectInspectorException(String message, Throwable cause) {
    super(message, cause);
  }
}