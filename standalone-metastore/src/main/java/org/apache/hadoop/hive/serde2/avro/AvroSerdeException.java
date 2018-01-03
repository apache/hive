package org.apache.hadoop.hive.serde2.avro;

public class AvroSerdeException extends Exception {
  public AvroSerdeException(String s, Exception ex) {
    super(s, ex);
  }

  public AvroSerdeException(String msg) {
    super(msg);
  }
}
