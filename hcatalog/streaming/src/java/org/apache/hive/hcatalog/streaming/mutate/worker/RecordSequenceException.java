package org.apache.hive.hcatalog.streaming.mutate.worker;

public class RecordSequenceException extends WorkerException {

  private static final long serialVersionUID = 1L;

  RecordSequenceException(String message) {
    super(message);
  }

}
