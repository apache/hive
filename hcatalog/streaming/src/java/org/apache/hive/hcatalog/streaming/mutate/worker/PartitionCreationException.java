package org.apache.hive.hcatalog.streaming.mutate.worker;

public class PartitionCreationException extends WorkerException {

  private static final long serialVersionUID = 1L;

  PartitionCreationException(String message, Throwable cause) {
    super(message, cause);
  }

  PartitionCreationException(String message) {
    super(message);
  }

}
