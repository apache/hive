package org.apache.hive.hcatalog.streaming.mutate.worker;

public class BucketIdException extends WorkerException {

  private static final long serialVersionUID = 1L;

  BucketIdException(String message) {
    super(message);
  }

}
