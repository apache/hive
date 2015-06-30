package org.apache.hive.hcatalog.streaming.mutate.worker;

public class GroupRevisitedException extends WorkerException {

  private static final long serialVersionUID = 1L;

  GroupRevisitedException(String message) {
    super(message);
  }

}
