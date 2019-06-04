package org.apache.hadoop.hive.metastore;

public abstract class ObjectStoreTestHook {

  public static ObjectStoreTestHook instance = null;

  public abstract void scheduledQueryPoll();

  static void onScheduledQueryPoll() {
    if (instance != null) {
      instance.scheduledQueryPoll();
    }
  }
}
