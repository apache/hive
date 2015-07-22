package org.apache.hive.hcatalog.streaming.mutate.client.lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Provides a means to handle the situation when a held lock fails. */
public interface LockFailureListener {

  static final Logger LOG = LoggerFactory.getLogger(LockFailureListener.class);

  static final LockFailureListener NULL_LISTENER = new LockFailureListener() {
    @Override
    public void lockFailed(long lockId, Long transactionId, Iterable<String> tableNames, Throwable t) {
      LOG.warn(
          "Ignored lock failure: lockId=" + lockId + ", transactionId=" + transactionId + ", tables=" + tableNames, t);
    }
    
    public String toString() {
      return LockFailureListener.class.getName() + ".NULL_LISTENER";
    }
  };

  /** Called when the specified lock has failed. You should probably abort your job in this case. */
  void lockFailed(long lockId, Long transactionId, Iterable<String> tableNames, Throwable t);

}
