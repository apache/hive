package org.apache.hive.hcatalog.streaming.mutate.worker;

import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies that the sequence of {@link RecordIdentifier RecordIdentifiers} are in a valid order for insertion into an
 * ACID delta file in a given partition and bucket.
 */
class SequenceValidator {

  private static final Logger LOG = LoggerFactory.getLogger(SequenceValidator.class);

  private Long lastTxId;
  private Long lastRowId;

  SequenceValidator() {
  }

  boolean isInSequence(RecordIdentifier recordIdentifier) {
    if (lastTxId != null && recordIdentifier.getTransactionId() < lastTxId) {
      LOG.debug("Non-sequential transaction ID. Expected >{}, recordIdentifier={}", lastTxId, recordIdentifier);
      return false;
    } else if (lastTxId != null && recordIdentifier.getTransactionId() == lastTxId && lastRowId != null
        && recordIdentifier.getRowId() <= lastRowId) {
      LOG.debug("Non-sequential row ID. Expected >{}, recordIdentifier={}", lastRowId, recordIdentifier);
      return false;
    }
    lastTxId = recordIdentifier.getTransactionId();
    lastRowId = recordIdentifier.getRowId();
    return true;
  }

  /**
   * Validator must be reset for each new partition and or bucket.
   */
  void reset() {
    lastTxId = null;
    lastRowId = null;
    LOG.debug("reset");
  }

  @Override
  public String toString() {
    return "SequenceValidator [lastTxId=" + lastTxId + ", lastRowId=" + lastRowId + "]";
  }

}
