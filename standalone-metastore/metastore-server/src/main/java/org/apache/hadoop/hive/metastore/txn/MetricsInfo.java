package org.apache.hadoop.hive.metastore.txn;

/**
 * ACID metrics info object.
 */
public class MetricsInfo {

  private int txnToWriteIdRowCount;
  private int completedTxnsRowCount;

  public int getTxnToWriteIdRowCount() {
    return txnToWriteIdRowCount;
  }

  public void setTxnToWriteIdRowCount(int txnToWriteIdRowCount) {
    this.txnToWriteIdRowCount = txnToWriteIdRowCount;
  }

  public int getCompletedTxnsRowCount() {
    return completedTxnsRowCount;
  }

  public void setCompletedTxnsRowCount(int completedTxnsRowCount) {
    this.completedTxnsRowCount = completedTxnsRowCount;
  }
}
