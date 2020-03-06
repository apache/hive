package org.apache.hadoop.hive.llap.io.decode;

import org.apache.hadoop.hive.llap.io.api.impl.ColumnVectorBatch;
import org.apache.orc.impl.TreeReaderFactory;

public class ColumnVectorBatchWrapper {
  private ColumnVectorBatch cvb;
  private TreeReaderFactory.MutableFilterContext fcnx;

  ColumnVectorBatchWrapper(int numCols) {
    this.cvb = new ColumnVectorBatch(numCols);
    this.fcnx = new TreeReaderFactory.MutableFilterContext();
  }

  public ColumnVectorBatch getCvb() {
    return cvb;
  }

  public TreeReaderFactory.MutableFilterContext getFilterContext(){
    return fcnx;
  }
}
