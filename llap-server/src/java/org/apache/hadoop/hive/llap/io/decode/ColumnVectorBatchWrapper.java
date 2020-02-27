package org.apache.hadoop.hive.llap.io.decode;

import org.apache.hadoop.hive.llap.io.api.impl.ColumnVectorBatch;
import org.apache.orc.impl.TreeReaderFactory;

public class ColumnVectorBatchWrapper {
  private ColumnVectorBatch cvb;
  private TreeReaderFactory.FilterContext fcnx = null;

  ColumnVectorBatchWrapper(int numCols) {
    this.cvb = new ColumnVectorBatch(numCols);
    this.fcnx = new TreeReaderFactory.FilterContext();
  }

  public ColumnVectorBatch getCvb() {
    return cvb;
  }

  public void setFilterContext(TreeReaderFactory.FilterContext cntx) {
    this.fcnx = cntx;
  }

  public TreeReaderFactory.FilterContext getFilterContext(){
    return fcnx;
  }

  public int[] getSelected() {
    return fcnx.getSelected();
  }

  public boolean isSelectedInUse() {
    return fcnx.isSelectedInUse();
  }

  public int getSelectedSize() {
    return fcnx.getSelectedSize();
  }

  public void resetCnx() {
    this.fcnx.setSelectedSize(0);
    this.fcnx.setSelected(null);
    this.fcnx.setSelectedInUse(false);
  }
}
