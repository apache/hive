package org.apache.hadoop.hive.ql.plan.ptf;


public class RangeBoundaryDef extends BoundaryDef {
  private int amt;

  public int compareTo(BoundaryDef other) {
    int c = getDirection().compareTo(other.getDirection());
    if (c != 0) {
      return c;
    }
    RangeBoundaryDef rb = (RangeBoundaryDef) other;
    return getAmt() - rb.getAmt();
  }

  @Override
  public int getAmt() {
    return amt;
  }

  public void setAmt(int amt) {
    this.amt = amt;
  }
}