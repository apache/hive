package org.apache.hadoop.hive.ql.plan.ptf;


public class WindowFrameDef {
  private BoundaryDef start;
  private BoundaryDef end;

  public BoundaryDef getStart() {
    return start;
  }

  public void setStart(BoundaryDef start) {
    this.start = start;
  }

  public BoundaryDef getEnd() {
    return end;
  }

  public void setEnd(BoundaryDef end) {
    this.end = end;
  }
}