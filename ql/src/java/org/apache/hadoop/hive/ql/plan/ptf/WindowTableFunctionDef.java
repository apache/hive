package org.apache.hadoop.hive.ql.plan.ptf;

import java.util.List;


public class WindowTableFunctionDef extends PartitionedTableFunctionDef {
  List<WindowFunctionDef> windowFunctions;

  public List<WindowFunctionDef> getWindowFunctions() {
    return windowFunctions;
  }
  public void setWindowFunctions(List<WindowFunctionDef> windowFunctions) {
    this.windowFunctions = windowFunctions;
  }
}
