package org.apache.hadoop.hive.ql.plan.ptf;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.PTFUtils;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.TypeCheckCtx;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class ShapeDetails {
  String serdeClassName;
  Map<String, String> serdeProps;
  List<String> columnNames;
  transient StructObjectInspector OI;
  transient SerDe serde;
  transient RowResolver rr;
  transient TypeCheckCtx typeCheckCtx;

  static {
    PTFUtils.makeTransient(ShapeDetails.class, "OI", "serde", "rr", "typeCheckCtx");
  }

  public String getSerdeClassName() {
    return serdeClassName;
  }

  public void setSerdeClassName(String serdeClassName) {
    this.serdeClassName = serdeClassName;
  }

  public Map<String, String> getSerdeProps() {
    return serdeProps;
  }

  public void setSerdeProps(Map<String, String> serdeProps) {
    this.serdeProps = serdeProps;
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public void setColumnNames(List<String> columnNames) {
    this.columnNames = columnNames;
  }

  public StructObjectInspector getOI() {
    return OI;
  }

  public void setOI(StructObjectInspector oI) {
    OI = oI;
  }

  public SerDe getSerde() {
    return serde;
  }

  public void setSerde(SerDe serde) {
    this.serde = serde;
  }

  public RowResolver getRr() {
    return rr;
  }

  public void setRr(RowResolver rr) {
    this.rr = rr;
  }

  public TypeCheckCtx getTypeCheckCtx() {
    return typeCheckCtx;
  }

  public void setTypeCheckCtx(TypeCheckCtx typeCheckCtx) {
    this.typeCheckCtx = typeCheckCtx;
  }
}