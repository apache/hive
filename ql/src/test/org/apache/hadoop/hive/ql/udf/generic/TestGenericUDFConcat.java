package org.apache.hadoop.hive.ql.udf.generic;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.testutil.DataBuilder;
import org.apache.hadoop.hive.ql.testutil.OperatorTestUtils;
import org.apache.hadoop.hive.ql.testutil.BaseScalarUdfTest;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class TestGenericUDFConcat extends BaseScalarUdfTest {

  @Override
  public InspectableObject[] getBaseTable() {
    DataBuilder db = new DataBuilder();
    db.setColumnNames("a", "b", "c");
    db.setColumnTypes(
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    db.addRow("one", "two", "three");
    db.addRow("four","two", "three");
    db.addRow( null, "two", "three");
    return db.createRows();
  }

  @Override
  public InspectableObject[] getExpectedResult() {
    DataBuilder db = new DataBuilder();
    db.setColumnNames("_col1", "_col2");
    db.setColumnTypes(PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    db.addRow("one", "onetwo");
    db.addRow("four", "fourtwo");
    db.addRow(null, null);
    return db.createRows();
  }

  @Override
  public List<ExprNodeDesc> getExpressionList() throws UDFArgumentException {
    ExprNodeDesc expr1 = OperatorTestUtils.getStringColumn("a");
    ExprNodeDesc expr2 = OperatorTestUtils.getStringColumn("b");
    ExprNodeDesc exprDesc2 = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("concat", expr1, expr2);
    List<ExprNodeDesc> earr = new ArrayList<ExprNodeDesc>();
    earr.add(expr1);
    earr.add(exprDesc2);
    return earr;
  }

}
