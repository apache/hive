package org.apache.hadoop.hive.ql.testutil;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.hive.ql.exec.CollectOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class OperatorTestUtils {

  public static ExprNodeColumnDesc getStringColumn(String columnName) {
    return new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, columnName, "", false);
  }

  /**
   *
   * @param expressionList
   * @return A list of columns named _colX where x is a number
   */
  public static List<String> createOutputColumnNames(List<ExprNodeDesc> expressionList){
    List<String> outputCols = new ArrayList<String>();
    for (int i = 0; i < expressionList.size(); i++) {
      outputCols.add("_col" + i);
    }
    return outputCols;
  }

  /**
   * Given a select operator and a collectOperator feed the sourceData into the operator
   * tree and assert that each row matches the expectedResult
   * @param selectOp
   * @param collectOp
   * @param sourceData
   * @param expected
   * @throws HiveException
   */
  public static void assertResults(Operator<SelectDesc> selectOp, CollectOperator collectOp,
      InspectableObject [] sourceData, InspectableObject [] expected) throws HiveException {
    InspectableObject resultRef = new InspectableObject();
    for (int i = 0; i < sourceData.length; i++) {
      selectOp.processOp(sourceData[i].o, 0);
      collectOp.retrieve(resultRef);
      StructObjectInspector expectedOi = (StructObjectInspector) expected[i].oi;
      List<? extends StructField> expectedFields = expectedOi.getAllStructFieldRefs();
      StructObjectInspector destinationOi = (StructObjectInspector) resultRef.oi;
      List<? extends StructField> destinationFields = destinationOi.getAllStructFieldRefs();
      Assert.assertEquals("Source and destination have differing numbers of fields ", expectedFields.size(), destinationFields.size());
      for (StructField field : expectedFields){
        StructField dest = expectedOi.getStructFieldRef(field.getFieldName());
        Assert.assertNotNull("Cound not find column named "+field.getFieldName(), dest);
        Assert.assertEquals(field.getFieldObjectInspector(), dest.getFieldObjectInspector());
        Assert.assertEquals("comparing " +
            expectedOi.getStructFieldData(expected[i].o, field)+" "+
            field.getFieldObjectInspector().getClass().getSimpleName()+" to "+
            destinationOi.getStructFieldData(resultRef.o, dest) + " " +
            dest.getFieldObjectInspector().getClass().getSimpleName(), 0,
            ObjectInspectorUtils.compare(
            expectedOi.getStructFieldData(expected[i].o, field), field.getFieldObjectInspector(),
            destinationOi.getStructFieldData(resultRef.o, dest), dest.getFieldObjectInspector()
            )
        );
      }

    }
    selectOp.close(false);
  }

}
