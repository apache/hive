package org.apache.hadoop.hive.ql.exec.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterExprAndExpr;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterExprOrExpr;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDoubleColLessDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColGreaterLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringColGreaterStringScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColAddLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColModuloLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColMultiplyLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColSubtractLongColumn;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.udf.UDFOPMinus;
import org.apache.hadoop.hive.ql.udf.UDFOPMod;
import org.apache.hadoop.hive.ql.udf.UDFOPMultiply;
import org.apache.hadoop.hive.ql.udf.UDFOPPlus;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.junit.Test;

public class TestVectorizationContext {

  @Test
  public void testArithmeticExpressionVectorization() throws HiveException {
    /**
     * Create original expression tree for following
     * (plus (minus (plus col1 col2) col3) (multiply col4 (mod col5 col6)) )
     */
    GenericUDFBridge udf1 = new GenericUDFBridge("+", true, UDFOPPlus.class);
    GenericUDFBridge udf2 = new GenericUDFBridge("-", true, UDFOPMinus.class);
    GenericUDFBridge udf3 = new GenericUDFBridge("*", true, UDFOPMultiply.class);
    GenericUDFBridge udf4 = new GenericUDFBridge("+", true, UDFOPPlus.class);
    GenericUDFBridge udf5 = new GenericUDFBridge("%", true, UDFOPMod.class);

    ExprNodeGenericFuncDesc sumExpr = new ExprNodeGenericFuncDesc();
    sumExpr.setGenericUDF(udf1);
    ExprNodeGenericFuncDesc minusExpr = new ExprNodeGenericFuncDesc();
    minusExpr.setGenericUDF(udf2);
    ExprNodeGenericFuncDesc multiplyExpr = new ExprNodeGenericFuncDesc();
    multiplyExpr.setGenericUDF(udf3);
    ExprNodeGenericFuncDesc sum2Expr = new ExprNodeGenericFuncDesc();
    sum2Expr.setGenericUDF(udf4);
    ExprNodeGenericFuncDesc modExpr = new ExprNodeGenericFuncDesc();
    modExpr.setGenericUDF(udf5);

    ExprNodeColumnDesc col1Expr = new  ExprNodeColumnDesc(Long.class, "col1", "table", false);
    ExprNodeColumnDesc col2Expr = new  ExprNodeColumnDesc(Long.class, "col2", "table", false);
    ExprNodeColumnDesc col3Expr = new  ExprNodeColumnDesc(Long.class, "col3", "table", false);
    ExprNodeColumnDesc col4Expr = new  ExprNodeColumnDesc(Long.class, "col4", "table", false);
    ExprNodeColumnDesc col5Expr = new  ExprNodeColumnDesc(Long.class, "col5", "table", false);
    ExprNodeColumnDesc col6Expr = new  ExprNodeColumnDesc(Long.class, "col6", "table", false);

    List<ExprNodeDesc> children1 = new ArrayList<ExprNodeDesc>(2);
    List<ExprNodeDesc> children2 = new ArrayList<ExprNodeDesc>(2);
    List<ExprNodeDesc> children3 = new ArrayList<ExprNodeDesc>(2);
    List<ExprNodeDesc> children4 = new ArrayList<ExprNodeDesc>(2);
    List<ExprNodeDesc> children5 = new ArrayList<ExprNodeDesc>(2);

    children1.add(minusExpr);
    children1.add(multiplyExpr);
    sumExpr.setChildExprs(children1);

    children2.add(sum2Expr);
    children2.add(col3Expr);
    minusExpr.setChildExprs(children2);

    children3.add(col1Expr);
    children3.add(col2Expr);
    sum2Expr.setChildExprs(children3);

    children4.add(col4Expr);
    children4.add(modExpr);
    multiplyExpr.setChildExprs(children4);

    children5.add(col5Expr);
    children5.add(col6Expr);
    modExpr.setChildExprs(children5);

    Map<String, Integer> columnMap = new HashMap<String, Integer>();
    columnMap.put("col1", 1);
    columnMap.put("col2", 2);
    columnMap.put("col3", 3);
    columnMap.put("col4", 4);
    columnMap.put("col5", 5);
    columnMap.put("col6", 6);

    //Generate vectorized expression
    VectorizationContext vc = new VectorizationContext(columnMap, 6);

    VectorExpression ve = vc.getVectorExpression(sumExpr);

    //Verify vectorized expression
    assertTrue(ve instanceof LongColAddLongColumn);
    assertEquals(2, ve.getChildExpressions().length);
    VectorExpression childExpr1 = ve.getChildExpressions()[0];
    VectorExpression childExpr2 = ve.getChildExpressions()[1];
    assertEquals(6, ve.getOutputColumn());

    assertTrue(childExpr1 instanceof LongColSubtractLongColumn);
    assertEquals(1, childExpr1.getChildExpressions().length);
    assertTrue(childExpr1.getChildExpressions()[0] instanceof LongColAddLongColumn);
    assertEquals(7, childExpr1.getOutputColumn());
    assertEquals(6, childExpr1.getChildExpressions()[0].getOutputColumn());

    assertTrue(childExpr2 instanceof LongColMultiplyLongColumn);
    assertEquals(1, childExpr2.getChildExpressions().length);
    assertTrue(childExpr2.getChildExpressions()[0] instanceof LongColModuloLongColumn);
    assertEquals(8, childExpr2.getOutputColumn());
    assertEquals(6, childExpr2.getChildExpressions()[0].getOutputColumn());
  }

  @Test
  public void testStringFilterExpressions() throws HiveException {
    ExprNodeColumnDesc col1Expr = new  ExprNodeColumnDesc(String.class, "col1", "table", false);
    ExprNodeConstantDesc constDesc = new ExprNodeConstantDesc("Alpha");

    GenericUDFOPGreaterThan udf = new GenericUDFOPGreaterThan();
    ExprNodeGenericFuncDesc exprDesc = new ExprNodeGenericFuncDesc();
    exprDesc.setGenericUDF(udf);
    List<ExprNodeDesc> children1 = new ArrayList<ExprNodeDesc>(2);
    children1.add(col1Expr);
    children1.add(constDesc);
    exprDesc.setChildExprs(children1);

    Map<String, Integer> columnMap = new HashMap<String, Integer>();
    columnMap.put("col1", 1);
    columnMap.put("col2", 2);

    VectorizationContext vc = new VectorizationContext(columnMap, 2);
    vc.setOperatorType(OperatorType.FILTER);

    VectorExpression ve = vc.getVectorExpression(exprDesc);

    assertTrue(ve instanceof FilterStringColGreaterStringScalar);
  }

  @Test
  public void testFloatInExpressions() throws HiveException {
    ExprNodeColumnDesc col1Expr = new ExprNodeColumnDesc(Float.class, "col1", "table", false);
    ExprNodeConstantDesc constDesc = new ExprNodeConstantDesc(new Integer(10));

    GenericUDFBridge udf = new GenericUDFBridge("+", false, UDFOPPlus.class);
    ExprNodeGenericFuncDesc exprDesc = new ExprNodeGenericFuncDesc();
    exprDesc.setGenericUDF(udf);

    List<ExprNodeDesc> children1 = new ArrayList<ExprNodeDesc>(2);
    children1.add(col1Expr);
    children1.add(constDesc);
    exprDesc.setChildExprs(children1);

    Map<String, Integer> columnMap = new HashMap<String, Integer>();
    columnMap.put("col1", 0);

    VectorizationContext vc = new VectorizationContext(columnMap, 2);
    vc.setOperatorType(OperatorType.SELECT);

    VectorExpression ve = vc.getVectorExpression(exprDesc);

    assertTrue(ve.getOutputType().equalsIgnoreCase("double"));
  }

  @Test
  public void testVectorizeAndOrExpression() throws HiveException {
    ExprNodeColumnDesc col1Expr = new ExprNodeColumnDesc(Integer.class, "col1", "table", false);
    ExprNodeConstantDesc constDesc = new ExprNodeConstantDesc(new Integer(10));

    GenericUDFOPGreaterThan udf = new GenericUDFOPGreaterThan();
    ExprNodeGenericFuncDesc greaterExprDesc = new ExprNodeGenericFuncDesc();
    greaterExprDesc.setGenericUDF(udf);
    List<ExprNodeDesc> children1 = new ArrayList<ExprNodeDesc>(2);
    children1.add(col1Expr);
    children1.add(constDesc);
    greaterExprDesc.setChildExprs(children1);

    ExprNodeColumnDesc col2Expr = new ExprNodeColumnDesc(Float.class, "col2", "table", false);
    ExprNodeConstantDesc const2Desc = new ExprNodeConstantDesc(new Float(1.0));

    GenericUDFOPLessThan udf2 = new GenericUDFOPLessThan();
    ExprNodeGenericFuncDesc lessExprDesc = new ExprNodeGenericFuncDesc();
    lessExprDesc.setGenericUDF(udf2);
    List<ExprNodeDesc> children2 = new ArrayList<ExprNodeDesc>(2);
    children2.add(col2Expr);
    children2.add(const2Desc);
    lessExprDesc.setChildExprs(children2);

    GenericUDFOPAnd andUdf = new GenericUDFOPAnd();
    ExprNodeGenericFuncDesc andExprDesc = new ExprNodeGenericFuncDesc();
    andExprDesc.setGenericUDF(andUdf);
    List<ExprNodeDesc> children3 = new ArrayList<ExprNodeDesc>(2);
    children3.add(greaterExprDesc);
    children3.add(lessExprDesc);
    andExprDesc.setChildExprs(children3);

    Map<String, Integer> columnMap = new HashMap<String, Integer>();
    columnMap.put("col1", 0);
    columnMap.put("col2", 1);

    VectorizationContext vc = new VectorizationContext(columnMap, 2);
    vc.setOperatorType(OperatorType.FILTER);

    VectorExpression ve = vc.getVectorExpression(andExprDesc);

    assertEquals(ve.getClass(), FilterExprAndExpr.class);
    assertEquals(ve.getChildExpressions()[0].getClass(), FilterLongColGreaterLongScalar.class);
    assertEquals(ve.getChildExpressions()[1].getClass(), FilterDoubleColLessDoubleScalar.class);

    GenericUDFOPOr orUdf = new GenericUDFOPOr();
    ExprNodeGenericFuncDesc orExprDesc = new ExprNodeGenericFuncDesc();
    orExprDesc.setGenericUDF(orUdf);
    List<ExprNodeDesc> children4 = new ArrayList<ExprNodeDesc>(2);
    children4.add(greaterExprDesc);
    children4.add(lessExprDesc);
    orExprDesc.setChildExprs(children4);


    VectorExpression veOr = vc.getVectorExpression(orExprDesc);

    assertEquals(veOr.getClass(), FilterExprOrExpr.class);
    assertEquals(veOr.getChildExpressions()[0].getClass(), FilterLongColGreaterLongScalar.class);
    assertEquals(veOr.getChildExpressions()[1].getClass(), FilterDoubleColLessDoubleScalar.class);
  }


  @Test
  public void testFilterWithNegativeScalar() throws HiveException {
    ExprNodeColumnDesc col1Expr = new  ExprNodeColumnDesc(Integer.class, "col1", "table", false);
    ExprNodeConstantDesc constDesc = new ExprNodeConstantDesc(new Integer(-10));

    GenericUDFOPGreaterThan udf = new GenericUDFOPGreaterThan();
    ExprNodeGenericFuncDesc exprDesc = new ExprNodeGenericFuncDesc();
    exprDesc.setGenericUDF(udf);
    List<ExprNodeDesc> children1 = new ArrayList<ExprNodeDesc>(2);
    children1.add(col1Expr);
    children1.add(constDesc);
    exprDesc.setChildExprs(children1);

    Map<String, Integer> columnMap = new HashMap<String, Integer>();
    columnMap.put("col1", 1);
    columnMap.put("col2", 2);

    VectorizationContext vc = new VectorizationContext(columnMap, 2);
    vc.setOperatorType(OperatorType.FILTER);

    VectorExpression ve = vc.getVectorExpression(exprDesc);

    assertTrue(ve instanceof FilterLongColGreaterLongScalar);
  }
}
