/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.parse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Stack;

import org.antlr.runtime.CommonToken;
import org.antlr.runtime.tree.TreeWizard;
import org.antlr.runtime.tree.TreeWizard.ContextVisitor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.WindowFunctionInfo;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.OrderExpression;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.OrderSpec;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PTFInputSpec;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PTFQueryInputSpec;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PTFQueryInputType;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitionExpression;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitionSpec;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitionedTableFunctionSpec;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitioningSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.BoundarySpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.CurrentRowSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.RangeBoundarySpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.ValueBoundarySpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowExpressionSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowFrameSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowFunctionSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowSpec;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.ql.plan.PTFDesc.BoundaryDef;
import org.apache.hadoop.hive.ql.plan.PTFDesc.CurrentRowDef;
import org.apache.hadoop.hive.ql.plan.PTFDesc.OrderDef;
import org.apache.hadoop.hive.ql.plan.PTFDesc.OrderExpressionDef;
import org.apache.hadoop.hive.ql.plan.PTFDesc.PTFExpressionDef;
import org.apache.hadoop.hive.ql.plan.PTFDesc.PTFInputDef;
import org.apache.hadoop.hive.ql.plan.PTFDesc.PTFQueryInputDef;
import org.apache.hadoop.hive.ql.plan.PTFDesc.PartitionDef;
import org.apache.hadoop.hive.ql.plan.PTFDesc.PartitionedTableFunctionDef;
import org.apache.hadoop.hive.ql.plan.PTFDesc.RangeBoundaryDef;
import org.apache.hadoop.hive.ql.plan.PTFDesc.ShapeDetails;
import org.apache.hadoop.hive.ql.plan.PTFDesc.ValueBoundaryDef;
import org.apache.hadoop.hive.ql.plan.PTFDesc.WindowExpressionDef;
import org.apache.hadoop.hive.ql.plan.PTFDesc.WindowFrameDef;
import org.apache.hadoop.hive.ql.plan.PTFDesc.WindowFunctionDef;
import org.apache.hadoop.hive.ql.plan.PTFDesc.WindowTableFunctionDef;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLeadLag;
import org.apache.hadoop.hive.ql.udf.ptf.TableFunctionEvaluator;
import org.apache.hadoop.hive.ql.udf.ptf.TableFunctionResolver;
import org.apache.hadoop.hive.ql.udf.ptf.WindowingTableFunction.WindowingTableFunctionResolver;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class PTFTranslator {

  private static final Log LOG = LogFactory.getLog("org.apache.hadoop.hive.ql.parse");

  HiveConf hCfg;
  LeadLagInfo llInfo;
  SemanticAnalyzer semAly;
  UnparseTranslator unparseT;
  RowResolver inputRR;
  PTFDesc ptfDesc;
  PTFInvocationSpec ptfInvocation;
  WindowingSpec windowingSpec;

  private void init(SemanticAnalyzer semAly,
      HiveConf hCfg,
      RowResolver inputRR,
      UnparseTranslator unparseT) {
    this.semAly = semAly;
    this.hCfg = hCfg;
    this.inputRR = inputRR;
    this.unparseT = unparseT;
    llInfo = new LeadLagInfo();

  }

  public PTFDesc translate(PTFInvocationSpec qSpec,
      SemanticAnalyzer semAly,
      HiveConf hCfg,
      RowResolver inputRR,
      UnparseTranslator unparseT)
      throws SemanticException {
    init(semAly, hCfg, inputRR, unparseT);
    this.ptfInvocation = qSpec;
    ptfDesc = new PTFDesc();
    ptfDesc.setLlInfo(llInfo);
    translatePTFChain();
    return ptfDesc;
  }

  public PTFDesc translate(WindowingSpec wdwSpec, SemanticAnalyzer semAly, HiveConf hCfg, RowResolver inputRR,
      UnparseTranslator unparseT)
      throws SemanticException {
    init(semAly, hCfg, inputRR, unparseT);
    this.windowingSpec = wdwSpec;
    ptfDesc = new PTFDesc();
    ptfDesc.setLlInfo(llInfo);
    WindowTableFunctionDef wdwTFnDef = new WindowTableFunctionDef();
    ptfDesc.setFuncDef(wdwTFnDef);

    PTFQueryInputSpec inpSpec = new PTFQueryInputSpec();
    inpSpec.setType(PTFQueryInputType.WINDOWING);
    wdwTFnDef.setInput(translate(inpSpec, 0));
    ShapeDetails inpShape = wdwTFnDef.getInput().getOutputShape();

    WindowingTableFunctionResolver tFn = (WindowingTableFunctionResolver)
        FunctionRegistry.getTableFunctionResolver(FunctionRegistry.WINDOWING_TABLE_FUNCTION);
    if (tFn == null) {
      throw new SemanticException(String.format("INternal Error: Unknown Table Function %s",
          FunctionRegistry.WINDOWING_TABLE_FUNCTION));
    }
    wdwTFnDef.setName(FunctionRegistry.WINDOWING_TABLE_FUNCTION);
    wdwTFnDef.setResolverClassName(tFn.getClass().getName());
    wdwTFnDef.setAlias("ptf_" + 1 );
    wdwTFnDef.setExpressionTreeString(null);
    wdwTFnDef.setTransformsRawInput(false);
    tFn.initialize(hCfg, ptfDesc, wdwTFnDef);
    TableFunctionEvaluator tEval = tFn.getEvaluator();
    wdwTFnDef.setTFunction(tEval);
    wdwTFnDef.setCarryForwardNames(tFn.carryForwardNames());
    wdwTFnDef.setRawInputShape(inpShape);

    PartitioningSpec partiSpec = wdwSpec.getQueryPartitioningSpec();
    if ( partiSpec == null ) {
      throw new SemanticException(
          "Invalid use of Windowing: there is no Partitioning associated with Windowing");
    }
    PartitionDef partDef = translate(inpShape, wdwSpec.getQueryPartitionSpec());
    OrderDef ordDef = translate(inpShape, wdwSpec.getQueryOrderSpec(), partDef);

    wdwTFnDef.setPartition(partDef);
    wdwTFnDef.setOrder(ordDef);

    /*
     * process Wdw functions
     */
    ArrayList<WindowFunctionDef> windowFunctions = new ArrayList<WindowFunctionDef>();
    if ( wdwSpec.getWindowExpressions() != null ) {
      for(WindowExpressionSpec expr : wdwSpec.getWindowExpressions()) {
        if ( expr instanceof WindowFunctionSpec) {
          WindowFunctionDef wFnDef = translate(wdwTFnDef, (WindowFunctionSpec)expr);
          windowFunctions.add(wFnDef);
        }
      }
      wdwTFnDef.setWindowFunctions(windowFunctions);
    }

    /*
     * set outputFromWdwFnProcessing
     */
    if ( windowFunctions.size() > 0 ) {
      ArrayList<String> aliases = new ArrayList<String>();
      ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
      for(WindowFunctionDef wFnDef : windowFunctions) {
        aliases.add(wFnDef.getAlias());
        if ( wFnDef.isPivotResult() ) {
          fieldOIs.add(((ListObjectInspector)wFnDef.getOI()).getListElementObjectInspector());
        } else {
          fieldOIs.add(wFnDef.getOI());
        }
      }
      PTFTranslator.addInputColumnsToList(inpShape, aliases, fieldOIs);
      StructObjectInspector wdwOutOI = ObjectInspectorFactory.getStandardStructObjectInspector(
          aliases, fieldOIs);
      tFn.setWdwProcessingOutputOI(wdwOutOI);
      RowResolver wdwOutRR = buildRowResolverForWindowing(wdwTFnDef, false);
      ShapeDetails wdwOutShape = setupShape(wdwOutOI, null, wdwOutRR);
      wdwTFnDef.setOutputFromWdwFnProcessing(wdwOutShape);
    }
    else {
      wdwTFnDef.setOutputFromWdwFnProcessing(inpShape);
    }

    /*
     * process Wdw expressions
     */
    ShapeDetails wdwOutShape = wdwTFnDef.getOutputFromWdwFnProcessing();
    ArrayList<WindowExpressionDef> windowExpressions = new ArrayList<WindowExpressionDef>();
    if ( wdwSpec.getWindowExpressions() != null ) {
      for(WindowExpressionSpec expr : wdwSpec.getWindowExpressions()) {
        if ( !(expr instanceof WindowFunctionSpec) ) {
          try {
            PTFExpressionDef eDef = buildExpressionDef(wdwOutShape, expr.getExpression());
            WindowExpressionDef wdwEDef = new WindowExpressionDef(eDef);
            wdwEDef.setAlias(expr.getAlias());
            windowExpressions.add(wdwEDef);
          }
          catch(HiveException he) {
            throw new SemanticException(he);
          }
        }
      }
      wdwTFnDef.setWindowExpressions(windowExpressions);
    }

    /*
     * set outputOI
     */
    if ( windowExpressions.size() > 0 ) {
      ArrayList<String> aliases = new ArrayList<String>();
      ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
      for(WindowExpressionDef wEDef : windowExpressions) {
        aliases.add(wEDef.getAlias());
        fieldOIs.add(wEDef.getOI());
      }
      PTFTranslator.addInputColumnsToList(wdwOutShape, aliases, fieldOIs);
      StructObjectInspector outOI = ObjectInspectorFactory.getStandardStructObjectInspector(
          aliases, fieldOIs);
      RowResolver outRR = buildRowResolverForWindowing(wdwTFnDef, true);
      ShapeDetails outShape = setupShape(outOI, null, outRR);
      wdwTFnDef.setOutputShape(outShape);
    }
    else {
      wdwTFnDef.setOutputShape(copyShape(wdwOutShape));
    }

    tFn.setupOutputOI();

    /*
     * If we have windowExpressions then we convert to Std. Object to process;
     * we just stream these rows; no need to put in an output Partition.
     */
    if ( windowExpressions.size() > 0 ) {
      StructObjectInspector oi = (StructObjectInspector)
          ObjectInspectorUtils.getStandardObjectInspector(wdwTFnDef.getOutputShape().getOI());
      wdwTFnDef.getOutputShape().setOI(oi);
    }

    return ptfDesc;
  }

  private void translatePTFChain() throws SemanticException {

    Stack<PTFInputSpec> ptfChain = new Stack<PTFInvocationSpec.PTFInputSpec>();
    PTFInputSpec currentSpec = ptfInvocation.getFunction();
    while (currentSpec != null ) {
      ptfChain.push(currentSpec);
      currentSpec = currentSpec.getInput();
    }

    int inputNum = 0;
    PTFInputDef currentDef = null;
    while ( !ptfChain.isEmpty() ) {
      currentSpec = ptfChain.pop();

      if ( currentSpec instanceof PTFQueryInputSpec) {
        currentDef = translate((PTFQueryInputSpec) currentSpec, inputNum);
      }
      else {
        currentDef = translate((PartitionedTableFunctionSpec) currentSpec,
            currentDef,
            inputNum);
      }
      inputNum++;
    }
    ptfDesc.setFuncDef((PartitionedTableFunctionDef)currentDef);
  }

  private PTFQueryInputDef translate(PTFQueryInputSpec spec,
      int inpNum) throws SemanticException
  {
    PTFQueryInputDef def = new PTFQueryInputDef();
    StructObjectInspector oi = PTFTranslator.getStandardStructOI(inputRR);
    ShapeDetails shp = setupShape(oi, null, inputRR);
    def.setOutputShape(shp);
    def.setType(spec.getType());
    def.setAlias(spec.getSource() == null ? "ptf_" + inpNum : spec.getSource());
    return def;
  }

  private PartitionedTableFunctionDef translate(PartitionedTableFunctionSpec spec,
      PTFInputDef inpDef,
      int inpNum)
      throws SemanticException {
    TableFunctionResolver tFn = FunctionRegistry.getTableFunctionResolver(spec.getName());
    if (tFn == null) {
      throw new SemanticException(String.format("Unknown Table Function %s",
          spec.getName()));
    }
    PartitionedTableFunctionDef def = new PartitionedTableFunctionDef();
    def.setInput(inpDef);
    def.setName(spec.getName());
    def.setResolverClassName(tFn.getClass().getName());
    def.setAlias(spec.getAlias() == null ? "ptf_" + inpNum : spec.getAlias());
    def.setExpressionTreeString(spec.getAstNode().toStringTree());
    def.setTransformsRawInput(tFn.transformsRawInput());
    /*
     * translate args
     */
    ArrayList<ASTNode> args = spec.getArgs();
    if (args != null)
    {
      for (ASTNode expr : args)
      {
        PTFExpressionDef argDef = null;
        try {
          argDef = buildExpressionDef(inpDef.getOutputShape(), expr);
        }
        catch(HiveException he) {
          throw new SemanticException(he);
        }
        def.addArg(argDef);
      }
    }

    tFn.initialize(hCfg, ptfDesc, def);
    TableFunctionEvaluator tEval = tFn.getEvaluator();
    def.setTFunction(tEval);
    def.setCarryForwardNames(tFn.carryForwardNames());
    tFn.setupRawInputOI();

    if ( tFn.transformsRawInput() ) {
      StructObjectInspector rawInOutOI = tEval.getRawInputOI();
      ArrayList<String> rawInOutColNames = tFn.getRawInputColumnNames();
      RowResolver rawInRR = buildRowResolverForPTF(def.getName(),
          spec.getAlias(),
          rawInOutOI,
          rawInOutColNames,
          inpDef.getOutputShape().getRr());
      ShapeDetails rawInpShape = setupTableFnShape(def.getName(),
          inpDef.getOutputShape(),
          rawInOutOI,
          rawInOutColNames,
          rawInRR);
      def.setRawInputShape(rawInpShape);
    }
    else {
      def.setRawInputShape(inpDef.getOutputShape());
    }

    translatePartitioning(def, spec);
    tFn.setupOutputOI();

    StructObjectInspector outputOI = tEval.getOutputOI();
    ArrayList<String> outColNames = tFn.getOutputColumnNames();
    RowResolver outRR = buildRowResolverForPTF(def.getName(),
        spec.getAlias(),
        outputOI,
        outColNames,
        def.getRawInputShape().getRr());
    ShapeDetails outputShape = setupTableFnShape(def.getName(),
        inpDef.getOutputShape(),
        outputOI,
        outColNames,
        outRR);
    def.setOutputShape(outputShape);

    return def;
  }

  private WindowFunctionDef translate(WindowTableFunctionDef wdwTFnDef,
      WindowFunctionSpec spec) throws SemanticException {
    WindowFunctionInfo wFnInfo = FunctionRegistry.getWindowFunctionInfo(spec.getName());
    WindowFunctionDef def = new WindowFunctionDef();
    def.setName(spec.getName());
    def.setAlias(spec.getAlias());
    def.setDistinct(spec.isDistinct());
    def.setExpressionTreeString(spec.getExpression().toStringTree());
    def.setStar(spec.isStar());
    def.setPivotResult(wFnInfo.isPivotResult());
    ShapeDetails inpShape = wdwTFnDef.getRawInputShape();

    /*
     * translate args
     */
    ArrayList<ASTNode> args = spec.getArgs();
    if (args != null)
    {
      for (ASTNode expr : args)
      {
        PTFExpressionDef argDef = null;
        try {
          argDef = buildExpressionDef(inpShape, expr);
        }
        catch(HiveException he) {
          throw new SemanticException(he);
        }
        def.addArg(argDef);
      }
    }

    if (RANKING_FUNCS.contains(spec.getName()))
    {
      setupRankingArgs(wdwTFnDef, def, spec);
    }

    WindowSpec wdwSpec = spec.getWindowSpec();
    if ( wdwSpec != null ) {
      String desc = spec.toString();

      WindowFrameDef wdwFrame = translate(spec.getName(), inpShape, wdwSpec);
      if (!wFnInfo.isSupportsWindow() )
      {
        BoundarySpec start = wdwSpec.getWindowFrame().getStart();
        if ( start.getAmt() != BoundarySpec.UNBOUNDED_AMOUNT ) {
          throw new SemanticException(
          String.format("Expecting left window frame boundary for " +
              "function %s to be unbounded. Found : %d", desc, start.getAmt())
              );
        }
        BoundarySpec end = wdwSpec.getWindowFrame().getEnd();
        if ( end.getAmt() != BoundarySpec.UNBOUNDED_AMOUNT ) {
          throw new SemanticException(
              String.format("Expecting right window frame boundary for " +
              "function %s to be unbounded. Found : %d", desc, start.getAmt()));
        }
      }
      def.setWindowFrame(wdwFrame);
    }

    try {
      setupWdwFnEvaluator(def);
    }
    catch(HiveException he) {
      throw new SemanticException(he);
    }

    return def;
  }

  private void translatePartitioning(PartitionedTableFunctionDef def,
      PartitionedTableFunctionSpec spec)
      throws SemanticException {

    applyConstantPartition(spec);
    if ( spec.getPartition() == null ) {
      return;
    }
    PartitionDef partDef = translate(def.getRawInputShape(), spec.getPartition());
    OrderDef orderDef = translate(def.getRawInputShape(), spec.getOrder(), partDef);
    def.setPartition(partDef);
    def.setOrder(orderDef);
  }

  /*
   * If this the first PPTF in the chain and there is no partition specified
   * then assume the user wants to include the entire input in 1 partition.
   */
  private static void applyConstantPartition( PartitionedTableFunctionSpec spec) {
    if ( spec.getPartition() != null ) {
      return;
    }
    PTFInputSpec iSpec = spec.getInput();
    if ( iSpec instanceof PTFInputSpec ) {
        PartitionSpec partSpec = new PartitionSpec();
        PartitionExpression partExpr = new PartitionExpression();
        partExpr.setExpression(new ASTNode(new CommonToken(HiveParser.Number, "0")));
        partSpec.addExpression(partExpr);
        spec.setPartition(partSpec);
    }
  }

  private PartitionDef translate(ShapeDetails inpShape, PartitionSpec spec)
      throws SemanticException
  {
    if (spec == null || spec.getExpressions() == null || spec.getExpressions().size() == 0) {
      return null;
    }

    PartitionDef pDef = new PartitionDef();
    for (PartitionExpression pExpr : spec.getExpressions())
    {
      PTFExpressionDef expDef = translate(inpShape, pExpr);
      pDef.addExpression(expDef);
    }
    return pDef;
  }

  private PTFExpressionDef translate(ShapeDetails inpShape,
      PartitionExpression pExpr) throws SemanticException
  {
    PTFExpressionDef expDef = null;
    try {
      expDef = buildExpressionDef(inpShape, pExpr.getExpression());
    }
    catch(HiveException he) {
      throw new SemanticException(he);
    }
    PTFTranslator.validateComparable(expDef.getOI(),
        String.format("Partition Expression %s is not a comparable expression", pExpr.getExpression().toStringTree()));
    return expDef;
  }

  private OrderDef translate(ShapeDetails inpShape,
      OrderSpec spec,
      PartitionDef partitionDef) throws SemanticException {

    OrderDef def = new OrderDef();
    if (null == spec) {
      return def;
    }

    for (OrderExpression oExpr : spec.getExpressions())
    {
      OrderExpressionDef oexpDef = translate(inpShape, oExpr);
      def.addExpression(oexpDef);
    }

    return def;
  }

  private OrderExpressionDef translate(ShapeDetails inpShape,
      OrderExpression oExpr)
          throws SemanticException
  {
    OrderExpressionDef oexpDef = new OrderExpressionDef();
    oexpDef.setOrder(oExpr.getOrder());
    try {
      PTFExpressionDef expDef = buildExpressionDef(inpShape, oExpr.getExpression());
      oexpDef.setExpressionTreeString(expDef.getExpressionTreeString());
      oexpDef.setExprEvaluator(expDef.getExprEvaluator());
      oexpDef.setExprNode(expDef.getExprNode());
      oexpDef.setOI(expDef.getOI());
    }
    catch(HiveException he) {
      throw new SemanticException(he);
    }
    PTFTranslator.validateComparable(oexpDef.getOI(),
        String.format("Partition Expression %s is not a comparable expression",
            oExpr.getExpression().toStringTree()));
    return oexpDef;
  }

  private WindowFrameDef translate(String wFnName, ShapeDetails inpShape, WindowSpec spec)
      throws SemanticException {
    /*
     * Since we componentize Windowing, no need to translate
     * the Partition & Order specs of individual WFns.
    */
    return translate(inpShape, spec.getWindowFrame());
  }

  private WindowFrameDef translate(ShapeDetails inpShape,
      WindowFrameSpec spec)
      throws SemanticException {
    if (spec == null)
    {
      return null;
    }

    BoundarySpec s = spec.getStart();
    BoundarySpec e = spec.getEnd();
    int cmp = s.compareTo(e);
    if (cmp > 0)
    {
      throw new SemanticException(String.format(
          "Window range invalid, start boundary is greater than end boundary: %s", spec));
    }

    WindowFrameDef wfDef = new WindowFrameDef();
    wfDef.setStart(translate(inpShape, s));
    wfDef.setEnd(translate(inpShape, e));
    return wfDef;
  }

  private BoundaryDef translate(ShapeDetails inpShape, BoundarySpec bndSpec)
      throws SemanticException {
    if (bndSpec instanceof ValueBoundarySpec)
    {
      ValueBoundarySpec vBndSpec = (ValueBoundarySpec) bndSpec;
      ValueBoundaryDef vbDef = new ValueBoundaryDef();
      vbDef.setAmt(vBndSpec.getAmt());
      vbDef.setDirection(vBndSpec.getDirection());
      PTFTranslator.validateNoLeadLagInValueBoundarySpec(vBndSpec.getExpression());
      PTFExpressionDef exprDef = null;
      try {
        exprDef = buildExpressionDef(inpShape, vBndSpec.getExpression());
      }
      catch(HiveException he) {
        throw new SemanticException(he);
      }
      PTFTranslator.validateValueBoundaryExprType(exprDef.getOI());
      vbDef.setExpressionDef(exprDef);
      return vbDef;
    }
    else if (bndSpec instanceof RangeBoundarySpec)
    {
      RangeBoundarySpec rBndSpec = (RangeBoundarySpec) bndSpec;
      RangeBoundaryDef rbDef = new RangeBoundaryDef();
      rbDef.setAmt(rBndSpec.getAmt());
      rbDef.setDirection(rBndSpec.getDirection());
      return rbDef;
    }
    else if (bndSpec instanceof CurrentRowSpec)
    {
      CurrentRowDef cbDef = new CurrentRowDef();
      return cbDef;
    }
    throw new SemanticException("Unknown Boundary: " + bndSpec);
  }

  static void setupWdwFnEvaluator(WindowFunctionDef def) throws HiveException
  {
    ArrayList<PTFExpressionDef> args = def.getArgs();
    ArrayList<ObjectInspector> argOIs = new ArrayList<ObjectInspector>();
    ObjectInspector[] funcArgOIs = null;

    if (args != null) {
      for (PTFExpressionDef arg : args) {
        argOIs.add(arg.getOI());
      }
      funcArgOIs = new ObjectInspector[args.size()];
      funcArgOIs = argOIs.toArray(funcArgOIs);
    }

    GenericUDAFEvaluator wFnEval = FunctionRegistry.getGenericWindowingEvaluator(def.getName(), argOIs,
        def.isDistinct(), def.isStar());
    ObjectInspector OI = wFnEval.init(GenericUDAFEvaluator.Mode.COMPLETE, funcArgOIs);
    def.setWFnEval(wFnEval);
    def.setOI(OI);
  }

  private static void validateValueBoundaryExprType(ObjectInspector OI)
      throws SemanticException
  {
    if (!OI.getCategory().equals(Category.PRIMITIVE))
    {
      throw new SemanticException(
          String.format(
              "Value Boundary expression must be of primitve type. Found: %s",
              OI.getTypeName()));
    }

    PrimitiveObjectInspector pOI = (PrimitiveObjectInspector) OI;
    PrimitiveCategory pC = pOI.getPrimitiveCategory();

    switch (pC)
    {
    case BYTE:
    case DOUBLE:
    case FLOAT:
    case INT:
    case LONG:
    case SHORT:
    case DECIMAL:
    case TIMESTAMP:
    case STRING:
      break;
    default:
      throw new SemanticException(
          String.format("Primitve type %s not supported in Value Boundary expression",
              pC));
    }

  }

  private ShapeDetails setupTableFnShape(String fnName, ShapeDetails inpShape, StructObjectInspector OI, ArrayList<String> columnNames, RowResolver rr)
      throws SemanticException {
    if (fnName.equals(FunctionRegistry.NOOP_TABLE_FUNCTION)
        || fnName.equals(
            FunctionRegistry.NOOP_MAP_TABLE_FUNCTION))
    {
      return setupShapeForNoop(inpShape, OI, columnNames, rr);
    }
    return setupShape(OI, columnNames, rr);
  }

  private ShapeDetails setupShape(StructObjectInspector OI,
      ArrayList<String> columnNames,
      RowResolver rr) throws SemanticException {
    Map<String, String> serdePropsMap = new LinkedHashMap<String, String>();
    SerDe serde = null;
    ShapeDetails shp = new ShapeDetails();

    try {
      serde = PTFTranslator.createLazyBinarySerDe(hCfg, OI, serdePropsMap);
      shp.setOI((StructObjectInspector)serde.getObjectInspector());
    }
    catch(SerDeException se) {
      throw new SemanticException(se);
    }

    shp.setRr(rr);
    shp.setSerde(serde);
    shp.setSerdeClassName(serde.getClass().getName());
    shp.setSerdeProps(serdePropsMap);
    shp.setColumnNames(columnNames);

    TypeCheckCtx tCtx = new TypeCheckCtx(rr);
    tCtx.setUnparseTranslator(unparseT);
    shp.setTypeCheckCtx(tCtx);

    return shp;
  }

  private ShapeDetails copyShape(ShapeDetails src) {
    ShapeDetails dest = new ShapeDetails();
    dest.setSerdeClassName(src.getSerdeClassName());
    dest.setSerdeProps(src.getSerdeProps());
    dest.setColumnNames(src.getColumnNames());
    dest.setOI(src.getOI());
    dest.setSerde(src.getSerde());
    dest.setRr(src.getRr());
    dest.setTypeCheckCtx(src.getTypeCheckCtx());

    return dest;
  }

  private ShapeDetails setupShapeForNoop(ShapeDetails inpShape,
      StructObjectInspector OI,
      ArrayList<String> columnNames,
      RowResolver rr) throws SemanticException {
    ShapeDetails shp = new ShapeDetails();

    shp.setRr(rr);
    shp.setOI(inpShape.getOI());
    shp.setSerde(inpShape.getSerde());
    shp.setSerdeClassName(inpShape.getSerde().getClass().getName());
    shp.setSerdeProps(inpShape.getSerdeProps());
    shp.setColumnNames(columnNames);

    TypeCheckCtx tCtx = new TypeCheckCtx(rr);
    tCtx.setUnparseTranslator(unparseT);
    shp.setTypeCheckCtx(tCtx);

    return shp;
  }

  protected static ArrayList<OrderExpression> addPartitionExpressionsToOrderList(
      ArrayList<PartitionExpression> partCols,
      ArrayList<OrderExpression> orderCols) throws SemanticException {
    int numOfPartColumns = 0;
    int chkSize = partCols.size();

    chkSize = chkSize > orderCols.size() ? orderCols.size() : chkSize;
    for (int i = 0; i < chkSize; i++) {
      if (orderCols.get(i).getExpression().toStringTree()
          .equals(partCols.get(i).getExpression().toStringTree())) {
        numOfPartColumns++;
      } else {
        break;
      }
    }

    if (numOfPartColumns != 0 && numOfPartColumns != partCols.size()) {
      List<String> partitionColumnNames = new ArrayList<String>();
      for(PartitionExpression partitionExpression : partCols) {
        ASTNode column = partitionExpression.getExpression();
        if(column != null && column.getChildCount() > 0) {
          partitionColumnNames.add(column.getChild(0).getText());
        }
      }
      throw new SemanticException(
          String.format(
                  "all partition columns %s must be in order clause or none should be specified",
                  partitionColumnNames.toString()));
    }
    ArrayList<OrderExpression> combinedOrdExprs = new ArrayList<OrderExpression>();
    if (numOfPartColumns == 0)
    {
      for (PartitionExpression partCol : partCols)
      {
        OrderExpression orderCol = new OrderExpression(partCol);
        combinedOrdExprs.add(orderCol);
      }
    }
    combinedOrdExprs.addAll(orderCols);
    return combinedOrdExprs;
  }


  /*
   * Ranking Functions helpers
   */

  protected static final ArrayList<String> RANKING_FUNCS = new ArrayList<String>();
  static
  {
    RANKING_FUNCS.add("rank");
    RANKING_FUNCS.add("dense_rank");
    RANKING_FUNCS.add("percent_rank");
    RANKING_FUNCS.add("cume_dist");
  };

  private void setupRankingArgs(WindowTableFunctionDef wdwTFnDef,
      WindowFunctionDef wFnDef,
      WindowFunctionSpec wSpec)
      throws SemanticException
  {
    if (wSpec.getArgs().size() > 0)
    {
      throw new SemanticException("Ranking Functions can take no arguments");
    }

    OrderDef oDef = wdwTFnDef.getOrder();
    ArrayList<OrderExpressionDef> oExprs = oDef.getExpressions();
    for (OrderExpressionDef oExpr : oExprs)
    {
      wFnDef.addArg(oExpr);
    }
  }

  /*
   * Expr translation helper methods
   */
  public PTFExpressionDef buildExpressionDef(ShapeDetails inpShape, ASTNode arg)
      throws HiveException
  {
    PTFExpressionDef argDef = new PTFExpressionDef();

    ExprNodeDesc exprNode = semAly.genExprNodeDesc(arg, inpShape.getRr(),
        inpShape.getTypeCheckCtx());
    ExprNodeEvaluator exprEval = WindowingExprNodeEvaluatorFactory.get(llInfo, exprNode);
    ObjectInspector oi = initExprNodeEvaluator(exprEval, exprNode, inpShape);

    argDef.setExpressionTreeString(arg.toStringTree());
    argDef.setExprNode(exprNode);
    argDef.setExprEvaluator(exprEval);
    argDef.setOI(oi);
    return argDef;
  }

  private ObjectInspector initExprNodeEvaluator(ExprNodeEvaluator exprEval,
      ExprNodeDesc exprNode,
      ShapeDetails inpShape)
      throws HiveException
  {
    ObjectInspector outOI;
    outOI = exprEval.initialize(inpShape.getOI());

    /*
     * if there are any LeadLag functions in this Expression Tree: - setup a
     * duplicate Evaluator for the 1st arg of the LLFuncDesc - initialize it
     * using the InputInfo provided for this Expr tree - set the duplicate
     * evaluator on the LLUDF instance.
     */
    List<ExprNodeGenericFuncDesc> llFuncExprs = llInfo.getLLFuncExprsInTopExpr(exprNode);
    if (llFuncExprs != null)
    {
      for (ExprNodeGenericFuncDesc llFuncExpr : llFuncExprs)
      {
        ExprNodeDesc firstArg = llFuncExpr.getChildren().get(0);
        ExprNodeEvaluator dupExprEval = WindowingExprNodeEvaluatorFactory.get(llInfo, firstArg);
        dupExprEval.initialize(inpShape.getOI());
        GenericUDFLeadLag llFn = (GenericUDFLeadLag) llFuncExpr.getGenericUDF();
        llFn.setExprEvaluator(dupExprEval);
      }
    }

    return outOI;
  }

  /*
   * OI & Serde helper methods
   */

  protected static SerDe createLazyBinarySerDe(Configuration cfg,
      StructObjectInspector oi, Map<String,String> serdePropsMap) throws SerDeException {
    serdePropsMap = serdePropsMap == null ? new LinkedHashMap<String, String>() : serdePropsMap;

    addOIPropertiestoSerDePropsMap(oi, serdePropsMap);

    SerDe serDe = new LazyBinarySerDe();
    Properties p = new Properties();
    p.setProperty(org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS,
        serdePropsMap.get(org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS));
    p.setProperty(
        org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMN_TYPES,
        serdePropsMap.get(org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMN_TYPES));
    serDe.initialize(cfg, p);
    return serDe;
  }

  @SuppressWarnings({"unchecked"})
  public static void addOIPropertiestoSerDePropsMap(StructObjectInspector OI,
      Map<String,String> serdePropsMap) {

    if ( serdePropsMap == null ) {
      return;
    }

    ArrayList<? extends Object>[] tInfo = getTypeMap(OI);

    ArrayList<String> columnNames = (ArrayList<String>) tInfo[0];
    ArrayList<TypeInfo> fields = (ArrayList<TypeInfo>) tInfo[1];
    StringBuilder cNames = new StringBuilder();
    StringBuilder cTypes = new StringBuilder();

    for (int i = 0; i < fields.size(); i++)
    {
      cNames.append(i > 0 ? "," : "");
      cTypes.append(i > 0 ? "," : "");
      cNames.append(columnNames.get(i));
      cTypes.append(fields.get(i).getTypeName());
    }

    serdePropsMap.put(org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS,
        cNames.toString());
    serdePropsMap.put(org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMN_TYPES,
        cTypes.toString());
  }

  private static ArrayList<? extends Object>[] getTypeMap(
      StructObjectInspector oi) {
    StructTypeInfo t = (StructTypeInfo) TypeInfoUtils
        .getTypeInfoFromObjectInspector(oi);
    ArrayList<String> fnames = t.getAllStructFieldNames();
    ArrayList<TypeInfo> fields = t.getAllStructFieldTypeInfos();
    return new ArrayList<?>[]
    { fnames, fields };
  }

  /**
   * For each column on the input RR, construct a StructField for it
   * OI is constructed using the list of input column names and
   * their corresponding OIs.
   * @param rr
   * @return
   */
  public static StructObjectInspector getStandardStructOI(RowResolver rr) {
    StructObjectInspector oi;
    ArrayList<ColumnInfo> colLists = rr.getColumnInfos();
    ArrayList<String> structFieldNames = new ArrayList<String>();
    ArrayList<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();
    for (ColumnInfo columnInfo : colLists) {
      String colName = columnInfo.getInternalName();
      ObjectInspector colOI = columnInfo.getObjectInspector();
      structFieldNames.add(colName);
      structFieldObjectInspectors.add(colOI);
    }
    oi = ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames,
        structFieldObjectInspectors);
    return oi;

  }

  protected static void validateComparable(ObjectInspector OI, String errMsg)
      throws SemanticException
  {
    if (!ObjectInspectorUtils.compareSupported(OI))
    {
      throw new SemanticException(errMsg);
    }
  }

  private static void addInputColumnsToList(ShapeDetails shape,
      ArrayList<String> fieldNames, ArrayList<ObjectInspector> fieldOIs)
  {
    StructObjectInspector OI = shape.getOI();
    for (StructField f : OI.getAllStructFieldRefs())
    {
      fieldNames.add(f.getFieldName());
      fieldOIs.add(f.getFieldObjectInspector());
    }
  }

  /*
   * RowResolver helper methods
   */

  protected static RowResolver buildRowResolverForPTF(String tbFnName, String tabAlias,
      StructObjectInspector rowObjectInspector,
      ArrayList<String> outputColNames, RowResolver inputRR) throws SemanticException {

    if ( tbFnName.equals(FunctionRegistry.NOOP_TABLE_FUNCTION) ||
        tbFnName.equals(FunctionRegistry.NOOP_MAP_TABLE_FUNCTION) ) {
      return buildRowResolverForNoop(tabAlias, rowObjectInspector, inputRR);
    }

    RowResolver rwsch = new RowResolver();
    List<? extends StructField> fields = rowObjectInspector.getAllStructFieldRefs();
    for (int i = 0; i < fields.size(); i++)
    {
      ColumnInfo colInfo = new ColumnInfo(fields.get(i).getFieldName(),
          TypeInfoUtils.getTypeInfoFromObjectInspector(fields.get(i)
              .getFieldObjectInspector()),
          tabAlias,
          false);
      rwsch.put(tabAlias, outputColNames.get(i), colInfo);
    }
    return rwsch;
  }

  protected RowResolver buildRowResolverForWindowing(WindowTableFunctionDef def,
      boolean addWdwExprs)  throws SemanticException {
    RowResolver rr = new RowResolver();
    HashMap<String, WindowExpressionSpec> aliasToExprMap = windowingSpec.getAliasToWdwExpr();
    /*
     * add Window Expressions
     */
    if ( addWdwExprs ) {
      for(WindowExpressionDef wEDef : def.getWindowExpressions() ) {
        ASTNode ast = aliasToExprMap.get(wEDef.getAlias()).getExpression();
        ColumnInfo cInfo = new ColumnInfo(wEDef.getAlias(),
            TypeInfoUtils.getTypeInfoFromObjectInspector(wEDef.getOI()),
            null,
            false);
        rr.putExpression(ast, cInfo);
      }
    }

    /*
     * add Window Functions
     */
    for(WindowFunctionDef wFnDef : def.getWindowFunctions() ) {
      ASTNode ast = aliasToExprMap.get(wFnDef.getAlias()).getExpression();
      ObjectInspector wFnOI = null;
      if ( wFnDef.isPivotResult() ) {
        wFnOI = ((ListObjectInspector)wFnDef.getOI()).getListElementObjectInspector();
      }
      else {
        wFnOI = wFnDef.getOI();
      }
      ColumnInfo cInfo = new ColumnInfo(wFnDef.getAlias(),
          TypeInfoUtils.getTypeInfoFromObjectInspector(wFnOI),
          null,
          false);
      rr.putExpression(ast, cInfo);
    }

    RowResolver inpRR = def.getRawInputShape().getRr();
    /*
     * add columns from inpRR
     */
    for (ColumnInfo inpCInfo : inputRR.getColumnInfos()) {
      ColumnInfo cInfo = new ColumnInfo(inpCInfo);
      String colAlias = cInfo.getAlias();

      String[] tabColAlias = inputRR.reverseLookup(inpCInfo.getInternalName());
      if (tabColAlias != null) {
        colAlias = tabColAlias[1];
      }
      ASTNode inExpr = null;
      inExpr = PTFTranslator.getASTNode(inpCInfo, inpRR);
      if ( inExpr != null ) {
        rr.putExpression(inExpr, cInfo);
      }
      else {
        rr.put(cInfo.getTabAlias(), colAlias, cInfo);
      }
    }

    return rr;
  }

  protected static RowResolver buildRowResolverForNoop(String tabAlias,
      StructObjectInspector rowObjectInspector,
      RowResolver inputRowResolver) throws SemanticException
  {
    LOG.info("QueryTranslationInfo::getRowResolver invoked on ObjectInspector");
    RowResolver rwsch = new RowResolver();
    List<? extends StructField> fields = rowObjectInspector.getAllStructFieldRefs();
    for (int i = 0; i < fields.size(); i++)
    {
      StructField field = fields.get(i);
      String internalName = field.getFieldName();
      String[] tabColAlias = inputRowResolver == null ? null : inputRowResolver
          .reverseLookup(internalName);
      String colTabAlias = tabColAlias == null ? tabAlias : tabColAlias[0];
      String colAlias = tabColAlias == null ? null : tabColAlias[1];
      ColumnInfo inpColInfo;
      ColumnInfo colInfo;

      if (tabColAlias != null) {
        inpColInfo = inputRowResolver.get(colTabAlias, colAlias);
      }
      else {
        /*
         * for the Virtual columns:
         * - the internalName is UPPER Case and the alias is lower case
         * - since we put them in an OI, the fieldnames became lower cased.
         * - so we look in the inputRR for the fieldName as an alias.
         */
        inpColInfo = inputRowResolver == null ? null : inputRowResolver
            .get(tabAlias, internalName);
        colAlias = inpColInfo != null ? inpColInfo.getInternalName() : colAlias;
      }

      if (inpColInfo != null) {
        colInfo = new ColumnInfo(inpColInfo);
        colInfo.setTabAlias(tabAlias);
      }
      else {
        colInfo = new ColumnInfo(fields.get(i).getFieldName(),
            TypeInfoUtils.getTypeInfoFromObjectInspector(fields.get(i)
                .getFieldObjectInspector()),
            tabAlias,
            false);
        colAlias = colInfo.getInternalName();
      }

      ASTNode expr = inputRowResolver == null ? null : PTFTranslator.getASTNode(inpColInfo,
          inputRowResolver);

      if (expr != null) {
        rwsch.putExpression(expr, colInfo);
      }
      else {
        rwsch.put(tabAlias, colAlias, colInfo);
      }
    }
    return rwsch;
  }

  /*
   * If the cInfo is for an ASTNode, this function returns the ASTNode that it is for.
   */
  public static ASTNode getASTNode(ColumnInfo cInfo, RowResolver rr) throws SemanticException {
    for(Map.Entry<String, ASTNode> entry : rr.getExpressionMap().entrySet()) {
      ASTNode expr = entry.getValue();
      if ( rr.getExpression(expr).equals(cInfo)) {
        return expr;
      }
    }
    return null;
  }

  /*
   * Utility to visit all nodes in an AST tree.
   */
  public static void visit(Object t, ContextVisitor visitor) {
    _visit(t, null, 0, visitor);
  }

  /** Do the recursive work for visit */
  private static void _visit(Object t, Object parent, int childIndex, ContextVisitor visitor) {
    if ( t==null ) {
      return;
    }
    visitor.visit(t, parent, childIndex, null);
    int n = ParseDriver.adaptor.getChildCount(t);
    for (int i=0; i<n; i++) {
      Object child = ParseDriver.adaptor.getChild(t, i);
      _visit(child, t, i, visitor);
    }
  }

  public static ArrayList<PTFInvocationSpec> componentize(PTFInvocationSpec ptfInvocation) throws SemanticException {

    ArrayList<PTFInvocationSpec> componentInvocations = new ArrayList<PTFInvocationSpec>();

    Stack<PTFInputSpec> ptfChain = new Stack<PTFInvocationSpec.PTFInputSpec>();
    PTFInputSpec spec = ptfInvocation.getFunction();
    while (spec instanceof PartitionedTableFunctionSpec) {
        ptfChain.push(spec);
        spec = spec.getInput();
    }

    PartitionedTableFunctionSpec prevFn = (PartitionedTableFunctionSpec) ptfChain.pop();
    applyConstantPartition(prevFn);
    PartitionSpec partSpec = prevFn.getPartition();
    OrderSpec orderSpec = prevFn.getOrder();

    if ( partSpec == null ) {
      //oops this should have been caught before trying to componentize
      throw new SemanticException(
          "No Partitioning specification specified at start of a PTFChain");
    }
    if ( orderSpec == null ) {
      orderSpec = new OrderSpec(partSpec);
      prevFn.setOrder(orderSpec);
    }

    while (!ptfChain.isEmpty()) {
      PartitionedTableFunctionSpec currentFn = (PartitionedTableFunctionSpec) ptfChain.pop();
      String fnName = currentFn.getName();
      if (!FunctionRegistry.isTableFunction(fnName)) {
        throw new SemanticException(ErrorMsg.INVALID_FUNCTION.getMsg(fnName));
      }
      boolean transformsRawInput = FunctionRegistry.getTableFunctionResolver(fnName)
          .transformsRawInput();

      /*
       * if the current table function has no partition info specified: inherit it from the PTF up
       * the chain.
       */
      if (currentFn.getPartition() == null) {
        currentFn.setPartition(prevFn.getPartition());
        if (currentFn.getOrder() == null) {
          currentFn.setOrder(prevFn.getOrder());
        }
      }
      /*
       * If the current table function has no order info specified;
       */
      if (currentFn.getOrder() == null) {
        currentFn.setOrder(new OrderSpec(currentFn.getPartition()));
      }

      if (!currentFn.getPartition().equals(partSpec) ||
          !currentFn.getOrder().equals(orderSpec) ||
          transformsRawInput) {
        PTFInvocationSpec component = new PTFInvocationSpec();
        component.setFunction(prevFn);
        componentInvocations.add(component);
        PTFQueryInputSpec cQInSpec = new PTFQueryInputSpec();
        cQInSpec.setType(PTFQueryInputType.PTFCOMPONENT);
        currentFn.setInput(cQInSpec);
      }

      prevFn = currentFn;
      partSpec = prevFn.getPartition();
      orderSpec = prevFn.getOrder();
    }
    componentInvocations.add(ptfInvocation);
    return componentInvocations;
  }


  /*
   * Lead Lag functionality
   */
  public static class LeadLagInfo {
    /*
     * list of LL invocations in a Query.
     */
    List<ExprNodeGenericFuncDesc> leadLagExprs;
    /*
     * map from the Select Expr Node to the LL Function invocations in it.
     */
    Map<ExprNodeDesc, List<ExprNodeGenericFuncDesc>> mapTopExprToLLFunExprs;

    private void addLeadLagExpr(ExprNodeGenericFuncDesc llFunc)
    {
      leadLagExprs = leadLagExprs == null ? new ArrayList<ExprNodeGenericFuncDesc>() : leadLagExprs;
      leadLagExprs.add(llFunc);
    }

    public List<ExprNodeGenericFuncDesc> getLeadLagExprs()
    {
      return leadLagExprs;
    }

    public void addLLFuncExprForTopExpr(ExprNodeDesc topExpr, ExprNodeGenericFuncDesc llFuncExpr)
    {
      addLeadLagExpr(llFuncExpr);
      mapTopExprToLLFunExprs = mapTopExprToLLFunExprs == null ?
          new HashMap<ExprNodeDesc, List<ExprNodeGenericFuncDesc>>() : mapTopExprToLLFunExprs;
      List<ExprNodeGenericFuncDesc> funcList = mapTopExprToLLFunExprs.get(topExpr);
      if (funcList == null)
      {
        funcList = new ArrayList<ExprNodeGenericFuncDesc>();
        mapTopExprToLLFunExprs.put(topExpr, funcList);
      }
      funcList.add(llFuncExpr);
    }

    public List<ExprNodeGenericFuncDesc> getLLFuncExprsInTopExpr(ExprNodeDesc topExpr)
    {
      if (mapTopExprToLLFunExprs == null) {
        return null;
      }
      return mapTopExprToLLFunExprs.get(topExpr);
    }
  }

  public static void validateNoLeadLagInValueBoundarySpec(ASTNode node)
      throws SemanticException
  {
    String errMsg = "Lead/Lag not allowed in ValueBoundary Spec";
    TreeWizard tw = new TreeWizard(ParseDriver.adaptor, HiveParser.tokenNames);
    ValidateNoLeadLag visitor = new ValidateNoLeadLag(errMsg);
    tw.visit(node, HiveParser.TOK_FUNCTION, visitor);
    visitor.checkValid();
  }

  public static class ValidateNoLeadLag implements
      ContextVisitor
  {
    String errMsg;
    boolean throwError = false;
    ASTNode errorNode;

    public ValidateNoLeadLag(String errMsg)
    {
      this.errMsg = errMsg;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void visit(Object t, Object parent, int childIndex, Map labels)
    {
      ASTNode expr = (ASTNode) t;
      ASTNode nameNode = (ASTNode) expr.getChild(0);
      if (nameNode.getText().equals(FunctionRegistry.LEAD_FUNC_NAME)
          || nameNode.getText()
              .equals(FunctionRegistry.LAG_FUNC_NAME))
      {
        throwError = true;
        errorNode = expr;
      }
    }

    void checkValid() throws SemanticException
    {
      if (throwError)
      {
        throw new SemanticException(errMsg + errorNode.toStringTree());
      }
    }
  }
}
