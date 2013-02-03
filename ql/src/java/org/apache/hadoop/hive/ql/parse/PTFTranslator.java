package org.apache.hadoop.hive.ql.parse;

import static org.apache.hadoop.hive.ql.exec.PTFUtils.sprintf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.antlr.runtime.Token;
import org.antlr.runtime.tree.CommonTreeAdaptor;
import org.antlr.runtime.tree.Tree;
import org.antlr.runtime.tree.TreeAdaptor;
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
import org.apache.hadoop.hive.ql.exec.PTFUtils.Predicate;
import org.apache.hadoop.hive.ql.exec.PTFUtils.ReverseIterator;
import org.apache.hadoop.hive.ql.exec.WindowFunctionInfo;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.PTFSpec.ColumnSpec;
import org.apache.hadoop.hive.ql.parse.PTFSpec.OrderColumnSpec;
import org.apache.hadoop.hive.ql.parse.PTFSpec.OrderSpec;
import org.apache.hadoop.hive.ql.parse.PTFSpec.PTFComponentQuerySpec;
import org.apache.hadoop.hive.ql.parse.PTFSpec.PTFInputSpec;
import org.apache.hadoop.hive.ql.parse.PTFSpec.PTFTableOrSubQueryInputSpec;
import org.apache.hadoop.hive.ql.parse.PTFSpec.PartitionSpec;
import org.apache.hadoop.hive.ql.parse.PTFSpec.SelectSpec;
import org.apache.hadoop.hive.ql.parse.PTFSpec.TableFuncSpec;
import org.apache.hadoop.hive.ql.parse.PTFSpec.WindowFrameSpec;
import org.apache.hadoop.hive.ql.parse.PTFSpec.WindowFrameSpec.BoundarySpec;
import org.apache.hadoop.hive.ql.parse.PTFSpec.WindowFrameSpec.CurrentRowSpec;
import org.apache.hadoop.hive.ql.parse.PTFSpec.WindowFrameSpec.RangeBoundarySpec;
import org.apache.hadoop.hive.ql.parse.PTFSpec.WindowFrameSpec.ValueBoundarySpec;
import org.apache.hadoop.hive.ql.parse.PTFSpec.WindowFunctionSpec;
import org.apache.hadoop.hive.ql.parse.PTFSpec.WindowSpec;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.PTFDef;
import org.apache.hadoop.hive.ql.plan.PTFDef.ArgDef;
import org.apache.hadoop.hive.ql.plan.PTFDef.ColumnDef;
import org.apache.hadoop.hive.ql.plan.PTFDef.OrderColumnDef;
import org.apache.hadoop.hive.ql.plan.PTFDef.OrderDef;
import org.apache.hadoop.hive.ql.plan.PTFDef.PTFComponentQueryDef;
import org.apache.hadoop.hive.ql.plan.PTFDef.PTFInputDef;
import org.apache.hadoop.hive.ql.plan.PTFDef.PTFTableOrSubQueryInputDef;
import org.apache.hadoop.hive.ql.plan.PTFDef.PartitionDef;
import org.apache.hadoop.hive.ql.plan.PTFDef.SelectDef;
import org.apache.hadoop.hive.ql.plan.PTFDef.TableFuncDef;
import org.apache.hadoop.hive.ql.plan.PTFDef.WhereDef;
import org.apache.hadoop.hive.ql.plan.PTFDef.WindowDef;
import org.apache.hadoop.hive.ql.plan.PTFDef.WindowFrameDef;
import org.apache.hadoop.hive.ql.plan.PTFDef.WindowFrameDef.BoundaryDef;
import org.apache.hadoop.hive.ql.plan.PTFDef.WindowFrameDef.CurrentRowDef;
import org.apache.hadoop.hive.ql.plan.PTFDef.WindowFrameDef.RangeBoundaryDef;
import org.apache.hadoop.hive.ql.plan.PTFDef.WindowFrameDef.ValueBoundaryDef;
import org.apache.hadoop.hive.ql.plan.PTFDef.WindowFunctionDef;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLeadLag;
import org.apache.hadoop.hive.ql.udf.ptf.TableFunctionEvaluator;
import org.apache.hadoop.hive.ql.udf.ptf.TableFunctionResolver;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class PTFTranslator
{
  public PTFDef translate(PTFSpec qSpec, HiveConf qCfg, RowResolver rr, UnparseTranslator unparseT) throws SemanticException
  {
    PTFDef qry = new PTFDef();
    qry.setSpec(qSpec);

    PTFTranslationInfo transInfo = new PTFTranslationInfo();
    transInfo.setHiveCfg(qCfg);
    transInfo.unparseT = unparseT;
    qry.setTranslationInfo(transInfo);

    PTFTranslator.translateInput(qry, rr);
    PTFTranslator.translateWhere(qry);
    PTFTranslator.translateOutput(qry);

    return qry;
  }

  /*
   * Input translation methods
   */
  private static void translateInput(PTFDef qDef, RowResolver rr) throws SemanticException
  {
    PTFSpec spec = qDef.getSpec();

    /*
     * validate that input chain ends in a Hive Query or TAble.
     */
    if (!spec.getInput().sourcedFromHive())
    {
      throw new SemanticException("Translation not supported for HdfsLocation based queries");
    }

    PTFTranslationInfo tInfo = qDef.getTranslationInfo();
    PTFSpec qSpec = qDef.getSpec();
    Iterator<PTFInputSpec> it = PTFTranslator.iterateInputSpecs(qSpec, true);
    PTFInputDef currentIDef = null;
    int inputNum = 0;
    while (it.hasNext())
    {
      PTFInputSpec nextSpec = it.next();
      if (nextSpec instanceof PTFTableOrSubQueryInputSpec)
      {
        currentIDef = PTFTranslator.translate(qDef, (PTFTableOrSubQueryInputSpec) nextSpec,
            (PTFTableOrSubQueryInputDef) null, rr);
      }
      else if (nextSpec instanceof PTFComponentQuerySpec)
      {
        currentIDef = PTFTranslator.translate(qDef, (PTFComponentQuerySpec) nextSpec,
            (PTFComponentQueryDef) null, rr);
      }
      else
      {
        currentIDef = translate(qDef, (TableFuncSpec) nextSpec, currentIDef);
      }
      String alias = getTableAlias(qDef, inputNum, currentIDef);
      currentIDef.setAlias(alias);
      tInfo.addInput(currentIDef, inputNum == 0 ? rr : null);
      inputNum++;
    }

    qDef.setInput(currentIDef);
  }


  /*
   * todo: revisit computing the alias. For a HiveTableDef it probably only needs to be the
   * tablename.
   */
  private static String getTableAlias(PTFDef qDef, int inputNum, PTFInputDef inputDef)
      throws SemanticException
  {
    if (inputDef instanceof PTFTableOrSubQueryInputDef)
    {
      PTFTableOrSubQueryInputDef hTbldef = (PTFTableOrSubQueryInputDef) inputDef;
      String db = ((PTFTableOrSubQueryInputSpec) hTbldef.getSpec()).getDbName();
      String tableName = ((PTFTableOrSubQueryInputSpec) hTbldef.getSpec()).getTableName();
      return db + "." + tableName;
    }
    else if (inputDef instanceof PTFComponentQueryDef)
    {
      String alias = "ptf_" + inputNum ;
      return alias;
    }
    else if (inputDef instanceof TableFuncDef)
    {
      String alias = ((TableFuncDef) inputDef).getTableFuncSpec().getAlias();
      alias = alias == null ? "ptf_" + inputNum : alias;
      return alias;
    }
    throw new SemanticException(sprintf("Internal Error: attempt to translate %s",
        inputDef.getSpec()));
  }

   private static PTFComponentQueryDef translate(PTFDef qDef,
           PTFComponentQuerySpec spec,
           PTFComponentQueryDef def, RowResolver rr) throws SemanticException
       {
         def = def == null ? new PTFComponentQueryDef() : def;
         def.setSpec(spec);
         ObjectInspector oi = PTFTranslator.getInputOI(rr);
         Map<String, String> serdePropsMap = new LinkedHashMap<String, String>();
         SerDe serde = null;

         try {
           serde = PTFTranslator.createLazyBinarySerDe(
             qDef.getTranslationInfo().getHiveCfg(),
             (StructObjectInspector) oi, serdePropsMap);
         }
         catch(SerDeException se) {
           throw new SemanticException(se);
          }
          def.setSerde(serde);
          def.setCompSerdeClassName(serde.getClass().getName());
          def.setOI((StructObjectInspector) oi);
          return def;

        }


  private static PTFTableOrSubQueryInputDef translate(PTFDef qDef,
      PTFTableOrSubQueryInputSpec spec,
      PTFTableOrSubQueryInputDef def, RowResolver rr) throws SemanticException
  {
    def = def == null ? new PTFTableOrSubQueryInputDef() : def;
    def.setSpec(spec);
    ObjectInspector oi = PTFTranslator.getInputOI(rr);
    Map<String, String> serdePropsMap = new LinkedHashMap<String, String>();
    SerDe serde = null;

    try {
      serde = PTFTranslator.createLazyBinarySerDe(
        qDef.getTranslationInfo().getHiveCfg(),
        (StructObjectInspector) oi, serdePropsMap);
    }
    catch(SerDeException se) {
      throw new SemanticException(se);
    }
    def.setSerde(serde);
    def.setTableSerdeProps(serdePropsMap);
    def.setTableSerdeClassName(serde.getClass().getName());
    def.setOI((StructObjectInspector) oi);
    return def;

  }

  /*
   * <ol>
   * <li> Get the <code>TableFunctionResolver</code> for this Function from the FunctionRegistry.
   * <li> Create the TableFuncDef object.
   * <li> Get the InputInfo for the input to this function.
   * <li> Translate the Arguments to this Function in the Context of the InputInfo.
   * <li> ask the TableFunctionResolver to create a TableFunctionEvaluator based on the Args passed
   * in.
   * <li> ask the TableFunctionEvaluator to setup the Map-side ObjectInspector. Gives a chance to
   * functions that
   * reshape the Input before it is partitioned to define the Shape after raw data is transformed.
   * <li> Setup the Window Definition for this Function. The Window Definition is resolved wrt to
   * the InputDef's
   * Shape or the MapOI, for Functions that reshape the raw input.
   * <li> ask the TableFunctionEvaluator to setup the Output ObjectInspector for this Function.
   * <li> setup a Serde for the Output partition based on the OutputOI.
   * </ol>
   */
  private static TableFuncDef translate(PTFDef qDef, TableFuncSpec tSpec, PTFInputDef inputDef)
      throws SemanticException
  {
    PTFTranslationInfo tInfo = qDef.getTranslationInfo();

    TableFunctionResolver tFn = FunctionRegistry.getTableFunctionResolver(tSpec.getName());
    if (tFn == null)
    {
      throw new SemanticException(sprintf("Unknown Table Function %s", tSpec.getName()));
    }

    TableFuncDef tDef = new TableFuncDef();
    tDef.setSpec(tSpec);
    tDef.setInput(inputDef);
    PTFInputInfo iInfo = tInfo.getInputInfo(inputDef);

    /*
     * translate args
     */
    ArrayList<ASTNode> args = tSpec.getArgs();
    if (args != null)
    {
      for (ASTNode expr : args)
      {
        ArgDef argDef = null;
        try {
          argDef = translateTableFunctionArg(qDef, tDef, iInfo, expr);
        }
        catch(HiveException he) {
          throw new SemanticException(he);
        }
        tDef.addArg(argDef);
      }
    }

    tFn.initialize(qDef, tDef);
    TableFunctionEvaluator tEval = tFn.getEvaluator();
    tDef.setFunction(tEval);
    tDef.setCarryForwardNames(tFn.carryForwardNames());
    tFn.setupRawInputOI();
    tDef.setRawInputColumnNames(tFn.getRawInputColumnNames());
    tDef.setWindow(PTFTranslator.translateWindow(qDef, tDef));
    tFn.setupOutputOI();
    tDef.setOutputColumnNames(tFn.getOutputColumnNames());
    try {
      PTFTranslator.setupSerdeAndOI(tDef, inputDef, tInfo, tEval, true);
    }
    catch(SerDeException se) {
      throw new SemanticException(se);
    }

    return tDef;
  }

  private static ArgDef translateTableFunctionArg(PTFDef qDef, TableFuncDef tDef,
      PTFInputInfo iInfo, ASTNode arg) throws HiveException
  {
    return PTFTranslator.buildArgDef(qDef, iInfo, arg);
  }


  /*
   * Where Clause translation.
   */

  public static void translateWhere(PTFDef qDef) throws SemanticException
  {
    PTFTranslationInfo tInfo = qDef.getTranslationInfo();
    PTFSpec spec = qDef.getSpec();

    ASTNode wExpr = (ASTNode) spec.getWhereExpr();

    if (wExpr == null) {
      return;
    }

    wExpr = (ASTNode) wExpr.getChild(0);

    WhereDef whDef = new WhereDef();
    whDef.setExpression(wExpr);

    PTFInputDef iDef = qDef.getInput();
    PTFInputInfo iInfo = tInfo.getInputInfo(iDef);

    ExprNodeDesc exprNode = PTFTranslator.buildExprNode(wExpr, iInfo.getTypeCheckCtx());
    ExprNodeEvaluator exprEval = null;
    ObjectInspector oi = null;
    try {
      exprEval = WindowingExprNodeEvaluatorFactory.get(tInfo, exprNode);
      oi = PTFTranslator.initExprNodeEvaluator(qDef, exprNode, exprEval, iInfo);
    }
    catch(HiveException he) {
      throw new SemanticException(he);
    }
    try
    {
      ObjectInspectorConverters.getConverter(oi,
          PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);
    } catch (Throwable t)
    {
      throw new SemanticException("Where Expr must be convertible to a boolean value", t);
    }

    whDef.setExprNode(exprNode);
    whDef.setExprEvaluator(exprEval);
    whDef.setOI(oi);

    qDef.setWhere(whDef);
  }

  /*
   * Output Translation methods.
   */

  private static void translateOutput(PTFDef qDef) throws SemanticException
  {
    translateSelectExprs(qDef);
    setupSelectRRAndOI(qDef);
  }

  private static void translateSelectExprs(PTFDef qDef) throws SemanticException
  {
    PTFTranslationInfo tInfo = qDef.getTranslationInfo();
    PTFInputDef iDef = qDef.getInput();
    PTFInputInfo iInfo = tInfo.getInputInfo(iDef);
    SelectSpec selectSpec = qDef.getSpec().getSelectList();

    if (selectSpec == null) {
      return;
    }

    SelectDef selectDef = qDef.getSelectList();
    Iterator<Object> selectExprsAndAliases = selectSpec.getColumnListAndAlias();
    int i = 0;
    ColumnDef cDef = null;


    while (selectExprsAndAliases.hasNext())
    {
      Object[] o = (Object[]) selectExprsAndAliases.next();
      boolean isWnFn = ((Boolean) o[0]).booleanValue();

      if (!isWnFn)
      {
        cDef = translateSelectExpr(qDef, iInfo, i++, (String) o[1], (ASTNode) o[2]);
        selectDef.addColumn(cDef);
      }
    }
  }


  private static ColumnDef translateSelectExpr(PTFDef qDef, PTFInputInfo iInfo, int colIdx,
      String alias, ASTNode expr)
      throws SemanticException
  {
    ColumnDef cDef = new ColumnDef((ColumnSpec) null);
    ExprNodeDesc exprNode = PTFTranslator.buildExprNode(expr, iInfo.getTypeCheckCtx());
    ExprNodeEvaluator exprEval = null;
    ObjectInspector oi = null;
    try {
      exprEval = WindowingExprNodeEvaluatorFactory.get(qDef.getTranslationInfo(), exprNode);
      oi = PTFTranslator.initExprNodeEvaluator(qDef, exprNode, exprEval, iInfo);
    }
    catch(HiveException he) {
      throw new SemanticException(he);
    }

    cDef.setExpression(expr);
    cDef.setExprNode(exprNode);
    cDef.setExprEvaluator(exprEval);
    cDef.setOI(oi);

    cDef.setAlias(getAlias(alias, expr, colIdx));

    return cDef;
  }

  private static String getAlias(String alias, ASTNode expr, int columnIdx)
  {
    if (alias != null) {
      return alias;
    }

    if (expr.getToken().getType() == HiveParser.TOK_TABLE_OR_COL)
    {
      Tree child = expr.getChild(0);
      return child.getText();
    }

    return sprintf("col_%i", columnIdx);
  }

  /*
   * Setup based on the OI of the final PTF + expressions in the SelectList that are handled by the
   * Partition mechanism.
   * For expressions that are handled by the PTFOp : navigation expressions in SelectList or
   * windowing clauses
   * add the mapping from ASTNode to ColumnInfo so that the SelectOp doesn't try to evaluate these.
   */
  static void setupSelectRRAndOI(PTFDef qDef) throws SemanticException {
    PTFTranslationInfo tInfo = qDef.getTranslationInfo();
    PTFInputDef iDef = qDef.getInput();
    PTFInputInfo iInfo = tInfo.getInputInfo(iDef);
    SelectDef selectDef = qDef.getSelectList();
    RowResolver inputRR = iInfo.getRowResolver();
    SelectSpec selectSpec = qDef.getSpec().getSelectList();
    LinkedHashMap<String, ASTNode> aliasToAST = selectSpec == null ? null : selectSpec
        .getAliasToAST();
    boolean isWindowPTF = ((TableFuncDef) qDef.getInput()).getName() == FunctionRegistry.WINDOWING_TABLE_FUNCTION;
    RowResolver rr = new RowResolver();

    // should I just set it to the iDef.getAlias()?
    // if the ptf invocation didn't have an alias, the alias would be set to an internally generated
    // alias.
    String outAlias = ((TableFuncDef) qDef.getInput()).getTableFuncSpec().getAlias();

    /*
     * Give the Columns internalNames based on position.
     * The Columns are added in the following order:
     * - the columns in the SelectList processed by the PTF (ie the Select Exprs that have navigation expressions)
     * - the columns from the final PTF.
     * Why?
     * - during translation the input contains Virtual columns that are not present during runtime
     * - this messes with the Column Numbers (and hence internal Names) if we add the columns in a different order.
     */

    int pos = 0;

    if (selectDef.getColumns() != null) {
      for (ColumnDef cDef : selectDef.getColumns()) {
        String internalName = HiveConf.getColumnInternalName(pos++);
        ColumnInfo cInfo = new ColumnInfo(internalName,
            TypeInfoUtils.getTypeInfoFromObjectInspector(cDef.getOI()),
            outAlias,
            false);
        rr.putExpression(cDef.getExpression(), cInfo);
      }
    }

    for (ColumnInfo inpCInfo : inputRR.getColumnInfos()) {
      String internalName = HiveConf.getColumnInternalName(pos++);
      ColumnInfo cInfo = new ColumnInfo(inpCInfo);
      cInfo.setInternalName(internalName);
      String colAlias = cInfo.getAlias();

      if (outAlias != null) {
        cInfo.setTabAlias(outAlias);
      }

      String[] tabColAlias = inputRR.reverseLookup(inpCInfo.getInternalName());
      if (tabColAlias != null) {
        colAlias = tabColAlias[1];
      }
      if (isWindowPTF && aliasToAST.keySet().contains(colAlias)) {
        rr.putExpression(aliasToAST.get(colAlias), cInfo);
      }
      else {
        ASTNode inExpr = null;
        inExpr = PTFTranslator.getASTNode(inpCInfo, inputRR);
        if ( inExpr != null ) {
          rr.putExpression(inExpr, cInfo);
        }
        else {
          rr.put(cInfo.getTabAlias(), colAlias, cInfo);
        }
      }
    }

    selectDef.setRowResolver(rr);
    selectDef.setOI((StructObjectInspector) PTFTranslator.getInputOI(rr));
  }

  /*
   * Window Function Translation methods
   */
  public static WindowFunctionDef translate(PTFDef qDef, TableFuncDef windowTableFnDef,
      WindowFunctionSpec wFnSpec) throws SemanticException
  {
    PTFTranslationInfo tInfo = qDef.getTranslationInfo();
    PTFInputInfo iInfo = tInfo.getInputInfo(windowTableFnDef.getInput());

    WindowFunctionDef wFnDef = new WindowFunctionDef();
    wFnDef.setSpec(wFnSpec);

    /*
     * translate args
     */
    ArrayList<ASTNode> args = wFnSpec.getArgs();
    if (args != null)
    {
      for (ASTNode expr : args)
      {
        ArgDef argDef = null;
        try {
          argDef = translateWindowFunctionArg(qDef, windowTableFnDef, iInfo, expr);
        }
        catch(HiveException he) {
          throw new SemanticException(he);
        }
        wFnDef.addArg(argDef);
      }
    }

    if (RANKING_FUNCS.contains(wFnSpec.getName()))
    {
      setupRankingArgs(qDef, windowTableFnDef, wFnDef, wFnSpec);
    }

    WindowDef wDef = translateWindowSpec(qDef, iInfo, wFnSpec);
    wFnDef.setWindow(wDef);
    validateWindowDefForWFn(windowTableFnDef, wFnDef);

    try {
      setupEvaluator(wFnDef);
    }
    catch(HiveException he) {
      throw new SemanticException(he);
    }

    return wFnDef;
  }

  static void setupEvaluator(WindowFunctionDef wFnDef) throws HiveException
  {
    WindowFunctionSpec wSpec = wFnDef.getSpec();
    ArrayList<ArgDef> args = wFnDef.getArgs();
    ArrayList<ObjectInspector> argOIs = getWritableObjectInspector(args);
    GenericUDAFEvaluator wFnEval = org.apache.hadoop.hive.ql.exec.FunctionRegistry
        .getGenericUDAFEvaluator(wSpec.getName(), argOIs, wSpec.isDistinct(), wSpec.isStar());
    ObjectInspector[] funcArgOIs = null;

    if (args != null)
    {
      funcArgOIs = new ObjectInspector[args.size()];
      int i = 0;
      for (ArgDef arg : args)
      {
        funcArgOIs[i++] = arg.getOI();
      }
    }

    ObjectInspector OI = wFnEval.init(GenericUDAFEvaluator.Mode.COMPLETE, funcArgOIs);

    wFnDef.setEvaluator(wFnEval);
    wFnDef.setOI(OI);
  }

  private static ArgDef translateWindowFunctionArg(PTFDef qDef, TableFuncDef tDef,
      PTFInputInfo iInfo, ASTNode arg) throws HiveException
  {
    return PTFTranslator.buildArgDef(qDef, iInfo, arg);
  }

  private static ArrayList<ObjectInspector> getWritableObjectInspector(ArrayList<ArgDef> args)
  {
    ArrayList<ObjectInspector> result = new ArrayList<ObjectInspector>();
    if (args != null)
    {
      for (ArgDef arg : args)
      {
        result.add(arg.getOI());
      }
    }
    return result;
  }

  public static final ArrayList<String> RANKING_FUNCS = new ArrayList<String>();
  static
  {
    RANKING_FUNCS.add("rank");
    RANKING_FUNCS.add("denserank");
    RANKING_FUNCS.add("percentrank");
    RANKING_FUNCS.add("cumedist");
  };

  private static void setupRankingArgs(PTFDef qDef, TableFuncDef windowTableFnDef,
      WindowFunctionDef wFnDef, WindowFunctionSpec wSpec) throws SemanticException
  {
    if (wSpec.getArgs().size() > 0)
    {
      throw new SemanticException("Ranking Functions can take no arguments");
    }

    PTFInputDef inpDef = windowTableFnDef.getInput();
    PTFInputInfo inpInfo = qDef.getTranslationInfo().getInputInfo(inpDef);
    OrderDef oDef = getTableFuncOrderDef(windowTableFnDef);
    ArrayList<OrderColumnDef> oCols = oDef.getColumns();
    for (OrderColumnDef oCol : oCols)
    {
      try {
        wFnDef.addArg(PTFTranslator.buildArgDef(qDef, inpInfo, oCol.getExpression()));
      }
      catch(HiveException he) {
        throw new SemanticException(he);
      }
    }
  }

  private static OrderDef getTableFuncOrderDef(TableFuncDef tblFnDef) throws SemanticException
  {
    if (tblFnDef.getWindow() != null)
    {
      return tblFnDef.getWindow().getOrderDef();
    }
    PTFInputDef iDef = tblFnDef.getInput();
    if (iDef instanceof TableFuncDef)
    {
      return getTableFuncOrderDef((TableFuncDef) iDef);
    }
    throw new SemanticException("No Order by specification on Function: " + tblFnDef.getSpec());
  }

  private static WindowDef translateWindowSpec(PTFDef qDef, PTFInputInfo iInfo,
      WindowFunctionSpec wFnSpec) throws SemanticException
  {
    WindowSpec wSpec = wFnSpec.getWindowSpec();

    if (wSpec == null) {
      return null;
    }

    WindowFunctionInfo wFnInfo = FunctionRegistry.getWindowFunctionInfo(wFnSpec.getName());
    String desc = wFnSpec.toString();

    if (wSpec != null && !wFnInfo.isSupportsWindow() && wSpec.getWindow() != null)
    {
      throw new SemanticException(sprintf("Function %s doesn't support windowing", desc));
    }
    return PTFTranslator.translateWindowSpecOnInput(qDef, wSpec, iInfo, desc);
  }

  private static void validateWindowDefForWFn(TableFuncDef tFnDef, WindowFunctionDef wFnDef)
      throws SemanticException
  {
    WindowDef tWindow = tFnDef.getWindow();
    WindowDef fWindow = wFnDef.getWindow();

    PartitionDef tPart = tWindow == null ? null : tWindow.getPartDef();
    PartitionDef fPart = fWindow == null ? null : fWindow.getPartDef();

    if (!PTFTranslator.isCompatible(tPart, fPart))
    {
      throw new SemanticException(
          sprintf("Window Function '%s' has an incompatible partition clause",
              wFnDef.getSpec()));
    }

    OrderDef tOrder = tWindow == null ? null : tWindow.getOrderDef();
    OrderDef fOrder = fWindow == null ? null : fWindow.getOrderDef();
    if (!PTFTranslator.isCompatible(tOrder, fOrder))
    {
      throw new SemanticException(
          sprintf("Window Function '%s' has an incompatible order clause", wFnDef.getSpec()));
    }
  }

  public static void addInputColumnsToList(PTFDef qDef, TableFuncDef windowTableFnDef,
      ArrayList<String> fieldNames, ArrayList<ObjectInspector> fieldOIs)
  {
    PTFTranslationInfo tInfo = qDef.getTranslationInfo();
    PTFInputInfo iInfo = tInfo.getInputInfo(windowTableFnDef.getInput());

    StructObjectInspector OI = (StructObjectInspector) iInfo.getOI();
    for (StructField f : OI.getAllStructFieldRefs())
    {
      fieldNames.add(f.getFieldName());
      fieldOIs.add(f.getFieldObjectInspector());
    }
  }

  /*
   * Partitioning spec (Partition, Order and Window ) translation methods
   */
  /*
   * compute the Description to use for the Input.
   * get the inputInfo for the input: if the function has a MapPhase use the Map Inputfo.
   * invoke translateWindowSpecOnInput on WdwSpec of TblFunc
   * If TableFunc is the FunctionRegistry.WINDOWING_TABLE_FUNCTION:
   * -
   */
  static WindowDef translateWindow(PTFDef qDef, TableFuncDef tFnDef) throws SemanticException
  {
    PTFTranslationInfo tInfo = qDef.getTranslationInfo();
    TableFuncSpec tFnSpec = tFnDef.getTableFuncSpec();

    /*
     * for now the Language only allows explicit specification of Partition & Order clauses.
     * Easy to allow references to a Global Window Spec.
     */
    WindowSpec wSpec = new WindowSpec();

    wSpec.setPartition(tFnSpec.getPartition());
    wSpec.setOrder(tFnSpec.getOrder());
    PTFInputDef iDef = tFnDef.getInput();

    if (wSpec.getPartition() == null)
    {
      return null;
    }

    String desc = getInputDescription(qDef, tFnDef);
    TableFunctionEvaluator tFn = tFnDef.getFunction();
    PTFInputInfo iInfo = null;
    if (tFn.isTransformsRawInput())
    {
      iInfo = tInfo.getMapInputInfo(tFnDef);
    }
    else
    {
      iInfo = tInfo.getInputInfo(iDef);
    }

    return translateWindowSpecOnInput(qDef, wSpec, iInfo, desc);
  }

  /*
   * <ol>
   * <li> If wSpec points to a source WindowSpec. Validate that it is valid. If it hasn't been
   * already translated then translate it.
   * <li> Start with an empty WdwDef or a cloned WdwDef from the source WdwDef.
   * <li> translate the PartitionSpec if it exists. Replace the existing PDef with this; also remove
   * the OrderDef.
   * <li> translate the OrderSpec if it exists. Replace existing OrderDef with this.
   * <li> add in Partition Columns if not in OrderDef already.
   * <li> translate the WindowSpec if it exists. Replace existing WdwDef with it.
   * <li> If name is non-null add this def to TranslationInfo::nameToWdwDef map.
   * </ol>
   */
  static WindowDef translateWindowSpecOnInput(PTFDef qDef, WindowSpec wSpec, PTFInputInfo iInfo,
      String inputDesc) throws SemanticException
  {
    PTFSpec qSpec = qDef.getSpec();
    WindowDef wDef;

    fillInWindowSpec(qSpec, wSpec.getSourceId(), wSpec);
    wDef = new WindowDef(wSpec);

    PartitionSpec pSpec = wSpec.getPartition();
    OrderSpec oSpec = wSpec.getOrder();
    WindowFrameSpec wFrameSpec = wSpec.getWindow();
    PartitionDef pDef = translatePartition(qDef, iInfo, pSpec);
    OrderDef oDef = translateOrder(qDef, inputDesc, iInfo, oSpec, pDef);
    WindowFrameDef wdwDef = translateWindowFrame(qDef, wFrameSpec, iInfo);

    wDef.setPartDef(pDef);
    wDef.setOrderDef(oDef);
    wDef.setWindow(wdwDef);

    return wDef;
  }

  private static void fillInWindowSpec(PTFSpec qSpec, String sourceId, WindowSpec destWSpec)
      throws SemanticException
  {
    if (sourceId != null)
    {
      WindowSpec sourceWSpec = qSpec.getWindowSpecs().get(sourceId);
      if (sourceWSpec == null || sourceWSpec.equals(destWSpec))
      {
        throw new SemanticException(sprintf("Window Spec %s refers to an unknown source " ,
            destWSpec));
      }

      if (destWSpec.getPartition() == null)
      {
        destWSpec.setPartition(sourceWSpec.getPartition());
      }

      if (destWSpec.getOrder() == null)
      {
        destWSpec.setOrder(sourceWSpec.getOrder());
      }

      if (destWSpec.getWindow() == null)
      {
        destWSpec.setWindow(sourceWSpec.getWindow());
      }

      fillInWindowSpec(qSpec, sourceWSpec.getSourceId(), destWSpec);
    }
  }

  private static PartitionDef translatePartition(PTFDef qDef, PTFInputInfo iInfo, PartitionSpec spec)
      throws SemanticException
  {
    if (spec == null || spec.getColumns() == null || spec.getColumns().size() == 0) {
      return null;
    }

    PartitionDef pDef = new PartitionDef(spec);
    for (ColumnSpec colSpec : spec.getColumns())
    {
      ColumnDef cDef = translatePartitionColumn(qDef, iInfo, colSpec);
      pDef.addColumn(cDef);
    }
    return pDef;
  }

  private static OrderDef translateOrder(PTFDef qDef, String inputDesc, PTFInputInfo iInfo,
      OrderSpec spec, PartitionDef pDef) throws SemanticException
  {

    if (spec == null || spec.getColumns() == null || spec.getColumns().size() == 0)
    {
      if (pDef == null) {
        return null;
      }
      return new OrderDef(pDef);
    }

    if (pDef == null)
    {
      throw new SemanticException(sprintf(
          "Input %s cannot have an Order spec w/o a Partition spec", inputDesc));
    }

    OrderDef oDef = new OrderDef(spec);
    for (OrderColumnSpec colSpec : spec.getColumns())
    {
      OrderColumnDef cDef = translateOrderColumn(qDef, iInfo, colSpec);
      oDef.addColumn(cDef);
    }

    /*
     * either all partition columns must be in Order list or none must be specified.
     * If none are specified then add them all.
     */
    int numOfPartColumns = 0;
    List<OrderColumnDef> orderCols = oDef.getColumns();
    List<ColumnDef> partCols = pDef.getColumns();
    int chkSize = partCols.size();
    chkSize = chkSize > orderCols.size() ? orderCols.size() : chkSize;
    for (int i = 0; i < chkSize; i++)
    {
      if (orderCols.get(i).getSpec().getExpression().toStringTree()
          .equals(partCols.get(i).getSpec().getExpression().toStringTree()))
      {
        numOfPartColumns++;
      } else {
        break;
      }
    }

    if (numOfPartColumns != 0 && numOfPartColumns != partCols.size())
    {
      throw new SemanticException(
          sprintf(
                  "For Input %s:n all partition columns must be in order clause or none should be specified",
                  inputDesc));
    }

    ArrayList<OrderColumnDef> combinedOrderCols = new ArrayList<OrderColumnDef>();
    if (numOfPartColumns == 0)
    {
      for (ColumnDef cDef : partCols)
      {
        OrderColumnDef ocDef = new OrderColumnDef(cDef);
        combinedOrderCols.add(ocDef);
      }
      combinedOrderCols.addAll(orderCols);
      oDef.setColumns(combinedOrderCols);
    }

    return oDef;
  }

  private static OrderColumnDef translateOrderColumn(PTFDef qDef, PTFInputInfo iInfo,
      OrderColumnSpec oSpec) throws SemanticException
  {
    OrderColumnDef ocDef = new OrderColumnDef(oSpec);
    translateColumn(qDef, ocDef, iInfo, oSpec);
    PTFTranslator.validateComparable(ocDef.getOI(),
        sprintf("Partition Column %s is not comparable", oSpec));
    return ocDef;
  }


  private static ColumnDef translatePartitionColumn(PTFDef qDef, PTFInputInfo iInfo,
      ColumnSpec cSpec) throws SemanticException
  {
    ColumnDef cDef = new ColumnDef(cSpec);
    translateColumn(qDef, cDef, iInfo, cSpec);
    PTFTranslator.validateComparable(cDef.getOI(),
        sprintf("Partition Column %s is not comparable", cSpec));
    return cDef;
  }

  private static void translateColumn(PTFDef qDef, ColumnDef cDef, PTFInputInfo iInfo,
      ColumnSpec cSpec) throws SemanticException
  {
    String colTabName = cSpec.getTableName();
    if (colTabName != null && !colTabName.equals(iInfo.getAlias()))
    {
      throw new SemanticException(sprintf("Unknown Table Reference in column", cSpec));
    }

    ASTNode expr = cSpec.getExpression();
    ExprNodeDesc exprNode = PTFTranslator.buildExprNode(expr, iInfo.getTypeCheckCtx());
    ExprNodeEvaluator exprEval = null;
    ObjectInspector oi = null;

    try {
      exprEval = WindowingExprNodeEvaluatorFactory.get(qDef.getTranslationInfo(), exprNode);
      oi = PTFTranslator.initExprNodeEvaluator(qDef, exprNode, exprEval, iInfo);
    }
    catch(HiveException he) {
      throw new SemanticException(he);
    }

    cDef.setExpression(expr);
    cDef.setExprNode(exprNode);
    cDef.setExprEvaluator(exprEval);
    cDef.setOI(oi);
    cDef.setAlias(cSpec.getColumnName());
  }


  private static WindowFrameDef translateWindowFrame(PTFDef qDef, WindowFrameSpec wfSpec,
      PTFInputInfo iInfo) throws SemanticException
  {
    if (wfSpec == null)
    {
      return null;
    }

    BoundarySpec s = wfSpec.getStart();
    BoundarySpec e = wfSpec.getEnd();
    WindowFrameDef wfDef = new WindowFrameDef(wfSpec);

    wfDef.setStart(translateBoundary(qDef, s, iInfo));
    wfDef.setEnd(translateBoundary(qDef, e, iInfo));

    int cmp = s.compareTo(e);
    if (cmp > 0)
    {
      throw new SemanticException(sprintf(
          "Window range invalid, start boundary is greater than end boundary: %s", wfSpec));
    }
    return wfDef;
  }

  private static BoundaryDef translateBoundary(PTFDef qDef, BoundarySpec bndSpec, PTFInputInfo iInfo)
      throws SemanticException
  {
    if (bndSpec instanceof ValueBoundarySpec)
    {
      ValueBoundarySpec vBndSpec = (ValueBoundarySpec) bndSpec;
      ValueBoundaryDef vbDef = new ValueBoundaryDef(vBndSpec);
      PTFTranslator.validateNoLeadLagInValueBoundarySpec(vBndSpec.getExpression());
      ExprNodeDesc exprNode = PTFTranslator.buildExprNode(vBndSpec.getExpression(),
          iInfo.getTypeCheckCtx());
      vbDef.setExprNode(exprNode);
      ExprNodeEvaluator exprEval = null;
      ObjectInspector OI = null;
      try {
        exprEval = WindowingExprNodeEvaluatorFactory.get(qDef.getTranslationInfo(), exprNode);
        OI = PTFTranslator.initExprNodeEvaluator(qDef, exprNode, exprEval, iInfo);
      }
      catch(HiveException he) {
        throw new SemanticException(he);
      }
      PTFTranslator.validateValueBoundaryExprType(OI);
      vbDef.setExprEvaluator(exprEval);
      vbDef.setOI(OI);
      return vbDef;
    }
    else if (bndSpec instanceof RangeBoundarySpec)
    {
      RangeBoundarySpec rBndSpec = (RangeBoundarySpec) bndSpec;
      RangeBoundaryDef rbDef = new RangeBoundaryDef(rBndSpec);
      return rbDef;
    }
    else if (bndSpec instanceof CurrentRowSpec)
    {
      CurrentRowSpec cBndSpec = (CurrentRowSpec) bndSpec;
      CurrentRowDef cbDef = new CurrentRowDef(cBndSpec);
      return cbDef;
    }
    throw new SemanticException("Unknown Boundary: " + bndSpec);
  }

  private static String getInputDescription(PTFDef qDef, TableFuncDef tDef)
  {
    if (qDef.getInput() == tDef &&
        (tDef.getName().equals(FunctionRegistry.NOOP_TABLE_FUNCTION) ||
        tDef.getName().equals(FunctionRegistry.WINDOWING_TABLE_FUNCTION)))
    {
      return "Query";
    }
    return sprintf("TableFunction %s[alias:%s]", tDef.getName(), tDef.getAlias());

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
   * The information captured during translation of a QuerySpec into a QueryDef
   */
  public static class PTFTranslationInfo
  {
    private static final Log LOG = LogFactory.getLog("com.sap.hadoop.windowing");

    HiveConf hCfg;

    /*
     * A map from a QueryInput to the WindowDefns defined on it.
     * In the future this will enable (Partition, Order) specification for a QueryInput to refer to
     * a Window definition.
     * This will require that we accept window definitions on DataSets other then the input to the
     * Windowing Table Function.
     */
    Map<String, Map<String, WindowDef>> windowDefMap;

    /*
     * A map from a QueryInput to its Shape.
     */
    Map<String, PTFInputInfo> inputInfoMap;

    /*
     * InputInfos for table functions that rehape the input map-side.
     */
    Map<String, PTFInputInfo> mapReshapeInfoMap;

    LeadLagInfo llInfo;

    UnparseTranslator unparseT;

    public HiveConf getHiveCfg()
    {
      return hCfg;
    }

    public void setHiveCfg(HiveConf hCfg)
    {
      this.hCfg = hCfg;
    }

    void addInput(PTFInputDef input, RowResolver rr) throws SemanticException
    {
      inputInfoMap = inputInfoMap == null ? new HashMap<String, PTFInputInfo>() : inputInfoMap;

      RowResolver prevRR = null;
      if (input instanceof TableFuncDef) {
        PTFInputDef prevInpDef = ((TableFuncDef) input).getInput();
        prevRR = inputInfoMap.get(prevInpDef.getAlias()).rr;
      }

      PTFInputInfo iInfo = new PTFInputInfo(this, input, null, rr, prevRR, unparseT);
      input.setInputInfo(iInfo);
      inputInfoMap.put(input.getAlias(), iInfo);
    }

    public PTFInputInfo getMapInputInfo(TableFuncDef tDef) throws SemanticException
    {
      TableFunctionEvaluator tFn = tDef.getFunction();
      if (!tFn.isTransformsRawInput())
      {
        return null;
      }
      mapReshapeInfoMap = mapReshapeInfoMap == null ? new HashMap<String, PTFInputInfo>()
          : mapReshapeInfoMap;
      PTFInputInfo ii = mapReshapeInfoMap.get(tDef.getAlias());
      if (ii == null)
      {
        PTFInputDef prevInpDef = tDef.getInput();
        RowResolver prevRR = inputInfoMap.get(prevInpDef.getAlias()).rr;
        ii = new PTFInputInfo(this, tDef, tFn.getRawInputOI(), null, prevRR, unparseT);
        tDef.setRawInputInfo(ii);
        mapReshapeInfoMap.put(tDef.getAlias(), ii);
      }
      return ii;
    }

    public PTFInputInfo getInputInfo(PTFInputDef input)
    {
      return inputInfoMap.get(input.getAlias());
    }

    public LeadLagInfo getLLInfo()
    {
      llInfo = llInfo == null ? new LeadLagInfo() : llInfo;
      return llInfo;
    }

    public static RowResolver buildRowResolver(String tabAlias,
        StructObjectInspector rowObjectInspector,
        ArrayList<String> outputColNames,
        RowResolver inputRowResolver) throws SemanticException {
      if ( outputColNames != null ) {
        return buildRowResolver(tabAlias, rowObjectInspector, outputColNames);
      }
      else {
        return buildRowResolver(tabAlias, rowObjectInspector, inputRowResolver);
      }
    }

    protected static RowResolver buildRowResolver(String tabAlias,
        StructObjectInspector rowObjectInspector,
        ArrayList<String> outputColNames) throws SemanticException {
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

    /*
     * Construct a Row Resolver from a PTF OI.
     * For WindowTablFunction and Noop functions the PTF's Input RowResolver is also passed in.
     * This way for any columns that are in the Input Row Resolver we carry forward their
     * internalname, alias and table Alias.
     */
    protected static RowResolver buildRowResolver(String tabAlias,
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

        if ( tabColAlias != null ) {
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

        if ( inpColInfo != null ) {
          colInfo = new ColumnInfo(inpColInfo);
        }
        else {
          colInfo = new ColumnInfo(fields.get(i).getFieldName(),
              TypeInfoUtils.getTypeInfoFromObjectInspector(fields.get(i)
                  .getFieldObjectInspector()),
              tabAlias,
              false);
          colAlias = colInfo.getInternalName();
        }

        ASTNode expr = inputRowResolver == null ? null :PTFTranslator.getASTNode(inpColInfo, inputRowResolver);

        if ( expr != null ) {
          rwsch.putExpression(expr, colInfo);
        }
        else {
          rwsch.put(colTabAlias, colAlias, colInfo);
        }
      }
      return rwsch;
    }
  }

  public static class PTFInputInfo
  {
    private final boolean forMapPhase;
    private final PTFInputDef inpDef;
    private final StructObjectInspector OI;
    private final RowResolver rr;
    private final TypeCheckCtx tCtx;

    PTFInputInfo(PTFTranslationInfo tInfo, PTFInputDef input,
        StructObjectInspector mapOI, RowResolver inputRR,
        RowResolver prevInputRR,
        UnparseTranslator unparseT)
        throws SemanticException
    {
      this.inpDef = input;
      this.forMapPhase = mapOI != null;
      OI = forMapPhase ? mapOI : inpDef.getOI();

      if ( inputRR != null ) {
        rr = inputRR;
      }
      else if ( input instanceof TableFuncDef ){
        TableFuncDef tFnDef = (TableFuncDef) input;
        boolean carryFwdNames = tFnDef.isCarryForwardNames();
        ArrayList<String> outputColNames = forMapPhase ? tFnDef.getRawInputColumnNames() : tFnDef.getOutputColumnNames();
        rr = PTFTranslationInfo.buildRowResolver(null, OI, outputColNames, carryFwdNames ? prevInputRR : null);
      }
      else {
        /*
         * this is for the start of a Chain; so prevInputRR must be null.
         */
        rr = PTFTranslationInfo.buildRowResolver(null, OI, null, prevInputRR);
      }
      tCtx = new TypeCheckCtx(rr);
      tCtx.setUnparseTranslator(unparseT);
    }

    public RowResolver getRowResolver()
    {
      return rr;
    }

    public TypeCheckCtx getTypeCheckCtx()
    {
      return tCtx;
    }

    public String getAlias()
    {
      return inpDef.getAlias();
    }

    public PTFInputDef getInputDef()
    {
      return inpDef;
    }

    public ObjectInspector getOI()
    {
      return OI;
    }
  }

  public static class LeadLagInfo
  {
    List<ExprNodeGenericFuncDesc> leadLagExprs;
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

  public static ArrayList<PTFSpec> componentize(PTFSpec ptfSpec) throws SemanticException {
    ArrayList<PTFSpec> componentPTFSpecs = new ArrayList<PTFSpec>();
    Iterator<TableFuncSpec> it = PTFTranslator.iterateTableFuncSpecs(ptfSpec, true);
    TableFuncSpec prevInSpec = it.next();

    PartitionSpec partSpec = prevInSpec.getPartition();
    OrderSpec orderSpec = prevInSpec.getOrder();

    if ( partSpec == null ) {
      //oops this should have been caught before trying to componentize
      throw new SemanticException("No Partitioning specification specified at start of a PTFChain");
    }
    if ( orderSpec == null ) {
      orderSpec = new OrderSpec(partSpec);
      prevInSpec.setOrder(orderSpec);
    }

    while (it.hasNext()) {
      TableFuncSpec currentSpec = it.next();

      String fnName = currentSpec.getName();
      if ( !FunctionRegistry.isTableFunction(fnName)) {
        throw new SemanticException(ErrorMsg.INVALID_FUNCTION.getMsg(fnName));
      }
      boolean transformsRawInput = FunctionRegistry.getTableFunctionResolver(fnName).transformsRawInput();

      /*
       * if the current table function has no partition info specified: inherit it from the PTF up the chain.
       */
      if ( currentSpec.getPartition() == null ) {
        currentSpec.setPartition(partSpec);
        if ( currentSpec.getOrder() == null ) {
          currentSpec.setOrder(orderSpec);
        }
      }
      /*
       * If the current table function has no order info specified;
       */
      if ( currentSpec.getOrder() == null ) {
        currentSpec.setOrder(new OrderSpec(currentSpec.getPartition()));
      }

      if(!currentSpec.getPartition().equals(partSpec) ||
          !currentSpec.getOrder().equals(orderSpec) ||
          transformsRawInput ) {
        PTFSpec cQSpec = new PTFSpec();
        cQSpec.setInput(prevInSpec);
        componentPTFSpecs.add(cQSpec);
        PTFComponentQuerySpec cQInSpec= new PTFComponentQuerySpec();
        currentSpec.setInput(cQInSpec);
      }
      prevInSpec = currentSpec;
      partSpec = prevInSpec.getPartition();
      orderSpec = prevInSpec.getOrder();
    }

    componentPTFSpecs.add(ptfSpec);

    return componentPTFSpecs;

  }

  /*
   * Utility classes and methods
   */

  // Function Chain Iterators
  public static Predicate<PTFInputSpec> IsTableFunc = new Predicate<PTFInputSpec>() {
    @Override
    public boolean apply(PTFInputSpec obj) {
      return obj != null && obj instanceof TableFuncSpec;
    }
  };

  public static class QueryInputSpecIterator implements Iterator<PTFInputSpec>
  {
    PTFSpec qSpec;
    PTFInputSpec nextInput;

    public QueryInputSpecIterator(PTFSpec qSpec)
    {
      this.qSpec = qSpec;
      nextInput = qSpec.getInput();
    }

    @Override
    public boolean hasNext()
    {
      return nextInput != null;
    }

    @Override
    public PTFInputSpec next()
    {
      PTFInputSpec curr = nextInput;
      if (curr instanceof TableFuncSpec)
      {
        TableFuncSpec tFunc = (TableFuncSpec) curr;
        nextInput = tFunc.getInput();
      }
      else
      {
        nextInput = null;
      }
      return curr;
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }
  }

  public static class ReverseQueryInputSpecIterator extends ReverseIterator<PTFInputSpec>
  {
    ReverseQueryInputSpecIterator(PTFSpec qSpec)
    {
      super(new QueryInputSpecIterator(qSpec));
    }
  }

  public static class TableFunctionSpecIterator implements Iterator<TableFuncSpec>
  {
    QueryInputSpecIterator qSpecIt;
    TableFuncSpec nextInput;

    public TableFunctionSpecIterator(PTFSpec qSpec)
    {
      qSpecIt = new QueryInputSpecIterator(qSpec);
    }

    @Override
    public boolean hasNext()
    {
      if (qSpecIt.hasNext())
      {
        PTFInputSpec iSpec = qSpecIt.next();
        if (iSpec instanceof TableFuncSpec)
        {
          nextInput = (TableFuncSpec) iSpec;
          return true;
        }
      }
      return false;
    }

    @Override
    public TableFuncSpec next()
    {
      return nextInput;
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }
  }

  public static class ReverseTableFunctionSpecIterator extends ReverseIterator<TableFuncSpec>
  {
    ReverseTableFunctionSpecIterator(PTFSpec qSpec)
    {
      super(new TableFunctionSpecIterator(qSpec));
    }
  }

  public static class QueryInputDefIterator implements Iterator<PTFInputDef>
  {
    PTFDef qDef;
    PTFInputDef nextInput;

    public QueryInputDefIterator(PTFDef qDef)
    {
      this.qDef = qDef;
      nextInput = qDef.getInput();
    }

    @Override
    public boolean hasNext()
    {
      return nextInput != null;
    }

    @Override
    public PTFInputDef next()
    {
      PTFInputDef curr = nextInput;
      if (curr instanceof TableFuncDef)
      {
        TableFuncDef tFunc = (TableFuncDef) curr;
        nextInput = tFunc.getInput();
      }
      else
      {
        nextInput = null;
      }
      return curr;
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }
  }

  public static class ReverseQueryInputDefIterator extends ReverseIterator<PTFInputDef>
  {
    ReverseQueryInputDefIterator(PTFDef qDef)
    {
      super(new QueryInputDefIterator(qDef));
    }
  }

  static Iterator<PTFInputSpec> iterateInputSpecs(PTFSpec qSpec,
      boolean reverse)
  {
    return reverse ? new ReverseQueryInputSpecIterator(qSpec)
        : new QueryInputSpecIterator(qSpec);
  }

  public static Iterator<TableFuncSpec> iterateTableFuncSpecs(
      PTFSpec qSpec, boolean reverse)
  {
    return reverse ? new ReverseTableFunctionSpecIterator(qSpec)
        : new TableFunctionSpecIterator(qSpec);
  }

  public static Iterator<PTFInputDef> iterateInputDefs(PTFDef qDef,
      boolean reverse)
  {
    return reverse ? new ReverseQueryInputDefIterator(qDef)
        : new QueryInputDefIterator(qDef);
  }

  public static PTFTableOrSubQueryInputSpec getHiveTableSpec(PTFSpec qSpec) {
    Iterator<PTFInputSpec> it = PTFTranslator.iterateInputSpecs(qSpec, true);
    PTFInputSpec qInSpec = it.next();
    return (PTFTableOrSubQueryInputSpec) qInSpec;
  }

  public static ExprNodeDesc buildExprNode(ASTNode expr,
      TypeCheckCtx typeCheckCtx) throws SemanticException
  {
    // todo: use SemanticAnalyzer::genExprNodeDesc
    // currently SA not available to PTFTranslator.
    HashMap<Node, Object> map = TypeCheckProcFactory
        .genExprNode(expr, typeCheckCtx);
    ExprNodeDesc desc = (ExprNodeDesc) map.get(expr);
    if (desc == null) {
      String errMsg = typeCheckCtx.getError();
      if ( errMsg == null) {
        errMsg = "Error in parsing ";
      }
      throw new SemanticException(errMsg);
    }
    return desc;
  }

  public static ObjectInspector initExprNodeEvaluator(PTFDef qDef,
      ExprNodeDesc exprNode, ExprNodeEvaluator exprEval, PTFInputInfo iInfo)
      throws HiveException
  {
    ObjectInspector OI;
    OI = exprEval.initialize(iInfo.getOI());

    /*
     * if there are any LeadLag functions in this Expression Tree: - setup a
     * duplicate Evaluator for the 1st arg of the LLFuncDesc - initialize it
     * using the InputInfo provided for this Expr tree - set the duplicate
     * evaluator on the LLUDF instance.
     */
    LeadLagInfo llInfo = qDef.getTranslationInfo().getLLInfo();
    List<ExprNodeGenericFuncDesc> llFuncExprs = llInfo
        .getLLFuncExprsInTopExpr(exprNode);
    if (llFuncExprs != null)
    {
      for (ExprNodeGenericFuncDesc llFuncExpr : llFuncExprs)
      {
        ExprNodeDesc firstArg = llFuncExpr.getChildren().get(0);
        ExprNodeEvaluator dupExprEval = WindowingExprNodeEvaluatorFactory
            .get(qDef.getTranslationInfo(), firstArg);
        dupExprEval.initialize(iInfo.getOI());
        GenericUDFLeadLag llFn = (GenericUDFLeadLag) llFuncExpr
            .getGenericUDF();
        llFn.setExprEvaluator(dupExprEval);
      }
    }

    return OI;
  }

  public static ArgDef buildArgDef(PTFDef qDef, PTFInputInfo iInfo, ASTNode arg)
      throws HiveException
  {
    ArgDef argDef = new ArgDef();

    ExprNodeDesc exprNode = PTFTranslator.buildExprNode(arg,
        iInfo.getTypeCheckCtx());
    ExprNodeEvaluator exprEval = WindowingExprNodeEvaluatorFactory.get(
        qDef.getTranslationInfo(), exprNode);
    ObjectInspector oi = initExprNodeEvaluator(qDef, exprNode, exprEval,
        iInfo);

    argDef.setExpression(arg);
    argDef.setExprNode(exprNode);
    argDef.setExprEvaluator(exprEval);
    argDef.setOI(oi);
    return argDef;
  }

  public static ArrayList<? extends Object>[] getTypeMap(
      StructObjectInspector oi)
  {
    StructTypeInfo t = (StructTypeInfo) TypeInfoUtils
        .getTypeInfoFromObjectInspector(oi);
    ArrayList<String> fnames = t.getAllStructFieldNames();
    ArrayList<TypeInfo> fields = t.getAllStructFieldTypeInfos();
    return new ArrayList<?>[]
    { fnames, fields };
  }

  public static SerDe createLazyBinarySerDe(Configuration cfg,
      StructObjectInspector oi) throws SerDeException
      {
        return  createLazyBinarySerDe(cfg, oi, null);
      }

  public static SerDe createLazyBinarySerDe(Configuration cfg,
      StructObjectInspector oi, Map<String,String> serdePropsMap) throws SerDeException
  {
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
  private static void addOIPropertiestoSerDePropsMap(StructObjectInspector OI, Map<String,String> serdePropsMap) {

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

    serdePropsMap.put(org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS, cNames.toString());
    serdePropsMap.put(org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMN_TYPES, cTypes.toString());
  }

  /*
   * @remove
   * used for validating that OI columnNames are the same during runtime.
   */
  @SuppressWarnings("unused")
  private static void checkSame(Map<String,String> transProps, Map<String,String> runtimeProps) throws SerDeException {
    String transNames = transProps.get(org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS);
    String runtimeNames = runtimeProps.get(org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS);

    if ( !transNames.startsWith(runtimeNames) ) {
      /*
       * Why is this false?
       * There is one case where the translation names don't match the runtime names.
       * If a UDAF expression has an alias and is used in the having clause (see test 52.)
       * it is added to the ReduceSink Operator's RowResolver twice with the same name.
       * But at runtime each instance of this column is given a different internal Name.
       * This doesn't seem to affect the child Operators of ReduceSink because their
       * ColumnExprNodeDesc's can still get to the one instance of the column in the RowResolver.
       */
      if ( false ) {
        throw new SerDeException("Runtime colNames changed: " + transNames + " -> " + runtimeNames );
      }
    }

    String transTypes = transProps.get(org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMN_TYPES);
    String runtimeTypes = runtimeProps.get(org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMN_TYPES);

    /*
     * Why is this false?
     * - see above
     * @revisit NPath tpath TypeInformation has alias names in Struct during translation; internalNames during runtime.
     */
    if ( !transTypes.startsWith(runtimeTypes)) {
      if ( false ) {
        throw new SerDeException("Runtime colTypes changed: " + transTypes + " -> " + runtimeTypes );
      }
    }
  }

  @SuppressWarnings("unchecked")
  public static Properties buildSerDePropertiesFromOI(StructObjectInspector OI) {
    Properties p = new Properties();
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
    p.setProperty(org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS,
        cNames.toString());
    p.setProperty(
        org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMN_TYPES,
        cTypes.toString());

    return p;
  }


  public static ASTNode buildASTNode(String colName)
  {
    TreeWizard tw = new TreeWizard(adaptor, HiveParser.tokenNames);
    Object o = tw.create(sprintf("(TOK_TABLE_OR_COL Identifier[%s])", colName));
    return (ASTNode) o;
  }

  public static void validateComparable(ObjectInspector OI, String errMsg)
      throws SemanticException
  {
    if (!ObjectInspectorUtils.compareSupported(OI))
    {
      throw new SemanticException(errMsg);
    }
  }

  /*
   * - equal if there columnName & tableName match - a null tableName is
   * interpreted as matching the other tableName
   */
  public static boolean isEqual(ColumnSpec spec1, ColumnSpec spec2)
  {
    if (spec1 == null && spec2 == null) {
      return false;
    }
    if (spec1 == null && spec2 != null) {
      return false;
    }
    if (spec1 != null && spec2 == null) {
      return false;
    }

    if (!spec1.getExpression().toStringTree().equals(spec2.getExpression().toStringTree())) {
      return false;
    }

    String t1 = spec1.getTableName();
    String t2 = spec2.getTableName();
    if (t1 == null || t2 == null) {
      return true;
    }

    return t1.equals(t2);
  }

  public static boolean isEqual(ColumnDef def1, ColumnDef def2)
  {
    if (def1 == null && def2 == null) {
      return false;
    }
    if (def1 == null && def2 != null) {
      return false;
    }
    if (def1 != null && def2 == null) {
      return false;
    }

    return isEqual(def1.getSpec(), def2.getSpec());
  }

  /**
   * For NOOP table functions, the serde is the same as that on the input
   * hive table,; for other table functions it is the lazy binary serde.
   * If the query has a map-phase, the map oi is set to be the oi on the
   * lazy binary serde unless the table function is a NOOP_MAP_TABLE_FUNCTION
   * (in which case it is set to the oi on the serde of the input hive
   * table definition).
   * @param tDef
   * @param inputDef
   * @param tInfo
   * @param tEval
   * @throws SerDeException
   */
  public static void setupSerdeAndOI(TableFuncDef tDef,
      PTFInputDef inputDef, PTFTranslationInfo tInfo,
      TableFunctionEvaluator tEval, boolean translationTime) throws SerDeException
  {
    /*
     * setup the SerDe.
     */
    SerDe serde = null;
    HashMap<String, String> serDePropsMap = new HashMap<String, String>();
    // treat Noop Function special because it just hands the input Partition
    // to the next function in the chain.
    if (tDef.getName().equals(FunctionRegistry.NOOP_TABLE_FUNCTION)
        || tDef.getName().equals(
            FunctionRegistry.NOOP_MAP_TABLE_FUNCTION))
    {
      serde = inputDef.getSerde();
      addOIPropertiestoSerDePropsMap((StructObjectInspector) serde.getObjectInspector(), serDePropsMap);
    }
    else
    {
      serde = PTFTranslator.createLazyBinarySerDe(tInfo.getHiveCfg(),
          tEval.getOutputOI(), serDePropsMap);
    }
    tDef.setSerde(serde);
    if ( translationTime ) {
      tDef.setOutputSerdeProps(serDePropsMap);
    }
    else {
      checkSame(tDef.getOutputSerdeProps(), serDePropsMap);
    }
    tDef.setOI((StructObjectInspector) serde.getObjectInspector());

    if (tEval.isTransformsRawInput())
    {
      serDePropsMap.clear();
      if (tDef.getName().equals(FunctionRegistry.NOOP_MAP_TABLE_FUNCTION))
      {
        serde = inputDef.getSerde();
        addOIPropertiestoSerDePropsMap((StructObjectInspector) serde.getObjectInspector(), serDePropsMap);
      }
      else
      {
        serde = PTFTranslator.createLazyBinarySerDe(
            tInfo.getHiveCfg(), tEval.getRawInputOI(), serDePropsMap);
      }
      tDef.setRawInputOI((StructObjectInspector) serde
          .getObjectInspector());
      if ( translationTime ) {
        tDef.setRawInputSerdeProps(serDePropsMap);
      }
      else {
        checkSame(tDef.getRawInputSerdeProps(), serDePropsMap);
      }
    }

  }

  /**
   * Returns true if the query needs a map-side reshape. PTFOperator is added
   * on the map-side before ReduceSinkOperator in this scenario.
   *
   * @param qdef
   * @return
   * @throws SemanticException
   */
  public static boolean addPTFMapOperator(PTFDef qdef) throws SemanticException {
    boolean hasMap = false;
    TableFuncDef tabDef = PTFTranslator.getFirstTableFunction(qdef);
    TableFunctionEvaluator tEval = tabDef.getFunction();
    if (tEval.isTransformsRawInput()) {
      hasMap = true;
    }
    return hasMap;

  }

  /**
   * For each column on the input RR, construct a StructField for it
   * OI is constructed using the list of input column names and
   * their corresponding OIs.
   * @param rr
   * @return
   */
  public static ObjectInspector getInputOI(RowResolver rr) {
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

  public static ExprNodeColumnDesc getStringColumn(String columnName)
  {
    return new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo,
        columnName, "", false);
  }

  /*
   * A Window Function's partition clause must exactly match that of the
   * associated tableFn.
   */
  public static boolean isCompatible(PartitionDef tFnPartDef,
      PartitionDef wFnPartDef)
  {
    if (tFnPartDef == null && wFnPartDef == null) {
      return true;
    }
    if (tFnPartDef == null && wFnPartDef != null) {
      return false;
    }
    if (tFnPartDef != null && wFnPartDef == null) {
      return true;
    }

    ArrayList<ColumnDef> cols1 = tFnPartDef.getColumns();
    ArrayList<ColumnDef> cols2 = wFnPartDef.getColumns();
    if (cols1.size() != cols2.size()) {
      return false;
    }
    for (int i = 0; i < cols1.size(); i++)
    {
      boolean e = isEqual(cols1.get(i), cols2.get(i));
      if (!e) {
        return false;
      }
    }
    return true;
  }

  public static boolean isEqual(OrderColumnSpec spec1, OrderColumnSpec spec2)
  {
    if (spec1 == null && spec2 == null) {
      return false;
    }
    if (spec1 == null && spec2 != null) {
      return false;
    }
    if (spec1 != null && spec2 == null) {
      return false;
    }

    if (!spec1.getExpression().toStringTree().equals(spec2.getExpression().toStringTree())) {
      return false;
    }
    if (!spec1.getOrder().equals(spec2.getOrder())) {
      return false;
    }

    String t1 = spec1.getTableName();
    String t2 = spec2.getTableName();
    if (t1 == null || t2 == null) {
      return true;
    }

    return t1.equals(t2);
  }

  public static boolean isEqual(OrderColumnDef def1, OrderColumnDef def2)
  {
    if (def1 == null && def2 == null) {
      return false;
    }
    if (def1 == null && def2 != null) {
      return false;
    }
    if (def1 != null && def2 == null) {
      return false;
    }

    return isEqual((OrderColumnSpec) def1.getSpec(),
        (OrderColumnSpec) def2.getSpec());
  }

  public static boolean isCompatible(OrderDef tFnOrderDef, OrderDef wFnOrderDef)
  {
    if (tFnOrderDef == null && wFnOrderDef == null) {
      return true;
    }
    if (tFnOrderDef == null && wFnOrderDef != null) {
      return false;
    }
    if (tFnOrderDef != null && wFnOrderDef == null) {
      return true;
    }

    ArrayList<OrderColumnDef> cols1 = tFnOrderDef.getColumns();
    ArrayList<OrderColumnDef> cols2 = wFnOrderDef.getColumns();
    if (cols1.size() != cols2.size()) {
      return false;
    }
    for (int i = 0; i < cols1.size(); i++)
    {
      boolean e = isEqual(cols1.get(i), cols2.get(i));
      if (!e) {
        return false;
      }
    }
    return true;
  }

  public static void validateValueBoundaryExprType(ObjectInspector OI)
      throws SemanticException
  {
    if (!OI.getCategory().equals(Category.PRIMITIVE))
    {
      throw new SemanticException(
          "Value Boundary expression must be of primitve type");
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
    case TIMESTAMP:
      break;
    default:
      throw new SemanticException(
          sprintf("Primitve type %s not supported in Value Boundary expression",
              pC));
    }

  }

  /**
   * Iterate the list of the query input definitions in reverse order
   * Return the first table function definition in the chain.
   * This table function is the first one to be executed on the
   * input hive table.
   * @param qDef
   * @return
   */
  public static TableFuncDef getFirstTableFunction(PTFDef qDef){
    TableFuncDef tabDef = null;
    Iterator<PTFInputDef> it = PTFTranslator.iterateInputDefs(qDef, true);
    while(it.hasNext()){
      PTFInputDef qIn = it.next();
      if(qIn instanceof TableFuncDef){
        tabDef = (TableFuncDef) qIn;
        break;
      }
    }
    return tabDef;

  }

  public static int compare(Object[] o1, ObjectInspector[] oi1, Object[] o2,
      ObjectInspector[] oi2)
  {
    int c = 0;
    for (int i = 0; i < oi1.length; i++)
    {
      c = ObjectInspectorUtils.compare(o1[i], oi1[i], o2[i], oi2[i]);
      if (c != 0) {
        return c;
      }
    }
    return c;
  }

  public static Object[] copyToStandardObject(Object[] o,
      ObjectInspector[] oi,
      ObjectInspectorCopyOption objectInspectorOption)
  {
    Object[] out = new Object[o.length];
    for (int i = 0; i < oi.length; i++)
    {
      out[i] = ObjectInspectorUtils.copyToStandardObject(o[i], oi[i],
          objectInspectorOption);
    }
    return out;
  }

  /**
   * Copied from Hive ParserDriver.
   */
  public static final TreeAdaptor adaptor = new CommonTreeAdaptor()
  {
    /**
     * Creates an ASTNode for the given token. The ASTNode is a wrapper
     * around antlr's CommonTree class that implements the Node interface.
     *
     * @param payload
     *            The token.
     * @return Object (which is actually an ASTNode) for the token.
     */
    @Override
    public Object create(Token payload)
    {
      return new ASTNode(payload);
    }
  };

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
    int n = adaptor.getChildCount(t);
    for (int i=0; i<n; i++) {
      Object child = adaptor.getChild(t, i);
      _visit(child, t, i, visitor);
    }
  }

  public static void validateNoLeadLagInValueBoundarySpec(ASTNode node)
      throws SemanticException
  {
    validateNoLeadLagInValueBoundarySpec(node, "Lead/Lag not allowed in ValueBoundary Spec");
  }

  public static void validateNoLeadLagInValueBoundarySpec(ASTNode node, String errMsg)
      throws SemanticException
  {
    TreeWizard tw = new TreeWizard(adaptor, HiveParser.tokenNames);
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

  /*
   * Walker & PTFDef Deserializer
   */
  public static class PTFDefWalker
  {
    PTFDefDeserializer visitor;

    public PTFDefWalker()
    {
      super();
    }

    public PTFDefWalker(PTFDefDeserializer visitor)
    {
      super();
      this.visitor = visitor;
    }

    /**
     * Use the visitor implementation to walk and reconstruct
     * the table functions, where and select constructs and
     * the query output definition.
     * @param qDef
     * @throws HiveException
     */
    public void walk(PTFDef qDef) throws HiveException
    {
      visitor.initialize(qDef);
      walkInputChain(qDef);

      if ( qDef.getWhere() != null )
      {
        visitor.visit(qDef.getWhere());
      }

      walk(qDef.getSelectList());
      //visitor.visit(qDef.getOutput());

      visitor.finish();
    }

    /**
     * Iterate the table functions and hive table
     * to reconstruct the definitions
     * in reverse order or the order of invocations.
     * HiveTableDef->PTF1->PTF2->WINDOWF
     * @param qDef
     * @throws HiveException
     */
    protected void walkInputChain(PTFDef qDef) throws HiveException
    {
      Iterator<PTFInputDef> it = PTFTranslator.iterateInputDefs(qDef, true);
      while(it.hasNext())
      {
        PTFInputDef nextDef = it.next();
        if (nextDef instanceof PTFTableOrSubQueryInputDef)
        {
          visitor.visit((PTFTableOrSubQueryInputDef) nextDef);
        }
        else if (nextDef instanceof PTFComponentQueryDef)
        {
          visitor.visit((PTFComponentQueryDef) nextDef);
        }
        else
        {
          walk(qDef, (TableFuncDef) nextDef);
        }
      }
    }

    /**
     * 1. Reconstruct the InputInfo during previsit
     * 2. setup OIs and Evaluators on the ArgDefs
     * 3. setup OIs and Evaluators on the columns in the
     *    PartitionDef, OrderDef and WindowFrameDef in the
     *    WindowFunction definitions
     * 4. walk the functions on the select list to
     *    setup the OI and GenericUDAFEvaluators
     *
     * @param qDef
     * @param tblFunc
     * @throws HiveException
     */
    protected void walk(PTFDef qDef, TableFuncDef tblFunc) throws HiveException
    {
      // 1. visit the Args; these are resolved based on the shape of the Input to the function.
      walk(tblFunc.getArgs());

      // 2. allow visitor to establish input
      visitor.preVisit(tblFunc);

      //3. walk the window objects; this resolved based on the rawInputTransformation, if it has happened.
      walk(tblFunc.getWindow());

      //4. for WindowTable Func walk the Window Functions on the Select List
      String fName = tblFunc.getName();
      if ( fName.equals(FunctionRegistry.WINDOWING_TABLE_FUNCTION))
      {
        SelectDef select = qDef.getSelectList();
        ArrayList<WindowFunctionDef> wFns = select.getWindowFuncs();
        for(WindowFunctionDef wFn : wFns)
        {
          walk(wFn.getWindow());
          walk(wFn.getArgs());
          visitor.visit(wFn);
        }
      }

      //5. revisit tblFunc
      visitor.visit(tblFunc);
    }

    /**
     * Setup the OIs and evaluators on ArgDefs
     * @param args
     * @throws HiveException
     */
    protected void walk(ArrayList<ArgDef> args) throws HiveException
    {
      if ( args != null )
      {
        for(ArgDef arg :args)
        {
          visitor.visit(arg);
        }
      }
    }

    /**
     * Visit the partition columns and order columns
     * Visit the window frame definitions
     * @param window
     * @throws HiveException
     */
    protected void walk(WindowDef window) throws HiveException
    {
      if ( window == null ) {
        return;
      }

      PartitionDef pDef = window.getPartDef();
      if(pDef != null){
        ArrayList<ColumnDef> cols = pDef.getColumns();
        for(ColumnDef col : cols)
        {
          visitor.visit(col);
        }
        visitor.visit(pDef);
      }


      OrderDef oDef = window.getOrderDef();
      if(oDef != null){
        ArrayList<OrderColumnDef> ocols = oDef.getColumns();
        for(OrderColumnDef ocol : ocols)
        {
          visitor.visit(ocol);
        }
        visitor.visit(oDef);
      }

      WindowFrameDef wFrmDef = window.getWindow();
      if ( wFrmDef != null)
      {
        walk(wFrmDef.getStart());
        walk(wFrmDef.getEnd());
        visitor.visit(wFrmDef);
      }
      visitor.visit(window);
    }

    /**
     * Visit all the implementations of
     * BoundaryDef
     * @param boundary
     * @throws HiveException
     */
    protected void walk(BoundaryDef boundary) throws HiveException
    {
      if ( boundary instanceof ValueBoundaryDef )
      {
        visitor.visit((ValueBoundaryDef)boundary);
      }
      else if ( boundary instanceof RangeBoundaryDef)
      {
        visitor.visit((RangeBoundaryDef)boundary);
      }
      else if ( boundary instanceof CurrentRowDef)
      {
        visitor.visit((CurrentRowDef)boundary);
      }
    }

    /**
     * Visit all the columns in the select list
     * to setup their OIs and evaluators
     * @param select
     * @throws HiveException
     */
    protected void walk(SelectDef select) throws HiveException
    {
      ArrayList<ColumnDef> cols = select.getColumns();
      if ( cols != null )
      {
        for(ColumnDef col : cols)
        {
          visitor.visit(col);
        }
      }
      visitor.visit(select);
    }

  }

  /*
   * An implementation of the {@link QueryDefVisitor} to reconstruct
   * the OIs, serdes and evaluators on the QueryDef.
   * This follows the same order used in the translation logic.
   */
  public static class PTFDefDeserializer
  {
    HiveConf hConf;
    PTFDef qDef;
    PTFInputDef qInDef;
    PTFInputInfo inputInfo;
    PTFTranslationInfo tInfo;
    ObjectInspector inputOI;
    TableFunctionResolver currentTFnResolver;

    // TODO get rid of this dependency
    static
    {
      FunctionRegistry.getWindowFunctionInfo("rank");
    }

    public PTFDefDeserializer(HiveConf hc, ObjectInspector inputOI)
    {
      this.hConf = hc;
      this.inputOI = inputOI;
    }

    public PTFDefDeserializer(HiveConf hc)
    {
      this.hConf = hc;
    }

    /*
     * Create new instance for the translation info and set the hiveConf on it
     */

    public void initialize(PTFDef queryDef)
    {
      qDef = queryDef;
      tInfo = new PTFTranslationInfo();
      tInfo.setHiveCfg(hConf);
      qDef.setTranslationInfo(tInfo);

    }

    public void visit(PTFComponentQueryDef compInputDef) throws HiveException
    {
      this.qInDef = compInputDef;
      String serDeClassName = compInputDef.getCompSerdeClassName();

      try
      {
        SerDe serDe = (SerDe) SerDeUtils.lookupDeserializer(serDeClassName);
        serDe.initialize(hConf, PTFTranslator.buildSerDePropertiesFromOI((StructObjectInspector)inputOI));
        compInputDef.setSerde(serDe);
        compInputDef.setOI((StructObjectInspector)inputOI);
      }
      catch (SerDeException se)
      {
        throw new HiveException(se);
      }

      tInfo.addInput(compInputDef, null);
      inputInfo = tInfo.getInputInfo(compInputDef);

    }


    /*
     * 1. Use the passed in InputOI to setup the SerDe and OI for the HiveTableDef.
     *    The runtime OI maybe different from the translation time oI (missing virtual columns).
     * 2. We add the hive table definition to the input
     *    map on the query translation info.
     */
    public void visit(PTFTableOrSubQueryInputDef hiveTable) throws HiveException
    {
      this.qInDef = hiveTable;

      String serDeClassName = hiveTable.getTableSerdeClassName();
      Properties serDeProps = new Properties();
      Map<String, String> serdePropsMap = hiveTable.getTableSerdeProps();
      for (String serdeName : serdePropsMap.keySet())
      {
        serDeProps.setProperty(serdeName, serdePropsMap.get(serdeName));
      }

      try
      {
        SerDe serDe = (SerDe) SerDeUtils.lookupDeserializer(serDeClassName);
        //serDe.initialize(hConf, serDeProps);
        serDe.initialize(hConf, PTFTranslator.buildSerDePropertiesFromOI((StructObjectInspector)inputOI));
        hiveTable.setSerde(serDe);
        //hiveTable.setOI((StructObjectInspector) serDe.getObjectInspector());
        hiveTable.setOI((StructObjectInspector)inputOI);
      }
      catch (SerDeException se)
      {
        throw new HiveException(se);
      }

      tInfo.addInput(hiveTable, null);
      inputInfo = tInfo.getInputInfo(hiveTable);
    }

    /*
     * If the query has a map phase, the inputInfo is retrieved from the map
     * output info of the table function definition. This is constructed using
     * the map output oi of the table function definition. If the query does not
     * have a map phase, the inputInfo is retrieved from the QueryInputDef
     * (either HiveTableDef or HiveQueryDef) of the query.
     */
    public void preVisit(TableFuncDef tblFuncDef) throws HiveException
    {
      TableFunctionEvaluator tEval = tblFuncDef.getFunction();
      currentTFnResolver = FunctionRegistry.getTableFunctionResolver(tEval.getTableDef().getName());
      currentTFnResolver.initialize(qDef, tblFuncDef, tEval);
      if (tEval.isTransformsRawInput())
      {
        currentTFnResolver.initializeRawInputOI();
        inputInfo = qDef.getTranslationInfo().getMapInputInfo(tblFuncDef);
      }
      else
      {
        inputInfo = qDef.getTranslationInfo().getInputInfo(qInDef);
      }
    }

    /*
     * 1. Invoke setupOI on the TableFunctionEvaluator
     * 2. Setup serde and OI on the table function definition
     * 3. Add the table function definition to input map
     *    on the query translation info
     * 4. Reset the inputInfo to the one associated
     *    with this table function definition.
     */
    public void visit(TableFuncDef tblFuncDef) throws HiveException
    {
      TableFunctionEvaluator tEval = tblFuncDef.getFunction();
      currentTFnResolver.initializeOutputOI();
      try {
        PTFTranslator.setupSerdeAndOI(tblFuncDef, qInDef, tInfo, tEval, false);
      }
      catch(SerDeException se) {
        throw new SemanticException(se);
      }
      tInfo.addInput(tblFuncDef, null);
      inputInfo = qDef.getTranslationInfo().getInputInfo(tblFuncDef);
    }

    /*
     * Recreate the ExprEvaluator, OI using the current inputInfo This is the
     * inputInfo on the first InputDef in chain if the query does not have a map
     * phase; else it is the mapInputInfo on the table function definition
     */
    public void visit(ArgDef arg) throws HiveException
    {
      ExprNodeDesc exprNodeDesc = arg.getExprNode();
      ExprNodeEvaluator exprEval = WindowingExprNodeEvaluatorFactory.get(
          tInfo, exprNodeDesc);
      ObjectInspector oi = PTFTranslator.initExprNodeEvaluator(qDef,
          exprNodeDesc, exprEval, inputInfo);

      arg.setExprEvaluator(exprEval);
      arg.setOI(oi);

    }

    /*
     * Recreate ExprNodeEvaluator, OI using InputInfo of first InputDef in
     * chain.
     */
    public void visit(ColumnDef column) throws HiveException
    {
      ExprNodeEvaluator exprEval = WindowingExprNodeEvaluatorFactory.get(
          tInfo, column.getExprNode());
      ObjectInspector oi = PTFTranslator.initExprNodeEvaluator(qDef,
          column.getExprNode(), exprEval, inputInfo);
      column.setExprEvaluator(exprEval);
      column.setOI(oi);
    }

    /*
     * Same as visit on {@link ColumnDef}
     */
    public void visit(OrderColumnDef column) throws HiveException
    {
      visit((ColumnDef) column);
    }

    /*
     * Recreate the ExprEvaluator, OI using the current inputInfo This is the
     * inputInfo on the first InputDef in chain if the query does not have a map
     * phase; else it is the mapInputInfo on the table function definition
     */
    public void visit(ValueBoundaryDef boundary) throws HiveException
    {
      ExprNodeEvaluator exprEval = WindowingExprNodeEvaluatorFactory.get(
          tInfo, boundary.getExprNode());
      ObjectInspector oi = PTFTranslator.initExprNodeEvaluator(qDef,
          boundary.getExprNode(), exprEval, inputInfo);
      boundary.setExprEvaluator(exprEval);
      boundary.setOI(oi);
    }

    /*
     * Setup the evaluators and OIs - Recreate the GenericUDAFEvaluator and use
     * this and OIs on function arguments to reconstruct the OI on the window
     * function definition
     */
    public void visit(WindowFunctionDef wFn) throws HiveException
    {
      PTFTranslator.setupEvaluator(wFn);
    }

    /*
     * Recreate ExprNodeEvaluator, OI using InputInfo of first InputDef in
     * chain.
     */
    public void visit(WhereDef where) throws HiveException
    {
      ExprNodeEvaluator exprEval = WindowingExprNodeEvaluatorFactory.get(
          tInfo, where.getExprNode());
      ObjectInspector oi = PTFTranslator.initExprNodeEvaluator(qDef,
          where.getExprNode(), exprEval, inputInfo);
      where.setExprEvaluator(exprEval);
      where.setOI(oi);
    }

    /*
     * Recreate OI on select list.
     */
    public void visit(SelectDef select) throws HiveException
    {
      PTFTranslator.setupSelectRRAndOI(qDef);
    }

    public void finish() throws HiveException
    {
    }

    public void visit(WindowDef window) throws HiveException
    {
    }

    public void visit(PartitionDef partition) throws HiveException
    {
    }

    public void visit(OrderDef order) throws HiveException
    {
    }

    public void visit(WindowFrameDef windowFrame) throws HiveException
    {
    }

    public void visit(CurrentRowDef boundary) throws HiveException
    {
    }

    public void visit(RangeBoundaryDef boundary) throws HiveException
    {
    }

  }


}

