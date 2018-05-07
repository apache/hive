/*
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

package org.apache.hadoop.hive.ql.udf.ptf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.PTFPartition;
import org.apache.hadoop.hive.ql.exec.PTFPartition.PTFPartitionIterator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.PTFTranslator;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.TypeCheckCtx;
import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowExpressionSpec;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.ql.plan.ptf.PTFExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.PTFInputDef;
import org.apache.hadoop.hive.ql.plan.ptf.PartitionedTableFunctionDef;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * return rows that meet a specified pattern. Use symbols to specify a list of expressions
 * to match.
 * Pattern is used to specify a Path. The results list can contain expressions based on
 * the input columns and also the matched Path.
 * <ol>
 * <li><b>pattern:</b> pattern for the Path. Path is 'dot' separated list of symbols.
 * Each element is treated as a symbol. Elements that end in '*' or '+' are interpreted with
 * the usual meaning of zero or more, one or more respectively. For e.g.
 * "LATE.EARLY*.ONTIMEOREARLY" implies a sequence of flights
 * where the first occurrence was LATE, followed by zero or more EARLY flights,
 * followed by a ONTIME or EARLY flight.
 * <li><b>symbols</b> specify a list of name, expression pairs. For e.g.
 * 'LATE', arrival_delay > 0, 'EARLY', arrival_delay < 0 , 'ONTIME', arrival_delay == 0.
 * These symbols can be used in the Pattern defined above.
 * <li><b>resultSelectList</b> specified as a select list.
 * The expressions in the selectList are evaluated in the context where all the
 * input columns are available, plus the attribute
 * "tpath" is available. Path is a collection of rows that represents the matching Path.
 * </ol>
 */
public class MatchPath extends TableFunctionEvaluator
{
  private transient String patternStr;
  private transient SymbolsInfo symInfo;
  private transient String resultExprStr;
  private transient SymbolFunction syFn;
  private ResultExprInfo resultExprInfo;
  /*
   * the names of the Columns of the input to MatchPath. Used to setup the tpath Struct column.
   */
  private HashMap<String,String> inputColumnNamesMap;

  @Override
  public void execute(PTFPartitionIterator<Object> pItr, PTFPartition outP) throws HiveException
  {
    while (pItr.hasNext())
    {
      Object iRow = pItr.next();

      SymbolFunctionResult syFnRes = SymbolFunction.match(syFn, iRow, pItr);
      if (syFnRes.matches )
      {
        int sz = syFnRes.nextRow - (pItr.getIndex() - 1);
        Object selectListInput = MatchPath.getSelectListInput(iRow,
            tableDef.getInput().getOutputShape().getOI(), pItr, sz);
        ArrayList<Object> oRow = new ArrayList<Object>();
        for(ExprNodeEvaluator resExprEval : resultExprInfo.resultExprEvals)
        {
          oRow.add(resExprEval.evaluate(selectListInput));
        }
        outP.append(oRow);
      }
    }
  }

  static void throwErrorWithSignature(String message) throws SemanticException
  {
    throw new SemanticException(String.format(
        "MatchPath signature is: SymbolPattern, one or more SymbolName, " +
        "expression pairs, the result expression as a select list. Error %s",
        message));
  }

  public HashMap<String,String> getInputColumnNames() {
    return inputColumnNamesMap;
  }

  public void setInputColumnNames(HashMap<String,String> inputColumnNamesMap) {
    this.inputColumnNamesMap = inputColumnNamesMap;
  }

  public static class MatchPathResolver extends TableFunctionResolver
  {

    @Override
    protected TableFunctionEvaluator createEvaluator(PTFDesc ptfDesc,
        PartitionedTableFunctionDef tDef)
    {

      return new MatchPath();
    }

    /**
     * <ul>
     * <li> check structure of Arguments:
     * <ol>
     * <li> First arg should be a String
     * <li> then there should be an even number of Arguments:
     * String, expression; expression should be Convertible to Boolean.
     * <li> finally there should be a String.
     * </ol>
     * <li> convert pattern into a NNode chain.
     * <li> convert symbol args into a Symbol Map.
     * <li> parse selectList into SelectList struct. The inputOI used to translate
     * these expressions should be based on the
     * columns in the Input, the 'path.attr'
     * </ul>
     */
    @Override
    public void setupOutputOI() throws SemanticException
    {
      MatchPath evaluator = (MatchPath) getEvaluator();
      PartitionedTableFunctionDef tDef = evaluator.getTableDef();

      List<PTFExpressionDef> args = tDef.getArgs();
      int argsNum = args == null ? 0 : args.size();

      if ( argsNum < 4 )
      {
        throwErrorWithSignature("at least 4 arguments required");
      }

      validateAndSetupPatternStr(evaluator, args);
      validateAndSetupSymbolInfo(evaluator, args, argsNum);
      validateAndSetupResultExprStr(evaluator, args, argsNum);
      setupSymbolFunctionChain(evaluator);

      /*
       * setup OI for input to resultExpr select list
       */
      RowResolver selectListInputRR = MatchPath.createSelectListRR(evaluator, tDef.getInput());

      /*
       * parse ResultExpr Str and setup OI.
       */
      ResultExpressionParser resultExprParser =
          new ResultExpressionParser(evaluator.resultExprStr, selectListInputRR);
      try {
        resultExprParser.translate();
      }
      catch(HiveException he) {
        throw new SemanticException(he);
      }
      evaluator.resultExprInfo = resultExprParser.getResultExprInfo();
      StructObjectInspector OI = evaluator.resultExprInfo.resultOI;

      setOutputOI(OI);
    }

    @Override
    public List<String> getReferencedColumns() throws SemanticException {
      MatchPath matchPath = (MatchPath) evaluator;
      List<String> columns = new ArrayList<>();
      for (ExprNodeDesc exprNode : matchPath.resultExprInfo.resultExprNodes) {
        Utilities.mergeUniqElems(columns, exprNode.getCols());
      }
      for (ExprNodeDesc exprNode : matchPath.symInfo.symbolExprsDecs) {
        Utilities.mergeUniqElems(columns, exprNode.getCols());
      }
      return columns;
    }
    
    /*
     * validate and setup patternStr
     */
    private void validateAndSetupPatternStr(MatchPath evaluator,
        List<PTFExpressionDef> args) throws SemanticException {
      PTFExpressionDef symboPatternArg = args.get(0);
      ObjectInspector symbolPatternArgOI = symboPatternArg.getOI();

      if ( !ObjectInspectorUtils.isConstantObjectInspector(symbolPatternArgOI) ||
          (symbolPatternArgOI.getCategory() != ObjectInspector.Category.PRIMITIVE) ||
          ((PrimitiveObjectInspector)symbolPatternArgOI).getPrimitiveCategory() !=
          PrimitiveObjectInspector.PrimitiveCategory.STRING )
      {
        throwErrorWithSignature("Currently the symbol Pattern must be a Constant String.");
      }

      evaluator.patternStr = ((ConstantObjectInspector)symbolPatternArgOI).
          getWritableConstantValue().toString();
    }

    /*
     * validate and setup SymbolInfo
     */
    private void validateAndSetupSymbolInfo(MatchPath evaluator,
        List<PTFExpressionDef> args,
        int argsNum) throws SemanticException {
      int symbolArgsSz = argsNum - 2;
      if ( symbolArgsSz % 2 != 0)
      {
        throwErrorWithSignature("Symbol Name, Expression need to be specified in pairs: " +
            "there are odd number of symbol args");
      }

      evaluator.symInfo = new SymbolsInfo(symbolArgsSz/2);
      for(int i=1; i <= symbolArgsSz; i += 2)
      {
        PTFExpressionDef symbolNameArg = args.get(i);
        ObjectInspector symbolNameArgOI = symbolNameArg.getOI();

        if ( !ObjectInspectorUtils.isConstantObjectInspector(symbolNameArgOI) ||
            (symbolNameArgOI.getCategory() != ObjectInspector.Category.PRIMITIVE) ||
            ((PrimitiveObjectInspector)symbolNameArgOI).getPrimitiveCategory() !=
            PrimitiveObjectInspector.PrimitiveCategory.STRING )
        {
          throwErrorWithSignature(
              String.format("Currently a Symbol Name(%s) must be a Constant String",
                  symbolNameArg.getExpressionTreeString()));
        }
        String symbolName = ((ConstantObjectInspector)symbolNameArgOI).
            getWritableConstantValue().toString();

        PTFExpressionDef symolExprArg = args.get(i+1);
        ObjectInspector symolExprArgOI = symolExprArg.getOI();
        if ( (symolExprArgOI.getCategory() != ObjectInspector.Category.PRIMITIVE) ||
              ((PrimitiveObjectInspector)symolExprArgOI).getPrimitiveCategory() !=
              PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN )
        {
          throwErrorWithSignature(String.format("Currently a Symbol Expression(%s) " +
              "must be a boolean expression", symolExprArg.getExpressionTreeString()));
        }
        evaluator.symInfo.add(symbolName, symolExprArg);
      }
    }

    /*
     * validate and setup resultExprStr
     */
    private void validateAndSetupResultExprStr(MatchPath evaluator,
        List<PTFExpressionDef> args,
        int argsNum) throws SemanticException {
      PTFExpressionDef resultExprArg = args.get(argsNum - 1);
      ObjectInspector resultExprArgOI = resultExprArg.getOI();

      if ( !ObjectInspectorUtils.isConstantObjectInspector(resultExprArgOI) ||
            (resultExprArgOI.getCategory() != ObjectInspector.Category.PRIMITIVE) ||
            ((PrimitiveObjectInspector)resultExprArgOI).getPrimitiveCategory() !=
            PrimitiveObjectInspector.PrimitiveCategory.STRING )
      {
        throwErrorWithSignature("Currently the result Expr parameter must be a Constant String.");
      }

      evaluator.resultExprStr = ((ConstantObjectInspector)resultExprArgOI).
          getWritableConstantValue().toString();
    }

    /*
     * setup SymbolFunction chain.
     */
    private void setupSymbolFunctionChain(MatchPath evaluator) throws SemanticException {
      SymbolParser syP = new SymbolParser(evaluator.patternStr,
          evaluator.symInfo.symbolExprsNames,
          evaluator.symInfo.symbolExprsEvaluators, evaluator.symInfo.symbolExprsOIs);
      syP.parse();
      evaluator.syFn = syP.getSymbolFunction();
    }

    @Override
    public boolean transformsRawInput()
    {
      return false;
    }

    @Override
    public void initializeOutputOI() throws HiveException {
      try {
        MatchPath evaluator = (MatchPath) getEvaluator();
        PartitionedTableFunctionDef tDef = evaluator.getTableDef();

        List<PTFExpressionDef> args = tDef.getArgs();
        int argsNum = args.size();

        validateAndSetupPatternStr(evaluator, args);
        validateAndSetupSymbolInfo(evaluator, args, argsNum);
        validateAndSetupResultExprStr(evaluator, args, argsNum);
        setupSymbolFunctionChain(evaluator);

        /*
         * setup OI for input to resultExpr select list
         */
        StructObjectInspector selectListInputOI = MatchPath.createSelectListOI( evaluator,
            tDef.getInput());
        ResultExprInfo resultExprInfo = evaluator.resultExprInfo;
        ArrayList<ObjectInspector> selectListExprOIs = new ArrayList<ObjectInspector>();
        resultExprInfo.resultExprEvals = new ArrayList<ExprNodeEvaluator>();

        for(int i=0 ; i < resultExprInfo.resultExprNodes.size(); i++) {
          ExprNodeDesc selectColumnExprNode =resultExprInfo.resultExprNodes.get(i);
          ExprNodeEvaluator selectColumnExprEval =
              ExprNodeEvaluatorFactory.get(selectColumnExprNode);
          ObjectInspector selectColumnOI = selectColumnExprEval.initialize(selectListInputOI);
          resultExprInfo.resultExprEvals.add(selectColumnExprEval);
          selectListExprOIs.add(selectColumnOI);
        }

        resultExprInfo.resultOI = ObjectInspectorFactory.getStandardStructObjectInspector(
            resultExprInfo.resultExprNames, selectListExprOIs);
        setOutputOI(resultExprInfo.resultOI);
      }
      catch(SemanticException se) {
        throw new HiveException(se);
      }
    }

    @Override
    public ArrayList<String> getOutputColumnNames() {
      MatchPath evaluator = (MatchPath) getEvaluator();
      return evaluator.resultExprInfo.getResultExprNames();
    }

  }

  public ResultExprInfo getResultExprInfo() {
    return resultExprInfo;
  }

  public void setResultExprInfo(ResultExprInfo resultExprInfo) {
    this.resultExprInfo = resultExprInfo;
  }

  static class SymbolsInfo {
    int sz;
    ArrayList<ExprNodeDesc> symbolExprsDecs;
    ArrayList<ExprNodeEvaluator> symbolExprsEvaluators;
    ArrayList<ObjectInspector> symbolExprsOIs;
    ArrayList<String> symbolExprsNames;

    SymbolsInfo(int sz)
    {
      this.sz = sz;
      symbolExprsEvaluators = new ArrayList<ExprNodeEvaluator>(sz);
      symbolExprsOIs = new ArrayList<ObjectInspector>(sz);
      symbolExprsNames = new ArrayList<String>(sz);
      symbolExprsDecs = new ArrayList<>(sz);
    }

    void add(String name, PTFExpressionDef arg)
    {
      symbolExprsNames.add(name);
      symbolExprsEvaluators.add(arg.getExprEvaluator());
      symbolExprsOIs.add(arg.getOI());
      symbolExprsDecs.add(arg.getExprNode());
    }
  }

  public static class ResultExprInfo {
    ArrayList<String> resultExprNames;
    ArrayList<ExprNodeDesc> resultExprNodes;
    private transient ArrayList<ExprNodeEvaluator> resultExprEvals;
    private transient StructObjectInspector resultOI;

    public ArrayList<String> getResultExprNames() {
      return resultExprNames;
    }
    public void setResultExprNames(ArrayList<String> resultExprNames) {
      this.resultExprNames = resultExprNames;
    }
    public ArrayList<ExprNodeDesc> getResultExprNodes() {
      return resultExprNodes;
    }
    public void setResultExprNodes(ArrayList<ExprNodeDesc> resultExprNodes) {
      this.resultExprNodes = resultExprNodes;
    }
  }

  public static abstract class SymbolFunction
  {
    SymbolFunctionResult result;

    public SymbolFunction()
    {
      result = new SymbolFunctionResult();
    }

    public static SymbolFunctionResult match(SymbolFunction syFn, Object row,
        PTFPartitionIterator<Object> pItr) throws HiveException
    {
      int resetToIdx = pItr.getIndex() - 1;
      try
      {
        return syFn.match(row, pItr);
      } finally
      {
        pItr.resetToIndex(resetToIdx);
      }
    }

    protected abstract SymbolFunctionResult match(Object row, PTFPartitionIterator<Object> pItr)
        throws HiveException;

    protected abstract boolean isOptional();
  }

  public static class Symbol extends SymbolFunction {
    ExprNodeEvaluator symbolExprEval;
    Converter converter;

    public Symbol(ExprNodeEvaluator symbolExprEval, ObjectInspector symbolOI)
    {
      this.symbolExprEval = symbolExprEval;
      converter = ObjectInspectorConverters.getConverter(
          symbolOI,
          PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);
    }

    @Override
    protected SymbolFunctionResult match(Object row, PTFPartitionIterator<Object> pItr)
        throws HiveException
    {
      Object val = null;
      val = symbolExprEval.evaluate(row);
      val = converter.convert(val);
      result.matches = ((Boolean) val).booleanValue();
      result.nextRow = pItr.getIndex();

      return result;
    }

    @Override
    protected boolean isOptional()
    {
      return false;
    }
  }

  public static class Star extends SymbolFunction {
    SymbolFunction symbolFn;

    public Star(SymbolFunction symbolFn)
    {
      this.symbolFn = symbolFn;
    }

    @Override
    protected SymbolFunctionResult match(Object row, PTFPartitionIterator<Object> pItr)
        throws HiveException
    {
      result.matches = true;
      SymbolFunctionResult rowResult = symbolFn.match(row, pItr);

      while (rowResult.matches && pItr.hasNext())
      {
        row = pItr.next();
        rowResult = symbolFn.match(row, pItr);
      }

      result.nextRow = pItr.getIndex();
      if(pItr.hasNext()) {
        result.nextRow -= 1;
      }
      return result;
    }

    @Override
    protected boolean isOptional()
    {
      return true;
    }
  }

  public static class Plus extends SymbolFunction {
    SymbolFunction symbolFn;

    public Plus(SymbolFunction symbolFn)
    {
      this.symbolFn = symbolFn;
    }

    @Override
    protected SymbolFunctionResult match(Object row, PTFPartitionIterator<Object> pItr)
        throws HiveException
    {
      SymbolFunctionResult rowResult = symbolFn.match(row, pItr);

      if (!rowResult.matches)
      {
        result.matches = false;
        result.nextRow = pItr.getIndex() - 1;
        return result;
      }

      result.matches = true;
      while (rowResult.matches && pItr.hasNext())
      {
        row = pItr.next();
        rowResult = symbolFn.match(row, pItr);
      }

      result.nextRow = pItr.getIndex() - 1;
      return result;
    }

    @Override
    protected boolean isOptional()
    {
      return false;
    }
  }

  public static class Chain extends SymbolFunction
  {
    ArrayList<SymbolFunction> components;

    public Chain(ArrayList<SymbolFunction> components)
    {
      this.components = components;
    }

    /*
     * Iterate over the Symbol Functions in the Chain:
     * - If we are not at the end of the Iterator (i.e. row != null )
     * - match the current componentFn
     * - if it returns false, then return false
     * - otherwise set row to the next row from the Iterator.
     * - if we are at the end of the Iterator
     * - skip any optional Symbol Fns (star patterns) at the end.
     * - but if we come to a non optional Symbol Fn, return false.
     * - if we match all Fns in the chain return true.
     */
    @Override
    protected SymbolFunctionResult match(Object row, PTFPartitionIterator<Object> pItr)
        throws HiveException
    {
      SymbolFunctionResult componentResult = null;
      for (SymbolFunction sFn : components)
      {
        if (row != null)
        {
          componentResult = sFn.match(row, pItr);
          if (!componentResult.matches)
          {
            result.matches = false;
            result.nextRow = componentResult.nextRow;
            return result;
          }
          row = pItr.resetToIndex(componentResult.nextRow);
        }
        else
        {
          if (!sFn.isOptional())
          {
            result.matches = false;
            result.nextRow = componentResult.nextRow;
            return result;
          }
        }
      }

      result.matches = true;
      result.nextRow = componentResult.nextRow;
      return result;
    }

    @Override
    protected boolean isOptional()
    {
      return false;
    }
  }


  public static class SymbolFunctionResult
  {
    /*
     * does the row match the pattern represented by this SymbolFunction
     */
    public boolean matches;
    /*
     * what is the index of the row beyond the set of rows that match this pattern.
     */
    public int nextRow;
  }

  public static class SymbolParser
  {
    String patternStr;
    String[] symbols;
    HashMap<String, Object[]> symbolExprEvalMap;
    ArrayList<SymbolFunction> symbolFunctions;
    Chain symbolFnChain;


    public SymbolParser(String patternStr, ArrayList<String> symbolNames,
        ArrayList<ExprNodeEvaluator> symbolExprEvals, ArrayList<ObjectInspector> symbolExprOIs)
    {
      super();
      this.patternStr = patternStr;
      symbolExprEvalMap = new HashMap<String, Object[]>();
      int sz = symbolNames.size();
      for(int i=0; i < sz; i++)
      {
        String symbolName = symbolNames.get(i);
        ExprNodeEvaluator symbolExprEval = symbolExprEvals.get(i);
        ObjectInspector symbolExprOI = symbolExprOIs.get(i);
        symbolExprEvalMap.put(symbolName.toLowerCase(),
            new Object[] {symbolExprEval, symbolExprOI});
      }
    }

    public SymbolFunction getSymbolFunction()
    {
      return symbolFnChain;
    }

    public void parse() throws SemanticException
    {
      symbols = patternStr.split("\\.");
      symbolFunctions = new ArrayList<SymbolFunction>();

      for(String symbol : symbols)
      {
        boolean isStar = symbol.endsWith("*");
        boolean isPlus = symbol.endsWith("+");

        symbol = (isStar || isPlus) ? symbol.substring(0, symbol.length() - 1) : symbol;
        Object[] symbolDetails = symbolExprEvalMap.get(symbol.toLowerCase());
        if ( symbolDetails == null )
        {
          throw new SemanticException(String.format("Unknown Symbol %s", symbol));
        }

        ExprNodeEvaluator symbolExprEval = (ExprNodeEvaluator) symbolDetails[0];
        ObjectInspector symbolExprOI = (ObjectInspector) symbolDetails[1];
        SymbolFunction sFn = new Symbol(symbolExprEval, symbolExprOI);

        if ( isStar )
        {
          sFn = new Star(sFn);
        }
        else if ( isPlus )
        {
          sFn = new Plus(sFn);
        }
        symbolFunctions.add(sFn);
      }
      symbolFnChain = new Chain(symbolFunctions);
    }
  }

  /*
   * ResultExpression is a Select List with the following variation:
   * - the select keyword is optional. The parser checks if the expression doesn't start with
   * select; if not it prefixes it.
   * - Window Fn clauses are not permitted.
   * - expressions can operate on the input columns plus the psuedo column 'path'
   * which is array of
   * structs. The shape of the struct is
   * the same as the input.
   */
  public static class ResultExpressionParser {
    String resultExprString;

    RowResolver selectListInputRowResolver;
    TypeCheckCtx selectListInputTypeCheckCtx;
    StructObjectInspector selectListInputOI;

    ArrayList<WindowExpressionSpec> selectSpec;

    ResultExprInfo resultExprInfo;

    public ResultExpressionParser(String resultExprString,
        RowResolver selectListInputRowResolver)
    {
      this.resultExprString = resultExprString;
      this.selectListInputRowResolver = selectListInputRowResolver;
    }

    public void translate() throws SemanticException, HiveException
    {
      setupSelectListInputInfo();
      fixResultExprString();
      parse();
      validateSelectExpr();
      buildSelectListEvaluators();
    }

    public ResultExprInfo getResultExprInfo() {
      return resultExprInfo;
    }

    private void buildSelectListEvaluators() throws SemanticException, HiveException
    {
      resultExprInfo = new ResultExprInfo();
      resultExprInfo.resultExprEvals = new ArrayList<ExprNodeEvaluator>();
      resultExprInfo.resultExprNames = new ArrayList<String>();
      resultExprInfo.resultExprNodes = new ArrayList<ExprNodeDesc>();
      //result
      ArrayList<ObjectInspector> selectListExprOIs = new ArrayList<ObjectInspector>();
      int i = 0;
      for(WindowExpressionSpec expr : selectSpec)
      {
        String selectColName = expr.getAlias();
        ASTNode selectColumnNode = expr.getExpression();
        ExprNodeDesc selectColumnExprNode =
            ResultExpressionParser.buildExprNode(selectColumnNode,
            selectListInputTypeCheckCtx);
        ExprNodeEvaluator selectColumnExprEval =
            ExprNodeEvaluatorFactory.get(selectColumnExprNode);
        ObjectInspector selectColumnOI = null;
        selectColumnOI = selectColumnExprEval.initialize(selectListInputOI);

        selectColName = getColumnName(selectColName, selectColumnExprNode, i);

        resultExprInfo.resultExprEvals.add(selectColumnExprEval);
        selectListExprOIs.add(selectColumnOI);
        resultExprInfo.resultExprNodes.add(selectColumnExprNode);
        resultExprInfo.resultExprNames.add(selectColName);
        i++;
      }

      resultExprInfo.resultOI = ObjectInspectorFactory.getStandardStructObjectInspector(
          resultExprInfo.resultExprNames, selectListExprOIs);
    }

    private void setupSelectListInputInfo() throws SemanticException
    {
      selectListInputTypeCheckCtx = new TypeCheckCtx(selectListInputRowResolver);
      selectListInputTypeCheckCtx.setUnparseTranslator(null);
      /*
       * create SelectListOI
       */
      selectListInputOI = PTFTranslator.getStandardStructOI(selectListInputRowResolver);
    }

    private void fixResultExprString()
    {
      String r = resultExprString.trim();
      if (r.length()<6 || !r.substring(0, 6).toLowerCase().equals("select"))
      {
        r = "select " + r;
      }
      resultExprString = r;
    }

    private void parse() throws SemanticException
    {
      selectSpec = SemanticAnalyzer.parseSelect(resultExprString);
    }

    private void validateSelectExpr() throws SemanticException
    {
      for (WindowExpressionSpec expr : selectSpec)
      {
        PTFTranslator.validateNoLeadLagInValueBoundarySpec(expr.getExpression());
      }
    }

    private String getColumnName(String alias, ExprNodeDesc exprNode, int colIdx)
    {
      if (alias != null)
      {
        return alias;
      }
      else if (exprNode instanceof ExprNodeColumnDesc)
      {
        ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) exprNode;
        return colDesc.getColumn();
      }
      return "matchpath_col_" + colIdx;
    }

    public static ExprNodeDesc buildExprNode(ASTNode expr,
        TypeCheckCtx typeCheckCtx) throws SemanticException
    {
      // todo: use SemanticAnalyzer::genExprNodeDesc
      // currently SA not available to PTFTranslator.
      Map<ASTNode, ExprNodeDesc> map = TypeCheckProcFactory
          .genExprNode(expr, typeCheckCtx);
      ExprNodeDesc desc = map.get(expr);
      if (desc == null) {
        String errMsg = typeCheckCtx.getError();
        if ( errMsg == null) {
          errMsg = "Error in parsing ";
        }
        throw new SemanticException(errMsg);
      }
      return desc;
    }
  }

  public static final String PATHATTR_NAME = "tpath";

  /*
   * add array<struct> to the list of columns
   */
  protected static RowResolver createSelectListRR(MatchPath evaluator,
      PTFInputDef inpDef) throws SemanticException {
    RowResolver rr = new RowResolver();
    RowResolver inputRR = inpDef.getOutputShape().getRr();

    evaluator.inputColumnNamesMap = new HashMap<String,String>();
    ArrayList<String> inputColumnNames = new ArrayList<String>();

    ArrayList<ObjectInspector> inpColOIs = new ArrayList<ObjectInspector>();

    for (ColumnInfo inpCInfo : inputRR.getColumnInfos()) {
      ColumnInfo cInfo = new ColumnInfo(inpCInfo);
      String colAlias = cInfo.getAlias();

      String[] tabColAlias = inputRR.reverseLookup(inpCInfo.getInternalName());
      if (tabColAlias != null) {
        colAlias = tabColAlias[1];
      }
      ASTNode inExpr = null;
      inExpr = PTFTranslator.getASTNode(inpCInfo, inputRR);
      if ( inExpr != null ) {
        rr.putExpression(inExpr, cInfo);
        colAlias = inExpr.toStringTree().toLowerCase();
      }
      else {
        colAlias = colAlias == null ? cInfo.getInternalName() : colAlias;
        rr.put(cInfo.getTabAlias(), colAlias, cInfo);
      }

      evaluator.inputColumnNamesMap.put(cInfo.getInternalName(), colAlias);
      inputColumnNames.add(colAlias);
      inpColOIs.add(cInfo.getObjectInspector());
    }

    StandardListObjectInspector pathAttrOI =
        ObjectInspectorFactory.getStandardListObjectInspector(
        ObjectInspectorFactory.getStandardStructObjectInspector(inputColumnNames,
            inpColOIs));

    ColumnInfo pathColumn = new ColumnInfo(PATHATTR_NAME,
        TypeInfoUtils.getTypeInfoFromObjectInspector(pathAttrOI),
        null,
        false, false);
    rr.put(null, PATHATTR_NAME, pathColumn);

    return rr;
  }

  protected static StructObjectInspector createSelectListOI(MatchPath evaluator, PTFInputDef inpDef) {
    StructObjectInspector inOI = inpDef.getOutputShape().getOI();
    ArrayList<String> inputColumnNames = new ArrayList<String>();
    ArrayList<String> selectListNames = new ArrayList<String>();
    ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
    for(StructField f : inOI.getAllStructFieldRefs()) {
      String inputColName = evaluator.inputColumnNamesMap.get(f.getFieldName());
      if ( inputColName != null ) {
        inputColumnNames.add(inputColName);
        selectListNames.add(f.getFieldName());
        fieldOIs.add(f.getFieldObjectInspector());
      }
    }

    StandardListObjectInspector pathAttrOI =
        ObjectInspectorFactory.getStandardListObjectInspector(
        ObjectInspectorFactory.getStandardStructObjectInspector(inputColumnNames,
            fieldOIs));

    ArrayList<ObjectInspector> selectFieldOIs = new ArrayList<ObjectInspector>();
    selectFieldOIs.addAll(fieldOIs);
    selectFieldOIs.add(pathAttrOI);
    selectListNames.add(MatchPath.PATHATTR_NAME);
    return ObjectInspectorFactory.getStandardStructObjectInspector(
        selectListNames, selectFieldOIs);
  }

  public static Object getSelectListInput(Object currRow, ObjectInspector rowOI,
      PTFPartitionIterator<Object> pItr, int sz)  throws HiveException {
    ArrayList<Object> oRow = new ArrayList<Object>();
    List<?> currRowAsStdObject = (List<?>) ObjectInspectorUtils
        .copyToStandardObject(currRow, rowOI);
    oRow.addAll(currRowAsStdObject);
    oRow.add(getPath(currRow, rowOI, pItr, sz));
    return oRow;
  }

  public static ArrayList<Object> getPath(Object currRow, ObjectInspector rowOI,
      PTFPartitionIterator<Object> pItr, int sz)  throws HiveException {
    int idx = pItr.getIndex() - 1;
    ArrayList<Object> path = new ArrayList<Object>();
    path.add(ObjectInspectorUtils.copyToStandardObject(currRow, rowOI));
    int pSz = 1;

    while (pSz < sz && pItr.hasNext())
    {
      currRow = pItr.next();
      path.add(ObjectInspectorUtils.copyToStandardObject(currRow, rowOI));
      pSz++;
    }
    pItr.resetToIndex(idx);
    return path;
  }
}
