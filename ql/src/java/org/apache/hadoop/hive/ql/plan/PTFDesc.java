package org.apache.hadoop.hive.ql.plan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.PTFUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.PTFSpec;
import org.apache.hadoop.hive.ql.parse.PTFSpec.ColumnSpec;
import org.apache.hadoop.hive.ql.parse.PTFSpec.Order;
import org.apache.hadoop.hive.ql.parse.PTFSpec.OrderColumnSpec;
import org.apache.hadoop.hive.ql.parse.PTFSpec.OrderSpec;
import org.apache.hadoop.hive.ql.parse.PTFSpec.PTFInputSpec;
import org.apache.hadoop.hive.ql.parse.PTFSpec.PTFTableOrSubQueryInputSpec;
import org.apache.hadoop.hive.ql.parse.PTFSpec.PartitionSpec;
import org.apache.hadoop.hive.ql.parse.PTFSpec.SelectSpec;
import org.apache.hadoop.hive.ql.parse.PTFSpec.TableFuncSpec;
import org.apache.hadoop.hive.ql.parse.PTFSpec.WindowFrameSpec;
import org.apache.hadoop.hive.ql.parse.PTFSpec.WindowFrameSpec.BoundarySpec;
import org.apache.hadoop.hive.ql.parse.PTFSpec.WindowFrameSpec.CurrentRowSpec;
import org.apache.hadoop.hive.ql.parse.PTFSpec.WindowFrameSpec.Direction;
import org.apache.hadoop.hive.ql.parse.PTFSpec.WindowFrameSpec.RangeBoundarySpec;
import org.apache.hadoop.hive.ql.parse.PTFSpec.WindowFrameSpec.ValueBoundarySpec;
import org.apache.hadoop.hive.ql.parse.PTFSpec.WindowFunctionSpec;
import org.apache.hadoop.hive.ql.parse.PTFSpec.WindowSpec;
import org.apache.hadoop.hive.ql.parse.PTFTranslator.PTFInputInfo;
import org.apache.hadoop.hive.ql.parse.PTFTranslator.PTFTranslationInfo;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.ptf.TableFunctionEvaluator;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

@Explain(displayName = "PTF Operator")
public class PTFDesc extends AbstractOperatorDesc
{

  private static final long serialVersionUID = 1L;
	@SuppressWarnings("unused")
	private static final Log LOG = LogFactory.getLog(PTFDesc.class.getName());
	PTFSpec spec;
	transient PTFTranslationInfo translationInfo;
	Map<String, WindowDef> windowDefs;
	SelectDef selectList;
	PTFInputDef input;
	WhereDef whereDef;
	static{
	  PTFUtils.makeTransient(PTFDesc.class, "translationInfo");
	}

	public PTFDesc()
        {

	}

	public PTFSpec getSpec()
	{
		return spec;
	}

	public void setSpec(PTFSpec spec)
	{
		this.spec = spec;
	}

	public PTFTranslationInfo getTranslationInfo()
	{
		return translationInfo;
	}

	public void setTranslationInfo(PTFTranslationInfo translationInfo)
	{
		this.translationInfo = translationInfo;
	}

	public Map<String, WindowDef> getWindowDefs()
	{
		return windowDefs;
	}

	public void setWindowDefs(Map<String, WindowDef> windowDefs)
	{
		this.windowDefs = windowDefs;
	}

	public SelectDef getSelectList()
	{
		selectList = selectList == null ? new SelectDef() : selectList;
		return selectList;
	}

	public void setSelectList(SelectDef selectList)
	{
		this.selectList = selectList;
	}

	public PTFInputDef getInput()
	{
		return input;
	}
	public void setInput(PTFInputDef input)
	{
		this.input = input;
	}

	public WhereDef getWhere()
	{
		return whereDef;
	}

	public void setWhere(WhereDef whereDef)
	{
		this.whereDef = whereDef;
	}

	/*
	 * get the Hive Table associated with this input chain.
	 */
	public PTFInputSpec getHiveTableSpec()
	{
	  return getInput().getHiveInputSpec();
	}

	public PTFInputDef getHiveTableDef()
	{
	  return getInput().getHiveInputDef();
	}

	/*
	 * QueryInputDef class
	 */
	public static abstract class PTFInputDef
	{
	  PTFInputSpec inputSpec;
	  WindowDef window;
	  transient StructObjectInspector OI;
	  transient SerDe serde;
	  /*
	   * The inputInfo is provided so that the setupOI call use the input RowResolver & TypeCheckCtx.
	   * This is not needed normally. The Args to a TableFunction are already translated when the setupOI call is made.
	   * But in the case of dynamic SQL: such as the ResultExpression in NPath, where an argument is parsed as a Select List
	   * the external names(aliases) of columns are needed to translate user specified columns to internal names.
	   */
	  transient PTFInputInfo inputInfo;
	  String alias;

	  static{
	    PTFUtils.makeTransient(PTFInputDef.class, "serde");
	    PTFUtils.makeTransient(PTFInputDef.class, "OI");
	    PTFUtils.makeTransient(PTFInputDef.class, "inputInfo");
	  }


	  public PTFInputDef(){

	  }

	  public PTFInputSpec getSpec()
	  {
	    return inputSpec;
	  }

	  public void setSpec(PTFInputSpec inputSpec)
	  {
	    this.inputSpec = inputSpec;
	  }

	  public WindowDef getWindow()
	  {
	    return window;
	  }

	  public void setWindow(WindowDef window)
	  {
	    this.window = window;
	  }

	  public StructObjectInspector getOI()
	  {
	    return OI;
	  }

	  public void setOI(StructObjectInspector oI)
	  {
	    OI = oI;
	  }

	  public PTFInputInfo getInputInfo() {
      return inputInfo;
    }

    public void setInputInfo(PTFInputInfo inputInfo) {
      this.inputInfo = inputInfo;
    }

    public SerDe getSerde()
	  {
	    return serde;
	  }

	  public void setSerde(SerDe serde)
	  {
	    this.serde = serde;
	  }

	  public String getAlias()
	  {
	    return alias;
	  }

	  public void setAlias(String alias)
	  {
	    this.alias = alias;
	  }

	  /*
	   * get the Hive Table associated with this input chain.
	   */
	  public abstract PTFInputSpec getHiveInputSpec();

	  public abstract PTFInputDef getHiveInputDef();

	  public PTFInputSpec getInputSpec() {
	    return inputSpec;
	  }

	  public void setInputSpec(PTFInputSpec inputSpec) {
	    this.inputSpec = inputSpec;
	  }
	}

  /*
  * PTF Component Query Def
  */
  public static class PTFComponentQueryDef extends PTFInputDef
  {

   String compSerdeClassName;

   public String getCompSerdeClassName() {
      return compSerdeClassName;
    }

    public void setCompSerdeClassName(String compSerdeClassName) {
      this.compSerdeClassName = compSerdeClassName;
    }

      @Override
    public PTFInputSpec getHiveInputSpec() {
      return this.getSpec();
    }

    @Override
    public PTFInputDef getHiveInputDef() {
      return this;
    }
  }

	/*
	 * HiveTableDef
	 */
	public static class PTFTableOrSubQueryInputDef extends PTFInputDef
	{
	  String tableSerdeClassName;
	  Map<String, String> tableSerdeProps;

	  String location;
	  String inputFormatClassName;

	  @Override
	  public PTFTableOrSubQueryInputSpec getHiveInputSpec()
	  {
	    return (PTFTableOrSubQueryInputSpec) inputSpec;
	  }

	  public String getTableSerdeClassName()
	  {
	    return tableSerdeClassName;
	  }

	  public void setTableSerdeClassName(String tableSerdeClassName)
	  {
	    this.tableSerdeClassName = tableSerdeClassName;
	  }

	  public Map<String, String> getTableSerdeProps()
	  {
	    return tableSerdeProps;
	  }

	  public void setTableSerdeProps(Map<String, String> tableSerdeProps)
	  {
	    this.tableSerdeProps = tableSerdeProps;
	  }

	  public String getLocation()
	  {
	    return location;
	  }

	  public void setLocation(String location)
	  {
	    this.location = location;
	  }

	  public String getInputFormatClassName()
	  {
	    return inputFormatClassName;
	  }

	  public void setInputFormatClassName(String inputFormatClassName)
	  {
	    this.inputFormatClassName = inputFormatClassName;
	  }

	  @Override
	  public PTFTableOrSubQueryInputDef getHiveInputDef() {
	    return this;
	  }

	}


	/*
	 * TableFuncDef
	 */
	public static class TableFuncDef extends PTFInputDef
	{
	  ArrayList<ArgDef> args;
	  PTFInputDef input;
	  ArrayList<String> outputColumnNames;
	  /*
	   * set during translation, based on value provided by PTFResolver.
	   * This is used during translation to decide if the internalName -> alias mapping from the Input to the PTF is carried
	   * forward when building the Output RR for this PTF.
	   * See {@link PTFResolver#carryForwardNames()} for details.
	   */
	  boolean carryForwardNames;
	  TableFunctionEvaluator tFunction;
	  ArrayList<String> rawInputColumnNames;
	  transient ObjectInspector rawInputOI;
	  transient PTFInputInfo rawInputInfo;

	  /*
	   * @remove
	   * used for validating that OI columnNames are the same during runtime.
	   */
	  HashMap<String, String> rawInputSerdeProps;
	  /*
     * @remove
     * used for validating that OI columnNames are the same during runtime.
     */
	  HashMap<String, String> outputSerdeProps;

	  static{
	    PTFUtils.makeTransient(TableFuncDef.class, "rawInputOI");
	    PTFUtils.makeTransient(TableFuncDef.class, "rawInputInfo");
	  }


	  public TableFuncSpec getTableFuncSpec()
	  {
	    return (TableFuncSpec) inputSpec;
	  }

	  public ArrayList<ArgDef> getArgs()
	  {
	    return args;
	  }

	  public void setArgs(ArrayList<ArgDef> args)
	  {
	    this.args = args;
	  }

	  public void addArg(ArgDef arg)
	  {
	    args = args == null ? new ArrayList<ArgDef>() : args;
	    args.add(arg);
	  }

	  public PTFInputDef getInput()
	  {
	    return input;
	  }

	  public void setInput(PTFInputDef input)
	  {
	    this.input = input;
	  }

	  public String getName()
	  {
	    return getTableFuncSpec().getName();
	  }

	  public TableFunctionEvaluator getFunction()
	  {
	    return tFunction;
	  }

	  public void setFunction(TableFunctionEvaluator tFunction)
	  {
	    this.tFunction = tFunction;
	  }

	  @Override
	  public PTFInputSpec getHiveInputSpec()
	  {
	    return input.getHiveInputSpec();
	  }
    public ObjectInspector getRawInputOI() {
      return rawInputOI;
    }

    public void setRawInputOI(ObjectInspector rawInputOI) {
      this.rawInputOI = rawInputOI;
    }

    public PTFInputInfo getRawInputInfo() {
      return rawInputInfo;
    }

    public void setRawInputInfo(PTFInputInfo rawInputInfo) {
      this.rawInputInfo = rawInputInfo;
    }

    @Override
    public PTFInputDef getHiveInputDef() {
      return input.getHiveInputDef();
	  }

    public ArrayList<String> getOutputColumnNames() {
      return outputColumnNames;
    }

    public void setOutputColumnNames(ArrayList<String> outputColumnNames) {
      this.outputColumnNames = outputColumnNames;
    }

    public ArrayList<String> getRawInputColumnNames() {
      return rawInputColumnNames;
    }

    public void setRawInputColumnNames(ArrayList<String> rawInputColumnNames) {
      this.rawInputColumnNames = rawInputColumnNames;
    }

    public boolean isCarryForwardNames() {
      return carryForwardNames;
    }

    public void setCarryForwardNames(boolean carryForwardNames) {
      this.carryForwardNames = carryForwardNames;
    }

    public HashMap<String, String> getRawInputSerdeProps() {
      return rawInputSerdeProps;
    }

    public void setRawInputSerdeProps(HashMap<String, String> rawInputSerdeProps) {
      this.rawInputSerdeProps = rawInputSerdeProps;
    }

    public HashMap<String, String> getOutputSerdeProps() {
      return outputSerdeProps;
    }

    public void setOutputSerdeProps(HashMap<String, String> outputSerdeProps) {
      this.outputSerdeProps = outputSerdeProps;
    }
	}

	/*
	 * WindowFuncDef class
	 */
	public static class WindowFunctionDef
	{
	  WindowFunctionSpec wSpec;
	  ArrayList<ArgDef> args;
	  WindowDef window;
	  transient GenericUDAFEvaluator wFnEval;
	  transient ObjectInspector OI;

	  static{
	    PTFUtils.makeTransient(WindowFunctionDef.class, "wFnEval");
	    PTFUtils.makeTransient(WindowFunctionDef.class, "OI");
	  }


	  public WindowFunctionDef(){

	  }

	  public WindowFunctionSpec getSpec()
	  {
	    return wSpec;
	  }

	  public void setSpec(WindowFunctionSpec wSpec)
	  {
	    this.wSpec = wSpec;
	  }

	  public ArrayList<ArgDef> getArgs()
	  {
	    return args;
	  }

	  public void setArgs(ArrayList<ArgDef> args)
	  {
	    this.args = args;
	  }

	  public void addArg(ArgDef arg)
	  {
	    args = args == null ? new ArrayList<ArgDef>() : args;
	    args.add(arg);
	  }

	  public WindowDef getWindow()
	  {
	    return window;
	  }

	  public void setWindow(WindowDef window)
	  {
	    this.window = window;
	  }

	  public GenericUDAFEvaluator getEvaluator()
	  {
	    return wFnEval;
	  }

	  public void setEvaluator(GenericUDAFEvaluator wFnEval)
	  {
	    this.wFnEval = wFnEval;
	  }

	  public ObjectInspector getOI()
	  {
	    return OI;
	  }

	  public void setOI(ObjectInspector oI)
	  {
	    OI = oI;
	  }

	}

	/**
	 * represents and argument of a Window or table Function.
	 *
	 */
	public static class ArgDef
	{
	  ASTNode expression;
	  ExprNodeDesc exprNode;
	  transient ExprNodeEvaluator exprEvaluator;
	  transient ObjectInspector OI;

	  static{
	    PTFUtils.makeTransient(ArgDef.class, "exprEvaluator");
	    PTFUtils.makeTransient(ArgDef.class, "OI");
	  }


	  public ArgDef(){

	  }

	  public ASTNode getExpression()
	  {
	    return expression;
	  }

	  public void setExpression(ASTNode expression)
	  {
	    this.expression = expression;
	  }

	  public ExprNodeDesc getExprNode()
	  {
	    return exprNode;
	  }

	  public void setExprNode(ExprNodeDesc exprNode)
	  {
	    this.exprNode = exprNode;
	  }

	  public ExprNodeEvaluator getExprEvaluator()
	  {
	    return exprEvaluator;
	  }

	  public void setExprEvaluator(ExprNodeEvaluator exprEvaluator)
	  {
	    this.exprEvaluator = exprEvaluator;
	  }

	  public ObjectInspector getOI()
	  {
	    return OI;
	  }

	  public void setOI(ObjectInspector oI)
	  {
	    OI = oI;
	  }

	}

	/*
	 * Represents either a (Partition, Order) definition or a QueryInput or represents a WindowSpec for a WindowFunc.
	 * <ul>
	 * <li> <code>sourceWSpec</code> will be null when this represents a (Partition, Order) definition or a QueryInput
	 * <li> If the <code>sourceWSpec</code> has a <code>sourceId</code> then the unspecified elements in the WindowSpec are applied from the referenced WIndowSpec.
	 * </ul>
	 */
	public static class WindowDef
	{
	  WindowSpec spec;
	  PartitionDef partDef;
	  OrderDef orderDef;
	  WindowFrameDef window;

	  public WindowDef(){

	  }

	  public WindowDef(WindowSpec spec)
	  {
	    this.spec = spec;
	  }

	  public WindowDef(WindowSpec spec, WindowDef srcWdwDef)
	  {
	    this(spec);
	    partDef = srcWdwDef.partDef;
	    orderDef = srcWdwDef.orderDef;
	    window = srcWdwDef.window;
	  }

	  public WindowSpec getsSpec()
	  {
	    return spec;
	  }

	  public PartitionDef getPartDef()
	  {
	    return partDef;
	  }

	  public void setPartDef(PartitionDef partDef)
	  {
	    this.partDef = partDef;
	  }

	  public OrderDef getOrderDef()
	  {
	    return orderDef;
	  }

	  public void setOrderDef(OrderDef orderDef)
	  {
	    this.orderDef = orderDef;
	  }

	  public WindowFrameDef getWindow()
	  {
	    return window;
	  }

	  public void setWindow(WindowFrameDef window)
	  {
	    this.window = window;
	  }

	}

	/*
	 * PartitionDef class
	 */
	public static class PartitionDef
	{
	  PartitionSpec spec;
	  ArrayList<ColumnDef> columns;

	  public PartitionDef(){

	  }

	  public PartitionDef(PartitionSpec spec)
	  {
	    this.spec = spec;
	  }

	  public PartitionSpec getSpec()
	  {
	    return spec;
	  }

	  public ArrayList<ColumnDef> getColumns()
	  {
	    return columns;
	  }

	  public void setColumns(ArrayList<ColumnDef> columns)
	  {
	    this.columns = columns;
	  }

	  public void addColumn(ColumnDef c)
	  {
	    columns = columns == null ? new ArrayList<ColumnDef>() : columns;
	    columns.add(c);
	  }

	}

	/*
	 * OrderDef class
	 */
	public static class OrderDef
	{
	  OrderSpec spec;
	  ArrayList<OrderColumnDef> columns;

	  public OrderDef(){

	  }

	  public OrderDef(OrderSpec spec)
	  {
	    this.spec = spec;
	  }

	  public OrderDef(PartitionDef pDef)
	  {
	    this.spec = new OrderSpec(pDef.getSpec());
	    for(ColumnDef cDef : pDef.getColumns())
	    {
	      addColumn(new OrderColumnDef(cDef));
	    }
	  }

	  public OrderSpec getSpec()
	  {
	    return spec;
	  }

	  public ArrayList<OrderColumnDef> getColumns()
	  {
	    return columns;
	  }

	  public void setColumns(ArrayList<OrderColumnDef> columns)
	  {
	    this.columns = columns;
	  }

	  public void addColumn(OrderColumnDef c)
	  {
	    columns = columns == null ? new ArrayList<OrderColumnDef>() : columns;
	    columns.add(c);
	  }
	}

	/*
	 * represents a Column reference in a Partition or Order Spec or an Expr in the SelectSpec.
	 */
	public static class ColumnDef
	{
	  /*
	   * non-null if this is a Column reference in a Partition or Order Spec.
	   */
	  ColumnSpec spec;

	  String alias;

	  ASTNode expression;
	  ExprNodeDesc exprNode;
	  transient ExprNodeEvaluator exprEvaluator;
	  transient ObjectInspector OI;

	  static{
	    PTFUtils.makeTransient(ColumnDef.class, "exprEvaluator");
	    PTFUtils.makeTransient(ColumnDef.class, "OI");
	  }

	  public ColumnDef(){

	  }

	  public ColumnDef(ColumnSpec spec)
	  {
	    this.spec = spec;
	  }

	  public ColumnDef(ColumnDef cDef)
	  {
	    spec = cDef.getSpec();
	    alias = cDef.getAlias();
	    expression = cDef.getExpression();
	    exprNode = cDef.getExprNode();
	    exprEvaluator = cDef.getExprEvaluator();
	    OI = cDef.getOI();
	  }

	  public void setSpec(ColumnSpec spec) {
	    this.spec = spec;
	  }

	  public ColumnSpec getSpec()
	  {
	    return spec;
	  }

	  public String getAlias()
	  {
	    return alias;
	  }

	  public void setAlias(String alias)
	  {
	    this.alias = alias;
	  }

	  public ASTNode getExpression()
	  {
	    return expression;
	  }

	  public void setExpression(ASTNode expression)
	  {
	    this.expression = expression;
	  }

	  public ExprNodeDesc getExprNode()
	  {
	    return exprNode;
	  }

	  public void setExprNode(ExprNodeDesc exprNode)
	  {
	    this.exprNode = exprNode;
	  }

	  public ObjectInspector getOI()
	  {
	    return OI;
	  }

	  public void setOI(ObjectInspector oI)
	  {
	    OI = oI;
	  }

	  public ExprNodeEvaluator getExprEvaluator()
	  {
	    return exprEvaluator;
	  }

	  public void setExprEvaluator(ExprNodeEvaluator exprEvaluator)
	  {
	    this.exprEvaluator = exprEvaluator;
	  }

	}

	/*
	 * OrderColumnDef class
	 */
	public static class OrderColumnDef extends ColumnDef
	{

	  public OrderColumnDef(){

	  }

	  public OrderColumnDef(OrderColumnSpec spec)
	  {
	    super(spec);
	  }

	  public OrderColumnDef(ColumnDef cDef)
	  {
	    super(cDef);
	    this.spec = new OrderColumnSpec(spec);
	  }

	  public Order getOrder()
	  {
	    return ((OrderColumnSpec)getSpec()).getOrder();
	  }

	}

	/*
	 * Window Frame classes
	 */
	public static class WindowFrameDef
	{
	  WindowFrameSpec spec;
	  BoundaryDef start;
	  BoundaryDef end;

	  public WindowFrameDef(){

	  }

	  public WindowFrameDef(WindowFrameSpec spec) { this.spec = spec; }

	  public BoundaryDef getStart()
	  {
	    return start;
	  }

	  public void setStart(BoundaryDef start)
	  {
	    this.start = start;
	  }

	  public BoundaryDef getEnd()
	  {
	    return end;
	  }

	  public void setEnd(BoundaryDef end)
	  {
	    this.end = end;
	  }

	  public abstract static class BoundaryDef implements Comparable<BoundaryDef>
	  {
	    BoundarySpec spec;

	    public BoundaryDef(){}

	    public BoundarySpec getSpec() {
	      return spec;
	    }

	    public void setSpec(BoundarySpec spec) {
	      this.spec = spec;
	    }

	    public BoundaryDef(BoundarySpec spec)
	    {
	      this.spec = spec;
	    }

	    public Direction getDirection() { return spec.getDirection(); }
	  }

	  public static class RangeBoundaryDef extends BoundaryDef
	  {
	    public RangeBoundaryDef(){}

	    public RangeBoundaryDef(RangeBoundarySpec spec) { super(spec);}

	    public int getAmt()
	    {
	      return ((RangeBoundarySpec)spec).getAmt();
	    }

	    public int compareTo(BoundaryDef other)
	    {
	      int c = getDirection().compareTo(other.getDirection());
	      if ( c != 0) {
	        return c;
	      }
	      RangeBoundaryDef rb = (RangeBoundaryDef) other;
	      return getAmt() - rb.getAmt();
	    }
	  }

	  public static class CurrentRowDef extends BoundaryDef
	  {

	    public CurrentRowDef(){}

	    public CurrentRowDef(CurrentRowSpec spec) { super(spec);}

	    public int compareTo(BoundaryDef other)
	    {
	      return getDirection().compareTo(other.getDirection());
	    }
	  }

	  public static class ValueBoundaryDef extends BoundaryDef
	  {
	    ExprNodeDesc exprNode;
	    transient ExprNodeEvaluator exprEvaluator;
	    transient ObjectInspector OI;

	    static{
	      PTFUtils.makeTransient(ValueBoundaryDef.class, "exprEvaluator");
	      PTFUtils.makeTransient(ValueBoundaryDef.class, "OI");
	    }

	    public ValueBoundaryDef(){ }

	    public ValueBoundaryDef(ValueBoundarySpec spec) { super(spec);}


	    public int getAmt()
	    {
	      return ((ValueBoundarySpec)spec).getAmt();
	    }

	    public ExprNodeDesc getExprNode()
	    {
	      return exprNode;
	    }

	    public void setExprNode(ExprNodeDesc exprNode)
	    {
	      this.exprNode = exprNode;
	    }

	    public ExprNodeEvaluator getExprEvaluator()
	    {
	      return exprEvaluator;
	    }


	    public void setExprEvaluator(ExprNodeEvaluator exprEvaluator)
	    {
	      this.exprEvaluator = exprEvaluator;
	    }

	    public ObjectInspector getOI()
	    {
	      return OI;
	    }


	    public void setOI(ObjectInspector oI)
	    {
	      OI = oI;
	    }

	    public int compareTo(BoundaryDef other)
	    {
	      int c = getDirection().compareTo(other.getDirection());
	      if ( c != 0) {
	        return c;
	      }
	      ValueBoundaryDef vb = (ValueBoundaryDef) other;
	      return getAmt() - vb.getAmt();
	    }
	  }

	}

	/*
	 * SelectDef class
	 */
	public static class SelectDef
	{
	  SelectSpec selectSpec;
	  ArrayList<WindowFunctionDef> windowFuncs;
	  ArrayList<ColumnDef> columns;
	  transient StructObjectInspector OI;
	  transient RowResolver rowResolver;

	  static{
	    PTFUtils.makeTransient(SelectDef.class, "OI");
	    PTFUtils.makeTransient(SelectDef.class, "rowResolver");
	  }


	  public SelectDef(){

	  }

	  public SelectSpec getSelectSpec()
	  {
	    return selectSpec;
	  }

	  public void setSelectSpec(SelectSpec selectSpec)
	  {
	    this.selectSpec = selectSpec;
	  }

	  public ArrayList<WindowFunctionDef> getWindowFuncs()
	  {
	    return windowFuncs;
	  }

	  public void setWindowFuncs(ArrayList<WindowFunctionDef> windowFuncs)
	  {
	    this.windowFuncs = windowFuncs;
	  }

	  public ArrayList<ColumnDef> getColumns()
	  {
	    return columns;
	  }

	  public void setColumns(ArrayList<ColumnDef> columns)
	  {
	    this.columns = columns;
	  }

	  public void addColumn(ColumnDef cDef)
	  {
	    columns = columns == null ? new ArrayList<ColumnDef>() : columns;
	    columns.add(cDef);
	  }

	  public StructObjectInspector getOI()
	  {
	    return OI;
	  }

	  public void setOI(StructObjectInspector oI)
	  {
	    OI = oI;
	  }

	  public RowResolver getRowResolver() {
	    return rowResolver;
	  }

	  public void setRowResolver(RowResolver rowResolver) {
	    this.rowResolver = rowResolver;
	  }
	}

	public static class WhereDef
	{
	  ASTNode expression;
	  ExprNodeDesc exprNode;
	  transient ExprNodeEvaluator exprEvaluator;
	  transient ObjectInspector OI;

	  static{
	    PTFUtils.makeTransient(WhereDef.class, "exprEvaluator");
	    PTFUtils.makeTransient(WhereDef.class, "OI");
	  }

	  public WhereDef(){}

	  public ASTNode getExpression()
	  {
	    return expression;
	  }

	  public void setExpression(ASTNode expression)
	  {
	    this.expression = expression;
	  }

	  public ExprNodeDesc getExprNode()
	  {
	    return exprNode;
	  }

	  public void setExprNode(ExprNodeDesc exprNode)
	  {
	    this.exprNode = exprNode;
	  }

	  public ExprNodeEvaluator getExprEvaluator()
	  {
	    return exprEvaluator;
	  }

	  public void setExprEvaluator(ExprNodeEvaluator exprEvaluator)
	  {
	    this.exprEvaluator = exprEvaluator;
	  }

	  public ObjectInspector getOI()
	  {
	    return OI;
	  }

	  public void setOI(ObjectInspector oI)
	  {
	    OI = oI;
	  }
	}

}
