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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.PTFUtils;

public class PTFSpec
{
	PTFInputSpec input;
	SelectSpec selectList;
	ASTNode whereExpr;
	Map<String, WindowSpec> windowSpecs;

	public PTFInputSpec getInput()
	{
		return input;
	}
	public void setInput(PTFInputSpec input)
	{
		this.input = input;
	}
	public ASTNode getWhereExpr()
	{
		return whereExpr;
	}
	public void setWhereExpr(CommonTree whereExpr)
	{
		this.whereExpr = (ASTNode) whereExpr;
	}
	public Map<String, WindowSpec> getWindowSpecs()
	{
		windowSpecs = windowSpecs == null ? new HashMap<String, WindowSpec>() : windowSpecs;
		return windowSpecs;
	}
	public void setWindowSpecs(Map<String, WindowSpec> windowSpecs)
	{
		this.windowSpecs = windowSpecs;
	}

	public void addWindowSpec(String name, WindowSpec wdwSpec)
	{
		windowSpecs = windowSpecs == null ? new HashMap<String, WindowSpec>() : windowSpecs;
		windowSpecs.put(name, wdwSpec);
	}

	public WindowSpec getWindowSpec(String name)
	{
		return windowSpecs == null ? null : windowSpecs.get(name);
	}

	public SelectSpec getSelectList()
	{
		return selectList;
	}
	public void setSelectList(SelectSpec selectList)
	{
		this.selectList = selectList;
	}


  public static String NL = System.getProperty("line.separator");
	@Override
  public String toString()
	{
		StringBuilder buf = new StringBuilder();

		buf.append(selectList).append(NL);
		buf.append("from ").append(input).append(NL);
		if ( whereExpr != null )
		{
			buf.append("where ").append(whereExpr.toStringTree()).append(NL);
		}

		if ( windowSpecs != null)
		{
			boolean first = true;
			buf.append("window ");
			for(Map.Entry<String, WindowSpec> wentry : windowSpecs.entrySet())
			{
				if ( first ) {
          first = false;
        } else {
          buf.append(",");
        }
				buf.append(NL).append("  ").append(wentry.getKey()).append(" as ").append(wentry.getValue());
			}
			buf.append(NL);
		}

		return buf.toString();
	}

	/*
	 * QueryInputSpec class
	 */
	public static abstract class PTFInputSpec
	{
	  ASTNode astNode;
	  PartitionSpec partition;
	  OrderSpec order;

	  public PartitionSpec getPartition()
	  {
	    return partition;
	  }
	  public void setPartition(PartitionSpec partition)
	  {
	    this.partition = partition;
	  }
	  public OrderSpec getOrder()
	  {
	    return order;
	  }
	  public void setOrder(OrderSpec order)
	  {
	    this.order = order;
	  }

	  public ASTNode getAstNode() {
	    return astNode;
	  }
	  public void setAstNode(ASTNode astNode) {
	    this.astNode = astNode;
	  }
	  /*
	   * is the starting point of this Input Chain a Hive Query or Table.
	   */
	  public abstract boolean sourcedFromHive();
	}

	public static enum Order
	{
	  ASC,
	  DESC;
	}


	/*
	 * HiveTableSpec class
	 */
	public static class PTFTableOrSubQueryInputSpec extends PTFInputSpec
	{
	  String dbName;
	  String tableName;

	  public PTFTableOrSubQueryInputSpec() {}

	  public PTFTableOrSubQueryInputSpec(String dbName, String tableName)
	  {
	    super();
	    this.dbName = dbName;
	    this.tableName = tableName;
	  }

	  public String getDbName()
	  {
	    return dbName;
	  }

	  public void setDbName(String dbName)
	  {
	    this.dbName = dbName;
	  }

	  public String getTableName()
	  {
	    return tableName;
	  }

	  public void setTableName(String tableName)
	  {
	    this.tableName = tableName;
	  }

	  @Override
    public boolean sourcedFromHive()
	  {
	    return true;
	  }

	  @Override
    public String toString()
	  {
	    StringBuilder buf = new StringBuilder();

	    if ( dbName != null) {
        buf.append(dbName).append(".");
      }
	    buf.append(tableName);

	    if ( partition != null )
	    {
	      buf.append(" ").append(partition);
	      if ( order != null ) {
          buf.append(" ").append(order);
        }
	    }

	    return buf.toString();
	  }

	}


  /**
   * To support multi-PTF operator chain
   * PTFComponentQuerySpec.
   *
   */
  public static class PTFComponentQuerySpec extends PTFInputSpec
  {
    @Override
    public boolean sourcedFromHive() {
      return true;
    }

    @Override
    public String toString()
    {
      return "PTFOp up the chain...";
    }
  }

	/*
	 * TableFunc Spec
	 */
	public static class TableFuncSpec extends PTFInputSpec
	{
	  String name;
	  String alias;
	  ArrayList<ASTNode> args;
	  PTFInputSpec input;

	  /*
	   * This is only set for WindowingTableFunction.
	   * We allow for the Partitioning Spec to be inferred from a WindowingFunction or Window Clause as a convenience.
	   * This is so that the language is compliant with standard SQL. But at runtime we only allow all functions to have the
	   * same Partitioning Spec. So as a further convenience functions w/o a Partitioning Spec can inherit this from the default.
	   * The rules for setting the details are the following:
	   * 1. On encountering the first UDAF with a WindowingSpec we set the WindowingTablFunc's default Window Spec to this.
	   * We also set the Table Func's Partition & Order spec based on the WindowSpec.
	   * 2. On encountering a Distribute/Cluster clause we set the WindowingTablFunc's Partition spec. We clear the default Window Spec.
	   * There is no default in this case; the Partitioning spec is being specified explicitly.
	   * 3. On encountering a Sort clause:
	   *   a. we set the WindowTableFunc's OrderSpec.
	   *   b. If the default WindowSpec is not null & it has no OrderSpec, we set its OrderSpec.
     * 4. On encountering the first Window Clause, if the WindowingTablFunc has no PartitionSpec we set it from the Window Clause.
     * If the TblFunc has no OrderSpec, we set it from the Window Clause.
     * If the TblFunc has a OrderSpec and the Window Clause doesn't then we set the OrderSpec on the Window Clause.
	   */
	  WindowSpec defaultWindowSpec;

	  public TableFuncSpec() {}
	  public TableFuncSpec(String name)
	  {
	    super();
	    this.name = name;
	  }

	  public String getName()
	  {
	    return name;
	  }

	  public void setName(String name)
	  {
	    this.name = name;
	  }

	  public String getAlias() {
	    return alias;
	  }

	  public void setAlias(String alias) {
	    this.alias = alias;
	  }

	  public ArrayList<ASTNode> getArgs()
	  {
	    return args;
	  }

	  public void setArgs(ArrayList<ASTNode> args)
	  {
	    this.args = args;
	  }

	  public void addArg(CommonTree arg)
	  {
	    args = args == null ? new ArrayList<ASTNode>() : args;
	    args.add((ASTNode)arg);
	  }

	  public PTFInputSpec getInput()
	  {
	    return input;
	  }
	  public void setInput(PTFInputSpec input)
	  {
	    this.input = input;
	  }

	  protected WindowSpec getDefaultWindowSpec() {
      return defaultWindowSpec;
    }
    protected void setDefaultWindowSpec(WindowSpec defaultWindowSpec) {
      this.defaultWindowSpec = defaultWindowSpec;
    }

    public void inferDefaultWindowingSpec(WindowFunctionSpec wFnSpec) {
      if ( !getName().equals(FunctionRegistry.WINDOWING_TABLE_FUNCTION)) {
        return;
      }

      WindowSpec wSpec = wFnSpec == null ? null : wFnSpec.getWindowSpec();
      if ( getDefaultWindowSpec() == null && wSpec != null && wSpec.getPartition() != null ) {
        setDefaultWindowSpec(wSpec);
        setPartition(wSpec.getPartition());
        setOrder(wSpec.getOrder());
      }
    }

    public void inferDefaultWindowingSpec(WindowSpec wSpec) {
      if ( !getName().equals(FunctionRegistry.WINDOWING_TABLE_FUNCTION)) {
        return;
      }

      if ( wSpec == null || wSpec.getPartition() == null || getPartition() != null ) {
        return;
      }

      setDefaultWindowSpec(wSpec);
      setPartition(wSpec.getPartition());

      if ( getOrder() == null ) {
        setOrder(wSpec.getOrder());
      }
      else {
        if ( wSpec.getOrder() == null ) {
          wSpec.setOrder(getOrder());
        }
      }

    }

    public void inferDefaultOrderSpec(OrderSpec oSpec) {
      if ( !getName().equals(FunctionRegistry.WINDOWING_TABLE_FUNCTION)) {
        return;
      }
      setOrder(oSpec);
      if ( getDefaultWindowSpec() != null ) {
        WindowSpec dws = getDefaultWindowSpec();
        if ( dws.getOrder() == null ) {
          dws.setOrder(oSpec);
        }
      }
    }

    @Override
	  public boolean sourcedFromHive()
	  {
	    return input.sourcedFromHive();
	  }

	  @Override
	  public String toString()
	  {
	    StringBuilder buf = new StringBuilder();

	    buf.append(name).append("(");
	    buf.append(PTFSpec.NL).append("  ").append(input);
	    if ( args != null )
	    {
	      buf.append(PTFSpec.NL).append("  ");
	      boolean first = true;
	      for(CommonTree arg : args)
	      {
	        if ( first) {
	          first = false;
	        } else {
	          buf.append(", ");
	        }
	        buf.append(arg.toStringTree());
	      }
	    }
	    buf.append(PTFSpec.NL).append("  )");
	    if ( partition != null )
	    {
	      buf.append(" ").append(partition);
	      if ( order != null ) {
	        buf.append(" ").append(order);
	      }
	    }

	    return buf.toString();
	  }
	}

	/*
	 * Window Function Spec
	 */
	public static class WindowFunctionSpec
	{
	  String name;
	  boolean isStar;
	  boolean isDistinct;
	  ArrayList<ASTNode> args;
	  WindowSpec windowSpec;
	  String alias;
	  ASTNode expression;


	  public String getName()
	  {
	    return name;
	  }

	  public void setName(String name)
	  {
	    this.name = name;
	  }

	  public boolean isStar()
	  {
	    return isStar;
	  }

	  public void setStar(boolean isStar)
	  {
	    this.isStar = isStar;
	  }

	  public boolean isDistinct()
	  {
	    return isDistinct;
	  }

	  public void setDistinct(boolean isDistinct)
	  {
	    this.isDistinct = isDistinct;
	  }

	  public ArrayList<ASTNode> getArgs()
	  {
	    args = args == null ? new ArrayList<ASTNode>() : args;
	    return args;
	  }

	  public void setArgs(ArrayList<ASTNode> args)
	  {
	    this.args = args;
	  }

	  public void addArg(CommonTree arg)
	  {
	    args = args == null ? new ArrayList<ASTNode>() : args;
	    args.add((ASTNode)arg);
	  }

	  public WindowSpec getWindowSpec()
	  {
	    return windowSpec;
	  }

	  public void setWindowSpec(WindowSpec windowSpec)
	  {
	    this.windowSpec = windowSpec;
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

	  @Override
	  public String toString()
	  {
	    StringBuilder buf = new StringBuilder();
	    buf.append(name).append("(");
	    if (isStar )
	    {
	      buf.append("*");
	    }
	    else
	    {
	      if ( isDistinct )
	      {
	        buf.append("distinct ");
	      }
	      if ( args != null )
	      {
	        boolean first = true;
	        for(CommonTree arg : args)
	        {
	          if ( first) {
	            first = false;
	          } else {
	            buf.append(", ");
	          }
	          buf.append(arg.toStringTree());
	        }
	      }
	    }

	    buf.append(")");

	    if ( windowSpec != null )
	    {
	      buf.append(" ").append(windowSpec.toString());
	    }

	    if ( alias != null )
	    {
	      buf.append(" as ").append(alias);
	    }

	    return buf.toString();
	  }

	}

	/*
	 * PartitionSpec class
	 */
	public static class PartitionSpec
	{
	  ArrayList<ColumnSpec> columns;

	  public ArrayList<ColumnSpec> getColumns()
	  {
	    return columns;
	  }

	  public void setColumns(ArrayList<ColumnSpec> columns)
	  {
	    this.columns = columns;
	  }

	  public void addColumn(ColumnSpec c)
	  {
	    columns = columns == null ? new ArrayList<ColumnSpec>() : columns;
	    columns.add(c);
	  }

	  @Override
	  public int hashCode()
	  {
	    final int prime = 31;
	    int result = 1;
	    result = prime * result + ((columns == null) ? 0 : columns.hashCode());
	    return result;
	  }

	  @Override
	  public boolean equals(Object obj)
	  {
	    if (this == obj) {
	      return true;
	    }
	    if (obj == null) {
	      return false;
	    }
	    if (getClass() != obj.getClass()) {
	      return false;
	    }
	    PartitionSpec other = (PartitionSpec) obj;
	    if (columns == null)
	    {
	      if (other.columns != null) {
	        return false;
	      }
	    }
	    else if (!columns.equals(other.columns)) {
	      return false;
	    }
	    return true;
	  }

	  @Override
	  public String toString()
	  {
	    return PTFUtils.sprintf("partitionColumns=%s",PTFUtils.toString(columns));
	  }
	}

	/*
	 * OrderSpec
	 */
	public static class OrderSpec
	{
	  ArrayList<OrderColumnSpec> columns;

	  public OrderSpec() {}

	  public OrderSpec(PartitionSpec pSpec)
	  {
	    for(ColumnSpec cSpec : pSpec.getColumns())
	    {
	      addColumn(new OrderColumnSpec(cSpec));
	    }
	  }

	  public ArrayList<OrderColumnSpec> getColumns()
	  {
	    return columns;
	  }

	  public void setColumns(ArrayList<OrderColumnSpec> columns)
	  {
	    this.columns = columns;
	  }

	  public void addColumn(OrderColumnSpec c)
	  {
	    columns = columns == null ? new ArrayList<OrderColumnSpec>() : columns;
	    columns.add(c);
	  }

	  @Override
	  public int hashCode()
	  {
	    final int prime = 31;
	    int result = 1;
	    result = prime * result + ((columns == null) ? 0 : columns.hashCode());
	    return result;
	  }

	  @Override
	  public boolean equals(Object obj)
	  {
	    if (this == obj) {
	      return true;
	    }
	    if (obj == null) {
	      return false;
	    }
	    if (getClass() != obj.getClass()) {
	      return false;
	    }
	    OrderSpec other = (OrderSpec) obj;
	    if (columns == null)
	    {
	      if (other.columns != null) {
	        return false;
	      }
	    }
	    else if (!columns.equals(other.columns)) {
	      return false;
	    }
	    return true;
	  }

	  @Override
	  public String toString()
	  {
	    return PTFUtils.sprintf("orderColumns=%s",PTFUtils.toString(columns));
	  }
	}

	/*
	 * ColumnSpec
	 */
	public static class ColumnSpec
	{
	  String tableName;
	  String columnName;
	  ASTNode expression;

	  public ColumnSpec() {}

	  public ColumnSpec(String tableName, String columnName)
	  {
	    super();
	    setTableName(tableName);
	    setColumnName(columnName);
	  }

	  public ColumnSpec(ColumnSpec cSpec)
	  {
	    this(cSpec.getTableName(), cSpec.getColumnName());
	    expression = cSpec.getExpression();
	  }

	  public String getTableName()
	  {
	    return tableName;
	  }

	  public void setTableName(String tableName)
	  {
	    this.tableName = tableName;
	  }

	  public String getColumnName()
	  {
	    return columnName;
	  }

	  public void setColumnName(String columnName)
	  {
	    this.columnName = columnName;
	  }

	  public ASTNode getExpression() {
	    return expression;
	  }

	  public void setExpression(ASTNode expression) {
	    this.expression = expression;
	  }

	  @Override
	  public int hashCode() {
	    final int prime = 31;
	    int result = 1;
	    result = prime * result + ((columnName == null) ? 0 : columnName.hashCode());
	    result = prime * result + ((expression == null) ? 0 : expression.hashCode());
	    result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
	    return result;
	  }

	  @Override
	  public boolean equals(Object obj) {
	    if (this == obj) {
	      return true;
	    }
	    if (obj == null) {
	      return false;
	    }
	    if (getClass() != obj.getClass()) {
	      return false;
	    }
	    ColumnSpec other = (ColumnSpec) obj;
	    if (columnName == null) {
	      if (other.columnName != null) {
	        return false;
	      }
	    } else if (!columnName.equals(other.columnName)) {
	      return false;
	    }
	    if (expression == null) {
	      if (other.expression != null) {
	        return false;
	      }
	    } else if (!expression.toStringTree().equals(other.expression.toStringTree())) {
	      return false;
	    }
	    if (tableName == null) {
	      if (other.tableName != null) {
	        return false;
	      }
	    } else if (!tableName.equals(other.tableName)) {
	      return false;
	    }
	    return true;
	  }

	  @Override
	  public String toString()
	  {
	/*    if ( tableName != null ) {
	      return PTFUtils.sprintf("%s.%s", tableName, columnName);
	    }
	    return columnName;
	*/
	  return expression.toStringTree();
	  }

	}

	/*
	 * OrderColumnSpec class
	 */
	public static class OrderColumnSpec extends ColumnSpec
	{
	  Order order;

	  public OrderColumnSpec() {}

	  public OrderColumnSpec(ColumnSpec cSpec)
	  {
	    super(cSpec);
	    order = Order.ASC;
	  }

	  public OrderColumnSpec(String tableName, String columnName, Order order)
	  {
	    super(tableName, columnName);
	    this.order = order;
	  }

	  public Order getOrder()
	  {
	    return order;
	  }

	  public void setOrder(Order order)
	  {
	    this.order = order;
	  }

	  @Override
	  public int hashCode()
	  {
	    final int prime = 31;
	    int result = super.hashCode();
	    result = prime * result + ((order == null) ? 0 : order.hashCode());
	    return result;
	  }

	  @Override
	  public boolean equals(Object obj)
	  {
	    if (this == obj) {
	      return true;
	    }
	    if (!super.equals(obj)) {
	      return false;
	    }
	    if (getClass() != obj.getClass()) {
	      return false;
	    }
	    OrderColumnSpec other = (OrderColumnSpec) obj;
	    if (order != other.order) {
	      return false;
	    }
	    return true;
	  }

	  @Override
	  public String toString()
	  {
	    return PTFUtils.sprintf("%s %s", super.toString(), order);
	  }
	}

	/*
	 * Window Spec class
	 */
	public static class WindowSpec
	{
	  String sourceId;
	  PartitionSpec partition;
	  OrderSpec order;
	  WindowFrameSpec window;

	  public WindowSpec() {}

	  public WindowSpec(String sourceId, PartitionSpec partition,
	      OrderSpec order, WindowFrameSpec window)
	  {
	    super();
	    this.sourceId = sourceId;
	    this.partition = partition;
	    this.order = order;
	    this.window = window;
	  }

	  public String getSourceId()
	  {
	    return sourceId;
	  }

	  public void setSourceId(String sourceId)
	  {
	    this.sourceId = sourceId;
	  }

	  public PartitionSpec getPartition()
	  {
	    return partition;
	  }

	  public void setPartition(PartitionSpec partition)
	  {
	    this.partition = partition;
	  }

	  public OrderSpec getOrder()
	  {
	    return order;
	  }

	  public void setOrder(OrderSpec order)
	  {
	    this.order = order;
	  }

	  public WindowFrameSpec getWindow()
	  {
	    return window;
	  }

	  public void setWindow(WindowFrameSpec window)
	  {
	    this.window = window;
	  }

	  @Override
	  public String toString()
	  {
	    StringBuilder buf = new StringBuilder();
	    if (sourceId != null) {
	      buf.append(PTFUtils.sprintf("%s ", sourceId));
	    }
	    if (partition != null) {
	      buf.append(PTFUtils.sprintf("%s ", partition));
	    }
	    if (order != null) {
	      buf.append(PTFUtils.sprintf("%s ", order));
	    }
	    if (window != null) {
	      buf.append(PTFUtils.sprintf("%s ", window));
	    }
	    return buf.toString();
	  }
	}

	/*
	 * Window Frame classes
	 */
	public static class WindowFrameSpec
	{
	  BoundarySpec start;
	  BoundarySpec end;

	  public WindowFrameSpec() {}

	  public WindowFrameSpec(BoundarySpec start, BoundarySpec end)
	  {
	    super();
	    this.start = start;
	    this.end = end;
	  }

	  public BoundarySpec getStart()
	  {
	    return start;
	  }

	  public void setStart(BoundarySpec start)
	  {
	    this.start = start;
	  }

	  public BoundarySpec getEnd()
	  {
	    return end;
	  }

	  public void setEnd(BoundarySpec end)
	  {
	    this.end = end;
	  }

	  @Override
	  public String toString()
	  {
	    return PTFUtils.sprintf( "window(start=%s, end=%s)", start, end);
	  }

	  public static enum Direction
	  {
	    PRECEDING,
	    CURRENT,
	    FOLLOWING
	  };

	  public abstract static class BoundarySpec implements Comparable<BoundarySpec>
	  {
	    public static int UNBOUNDED_AMOUNT = Integer.MAX_VALUE;

	    public abstract Direction getDirection();

	  }

	  public static class RangeBoundarySpec extends BoundarySpec
	  {

	    Direction direction;
	    int amt;

	    public RangeBoundarySpec() {}

	    public RangeBoundarySpec(Direction direction, int amt)
	    {
	      super();
	      this.direction = direction;
	      this.amt = amt;
	    }

	    @Override
	    public Direction getDirection()
	    {
	      return direction;
	    }

	    public void setDirection(Direction direction)
	    {
	      this.direction = direction;
	    }

	    public int getAmt()
	    {
	      return amt;
	    }

	    public void setAmt(int amt)
	    {
	      this.amt = amt;
	    }

	    @Override
	    public String toString()
	    {
	      return PTFUtils.sprintf( "range(%s %s)", (amt == UNBOUNDED_AMOUNT ? "Unbounded" : amt), direction);
	    }

	    public int compareTo(BoundarySpec other)
	    {
	      int c = direction.compareTo(other.getDirection());
	      if ( c != 0) {
	        return c;
	      }
	      RangeBoundarySpec rb = (RangeBoundarySpec) other;
	      return amt - rb.amt;
	    }

	  }

	  public static class CurrentRowSpec extends BoundarySpec
	  {
	    public CurrentRowSpec() {}

	    @Override
	    public String toString()
	    {
	      return PTFUtils.sprintf( "currentRow");
	    }

	    @Override
	    public Direction getDirection() { return Direction.CURRENT; }

	    public int compareTo(BoundarySpec other)
	    {
	      return getDirection().compareTo(other.getDirection());
	    }

	  }

	  public static class ValueBoundarySpec extends BoundarySpec
	  {
	    Direction direction;
	    ASTNode expression;
	    int amt;

	    public ValueBoundarySpec() {}

	    public ValueBoundarySpec(Direction direction, CommonTree expression,
	        int amt)
	    {
	      super();
	      this.direction = direction;
	      this.expression = (ASTNode) expression;
	      this.amt = amt;
	    }

	    @Override
	    public Direction getDirection()
	    {
	      return direction;
	    }

	    public void setDirection(Direction direction)
	    {
	      this.direction = direction;
	    }

	    public ASTNode getExpression()
	    {
	      return expression;
	    }

	    public void setExpression(CommonTree expression)
	    {
	      this.expression = (ASTNode) expression;
	    }

	    public void setExpression(ASTNode expression)
	    {
	      this.expression = expression;
	    }

	    public int getAmt()
	    {
	      return amt;
	    }

	    public void setAmt(int amt)
	    {
	      this.amt = amt;
	    }

	    @Override
	    public String toString()
	    {
	      return PTFUtils.sprintf( "value(%s %s %s)", expression.toStringTree(), amt, direction);
	    }

	    public int compareTo(BoundarySpec other)
	    {
	      int c = direction.compareTo(other.getDirection());
	      if ( c != 0) {
	        return c;
	      }
	      ValueBoundarySpec vb = (ValueBoundarySpec) other;
	      return amt - vb.amt;
	    }

	  }

	}

	/*
	 * Select Spec
	 */
	public static class SelectSpec implements Iterable<Object>
	{
	  ArrayList<ASTNode> expressions;
	  ArrayList<WindowFunctionSpec> windowFuncs;
	  ArrayList<Boolean> isWindowFn;
	  ArrayList<String> aliases;
	  LinkedHashMap<String, ASTNode> aliasToAST;

	  public ArrayList<ASTNode> getExpressions()
	  {
	    return expressions;
	  }

	  public void addExpression(CommonTree expr, String alias)
	  {
	    isWindowFn = isWindowFn == null ? new ArrayList<Boolean>() : isWindowFn;
	    expressions = expressions == null ? new ArrayList<ASTNode>() : expressions;
	    aliases = aliases == null ? new ArrayList<String>() : aliases;
	    isWindowFn.add(false);
	    expressions.add((ASTNode) expr);
	    aliases.add(alias);
	    aliasToAST = aliasToAST == null ? new LinkedHashMap<String, ASTNode>() : aliasToAST;
	    /*
	     * maintain alias mapping in lowercase. why?
	     * - colAlias mainatined as lowercase in RR
	     * - when we build an OI, fieldnames are converted to lowercase.
	     * So we get a match when looking for adding col expression mapping in the Select RR.
	     */
	    aliasToAST.put(alias.toLowerCase(), (ASTNode) expr);
	  }

	  public ArrayList<WindowFunctionSpec> getWindowFuncs()
	  {
	    return windowFuncs;
	  }

	  public void addWindowFunc(WindowFunctionSpec wFn, String alias)
	  {
	    isWindowFn = isWindowFn == null ? new ArrayList<Boolean>() : isWindowFn;
	    windowFuncs = windowFuncs == null ? new ArrayList<WindowFunctionSpec>() : windowFuncs;
	    aliases = aliases == null ? new ArrayList<String>() : aliases;
	    isWindowFn.add(true);
	    windowFuncs.add(wFn);
	    wFn.setAlias(alias);
	    aliases.add(alias);
	    aliasToAST = aliasToAST == null ? new LinkedHashMap<String, ASTNode>() : aliasToAST;
	    /*
	     * maintain alias mapping in lowercase. why?
	     * - colAlias mainatined as lowercase in RR
	     * - when we build an OI, fieldnames are converted to lowercase.
	     * So we get a match when looking for adding col expression mapping in the Select RR.
	     */
	    aliasToAST.put(alias.toLowerCase(), (ASTNode) wFn.getExpression());
	  }

	  public ArrayList<Boolean> getIsWindowFn()
	  {
	    return isWindowFn;
	  }

	  public class It implements Iterator<Object>
	  {
	    Iterator<ASTNode> exprIt;
	    Iterator<WindowFunctionSpec> wnFnIt;
	    Iterator<Boolean> isWnfnIt;

	    It()
	    {
	      if (SelectSpec.this.isWindowFn != null)
	      {
	        isWnfnIt = SelectSpec.this.isWindowFn.iterator();
	        if ( SelectSpec.this.windowFuncs != null ) {
	          wnFnIt = SelectSpec.this.windowFuncs.iterator();
	        }
	        if ( SelectSpec.this.expressions != null ) {
	          exprIt = SelectSpec.this.expressions.iterator();
	        }
	      }
	    }

	    @Override
	    public boolean hasNext()
	    {
	      return isWnfnIt != null && isWnfnIt.hasNext();
	    }

	    @Override
	    public Object next()
	    {
	      if ( hasNext() )
	      {
	        boolean isWFn = isWnfnIt.next();
	        if ( isWFn ) {
	          return wnFnIt.next();
	        } else {
	          return exprIt.next();
	        }
	      }
	      return null;
	    }

	    @Override
	    public void remove()
	    {
	      throw new UnsupportedOperationException();
	    }
	  }

	  public Iterator<Object> iterator()
	  {
	    return new It();
	  }

	  @Override
	  public String toString()
	  {
	    StringBuilder buf = new StringBuilder();
	    buf.append("select ");
	    boolean first = true;
	    Iterator<String> aIt = aliases.iterator();
	    for(Object o : this)
	    {
	      if ( first ) {
	        first = false;
	      } else {
	        buf.append(",");
	      }
	      buf.append(PTFSpec.NL).append("  ");
	      if ( o instanceof CommonTree)
	      {
	        buf.append(((CommonTree)o).toStringTree());
	      }
	      else
	      {
	        buf.append(o.toString());
	      }
	      String alias = aIt.next();
	      if (alias != null && (o instanceof CommonTree) )
	      {
	        buf.append(" as ").append(alias);
	      }
	    }
	    return buf.toString();
	  }

	  public void setExpressions(ArrayList<ASTNode> expressions)
	  {
	    this.expressions = expressions;
	  }

	  public void setWindowFuncs(ArrayList<WindowFunctionSpec> windowFuncs)
	  {
	    this.windowFuncs = windowFuncs;
	  }

	  public void setIsWindowFn(ArrayList<Boolean> isWindowFn)
	  {
	    this.isWindowFn = isWindowFn;
	  }

	  public void setAliases(ArrayList<String> aliases)
	  {
	    this.aliases = aliases;
	  }

	  public ArrayList<String> getAliases()
	  {
	    return aliases;
	  }

	  public Iterator<Object> getColumnListAndAlias()
	  {
	    return new ColumnListAndAliasItr();
	  }

	  public LinkedHashMap<String, ASTNode> getAliasToAST() {
	    return aliasToAST;
	  }

	  public void setAliasToAST(LinkedHashMap<String, ASTNode> aliasToAST) {
	    this.aliasToAST = aliasToAST;
	  }

	  class ColumnListAndAliasItr implements Iterator<Object>
	  {
	    int cnt;
	    int exprIdx;
	    int wdwFnIdx;
	    int idx;

	    ColumnListAndAliasItr()
	    {
	      cnt = SelectSpec.this.isWindowFn.size();
	      idx = 0;
	      exprIdx = 0;
	      wdwFnIdx = 0;
	    }

	    @Override
	    public boolean hasNext()
	    {
	      return idx < cnt;
	    }

	    @Override
	    public Object next()
	    {
	      boolean isWnFn = SelectSpec.this.isWindowFn.get(idx);
	      Object alias = SelectSpec.this.aliases.get(idx);
	      idx++;

	      if ( isWnFn )
	      {
	        return new Object[] { isWnFn, alias, SelectSpec.this.windowFuncs.get(wdwFnIdx++)};
	      }
	      else
	      {
	        return new Object[] { isWnFn, alias, SelectSpec.this.expressions.get(exprIdx++)};
	      }
	    }

	    @Override
	    public void remove()
	    {
	      throw new UnsupportedOperationException();
	    }

	  }

	}
}
