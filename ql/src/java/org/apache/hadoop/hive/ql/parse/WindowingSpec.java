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

import org.antlr.runtime.CommonToken;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.OrderSpec;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitionExpression;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitionSpec;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitioningSpec;

/*
 * Captures the Window processing specified in a Query. A Query may
 * contain:
 * - UDAF invocations on a Window.
 * - Lead/Lag function invocations that can only be evaluated in a
 *   Partition.
 * - For Queries that don't have a Group By all UDAF invocations are
 *   treated as Window Function invocations.
 * - For Queries that don't have a Group By, the Having condition is
 *   handled as a post processing on the rows output by Windowing
 *   processing.
 * Windowing is a container of all the Select Expressions that are
 * to be handled by Windowing. These are held in 2 lists: the functions
 * list holds WindowFunction invocations; the expressions list holds
 * Select Expressions having Lead/Lag function calls. It may also
 * contain an ASTNode representing the post filter to apply on the
 * output of Window Functions.
 * Windowing also contains all the Windows defined in the Query. One of
 * the Windows is designated as the 'default' Window. If the Query has a
 * Distribute By/Cluster By clause; then the information in these
 * clauses is captured as a Partitioning and used as the default Window
 * for the Query. Otherwise the first Window specified is treated as the
 * default.
 * Finally Windowing maintains a Map from an 'alias' to the ASTNode that
 * represents the Select Expression that was translated to a Window
 * Function invocation or a Window Expression. This is used when
 * building RowResolvers.
 */
public class WindowingSpec {
  HashMap<String, WindowExpressionSpec> aliasToWdwExpr;
  ASTNode filterExpr;
  HashMap<String, WindowSpec> windowSpecs;
  ArrayList<WindowExpressionSpec> windowExpressions;

  public void addWindowSpec(String name, WindowSpec wdwSpec) {
    windowSpecs = windowSpecs == null ? new HashMap<String, WindowSpec>() : windowSpecs;
    windowSpecs.put(name, wdwSpec);
  }

  public void addExpression(ASTNode expr, String alias) {
    windowExpressions = windowExpressions == null ?
        new ArrayList<WindowExpressionSpec>() : windowExpressions;
    aliasToWdwExpr = aliasToWdwExpr == null ?
        new HashMap<String, WindowExpressionSpec>() : aliasToWdwExpr;
    WindowExpressionSpec wExprSpec = new WindowExpressionSpec();
    wExprSpec.setAlias(alias);
    wExprSpec.setExpression(expr);

    windowExpressions.add(wExprSpec);
    aliasToWdwExpr.put(alias, wExprSpec);
  }

  public void addWindowFunction(WindowFunctionSpec wFn) {
    windowExpressions = windowExpressions == null ?
        new ArrayList<WindowExpressionSpec>() : windowExpressions;
    aliasToWdwExpr = aliasToWdwExpr == null ?
        new HashMap<String, WindowExpressionSpec>() : aliasToWdwExpr;
    windowExpressions.add(wFn);
    aliasToWdwExpr.put(wFn.getAlias(), wFn);
  }

  public HashMap<String, WindowExpressionSpec> getAliasToWdwExpr() {
    return aliasToWdwExpr;
  }

  public void setAliasToWdwExpr(HashMap<String, WindowExpressionSpec> aliasToWdwExpr) {
    this.aliasToWdwExpr = aliasToWdwExpr;
  }

  public ASTNode getFilterExpr() {
    return filterExpr;
  }

  public void setFilterExpr(ASTNode filterExpr) {
    this.filterExpr = filterExpr;
  }

  public HashMap<String, WindowSpec> getWindowSpecs() {
    return windowSpecs;
  }

  public void setWindowSpecs(HashMap<String, WindowSpec> windowSpecs) {
    this.windowSpecs = windowSpecs;
  }

  public ArrayList<WindowExpressionSpec> getWindowExpressions() {
    return windowExpressions;
  }

  public void setWindowExpressions(ArrayList<WindowExpressionSpec> windowExpressions) {
    this.windowExpressions = windowExpressions;
  }

  public PartitioningSpec getQueryPartitioningSpec() {
    /*
     * Why no null and class checks?
     * With the new design a WindowingSpec must contain a WindowFunctionSpec.
     * todo: cleanup datastructs.
     */
    WindowFunctionSpec wFn = (WindowFunctionSpec) getWindowExpressions().get(0);
    return wFn.getWindowSpec().getPartitioning();
  }

  public PartitionSpec getQueryPartitionSpec() {
    return getQueryPartitioningSpec().getPartSpec();
  }

  public OrderSpec getQueryOrderSpec() {
    return getQueryPartitioningSpec().getOrderSpec();
  }

  /*
   * Rules for Partitoning are:
   * | has Part | has Order | has Window | note                  |
   * |----------+-----------+------------+-----------------------|
   * | y        | y         | y          | everything specified  |
   * | y        | y         | n          | no window             |
   * | y        | n         | y          | order = partition     |
   * | y        | n         | n          | same as above         |
   * | n        | y         | y          | partition on constant |
   * | n        | y         | n          | same as above         |
   * | n        | n         | y          | same as above, o = p  |
   * | n        | n         | n          | the over() case       |
   */
  public void fillInWindowingSpecs() throws SemanticException {
    if ( getWindowExpressions() != null ) {
      for(WindowExpressionSpec expr : getWindowExpressions()) {
        if ( expr instanceof WindowFunctionSpec) {
          WindowFunctionSpec wFn = (WindowFunctionSpec) expr;
          WindowSpec wdwSpec = wFn.getWindowSpec();
          if ( wdwSpec != null ) {
            ArrayList<String> sources = new ArrayList<String>();
            fillInWindowSpec(wdwSpec.getSourceId(), wdwSpec, sources);
          }
          wFn.setWindowSpec(applyContantPartition(wdwSpec));
          wFn.getWindowSpec().prefixOrderByPartitionSpec();
        }
      }
    }
  }

  private void fillInWindowSpec(String sourceId, WindowSpec dest, ArrayList<String> visited)
      throws SemanticException
  {
    if (sourceId != null)
    {
      if ( visited.contains(sourceId)) {
        visited.add(sourceId);
        throw new SemanticException(String.format("Cycle in Window references %s", visited));
      }
      WindowSpec source = getWindowSpecs().get(sourceId);
      if (source == null || source.equals(dest))
      {
        throw new SemanticException(String.format("Window Spec %s refers to an unknown source " ,
            dest));
      }

      if (dest.getPartition() == null)
      {
        dest.setPartition(source.getPartition());
      }

      if (dest.getOrder() == null)
      {
        dest.setOrder(source.getOrder());
      }

      if (dest.getWindowFrame() == null)
      {
        dest.setWindowFrame(source.getWindowFrame());
      }

      visited.add(sourceId);

      fillInWindowSpec(source.getSourceId(), dest, visited);
    }
  }

  private WindowSpec applyContantPartition(WindowSpec wdwSpec) {
    wdwSpec = wdwSpec == null ? new WindowSpec() : wdwSpec;
    PartitionSpec partSpec = wdwSpec.getPartition();
    if ( partSpec == null ) {
      partSpec = new PartitionSpec();
      PartitionExpression partExpr = new PartitionExpression();
      partExpr.setExpression(new ASTNode(new CommonToken(HiveParser.Number, "0")));
      partSpec.addExpression(partExpr);
      wdwSpec.setPartition(partSpec);
    }
    return wdwSpec;
  }

  /*
   * Represents a Select Expression in the context of Windowing. These can
   * refer to the output of Windowing Functions and can navigate the
   * Partition using Lead/Lag functions.
   */
  public static class WindowExpressionSpec {
    String alias;
    ASTNode expression;
    public String getAlias() {
      return alias;
    }
    public void setAlias(String alias) {
      this.alias = alias;
    }
    public ASTNode getExpression() {
      return expression;
    }
    public void setExpression(ASTNode expression) {
      this.expression = expression;
    }
  }

  /*
   * Represents a UDAF invocation in the context of a Window Frame. As
   * explained above sometimes UDAFs will be handled as Window Functions
   * even w/o an explicit Window specification. This is to support Queries
   * that have no Group By clause. A Window Function invocation captures:
   * - the ASTNode that represents this invocation
   * - its name
   * - whether it is star/distinct invocation.
   * - its alias
   * - and an optional Window specification
   */
  public static class WindowFunctionSpec extends WindowExpressionSpec
  {
    String name;
    boolean isStar;
    boolean isDistinct;
    ArrayList<ASTNode> args;
    WindowSpec windowSpec;

    public String getName() {
      return name;
    }
    public void setName(String name) {
      this.name = name;
    }
    public boolean isStar() {
      return isStar;
    }
    public void setStar(boolean isStar) {
      this.isStar = isStar;
    }
    public boolean isDistinct() {
      return isDistinct;
    }
    public void setDistinct(boolean isDistinct) {
      this.isDistinct = isDistinct;
    }
    public ArrayList<ASTNode> getArgs() {
      args = args == null ? new ArrayList<ASTNode>() : args;
      return args;
    }
    public void setArgs(ArrayList<ASTNode> args) {
      this.args = args;
    }
    public void addArg(ASTNode arg) {
      args = args == null ? new ArrayList<ASTNode>() : args;
      args.add((ASTNode)arg);
    }
    public WindowSpec getWindowSpec() {
      return windowSpec;
    }
    public void setWindowSpec(WindowSpec windowSpec) {
      this.windowSpec = windowSpec;
    }
    @Override
    public String toString() {
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
          for(ASTNode arg : args)
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
   * It represents a WindowFrame applied to a Partitioning. A Window can
   * refer to a <i>source</i> Window by name. The source Window provides the
   * basis for this Window definition. This Window specification
   * extends/overrides the <i>source</i> Window definition. In our e.g. the
   * Select Expression $sum(p_retailprice) over (w1)$ is translated into a
   * WindowFunction instance that has a Window specification that refers
   * to the global Window Specification 'w1'. The Function's specification
   * has no content, but inherits all its attributes from 'w1' during
   * subsequent phases of translation.
   */
  public static class WindowSpec
  {
    String sourceId;
    PartitioningSpec partitioning;
    WindowFrameSpec windowFrame;
    public String getSourceId() {
      return sourceId;
    }
    public void setSourceId(String sourceId) {
      this.sourceId = sourceId;
    }
    public PartitioningSpec getPartitioning() {
      return partitioning;
    }
    public void setPartitioning(PartitioningSpec partitioning) {
      this.partitioning = partitioning;
    }
    public WindowFrameSpec getWindowFrame() {
      return windowFrame;
    }
    public void setWindowFrame(WindowFrameSpec windowFrame) {
      this.windowFrame = windowFrame;
    }
    public PartitionSpec getPartition() {
      return getPartitioning() == null ? null : getPartitioning().getPartSpec();
    }
    public void setPartition(PartitionSpec partSpec) {
      partitioning = partitioning == null ? new PartitioningSpec() : partitioning;
      partitioning.setPartSpec(partSpec);
    }
    public OrderSpec getOrder() {
      return getPartitioning() == null ? null : getPartitioning().getOrderSpec();
    }
    public void setOrder(OrderSpec orderSpec) {
      partitioning = partitioning == null ? new PartitioningSpec() : partitioning;
      partitioning.setOrderSpec(orderSpec);
    }

    protected void prefixOrderByPartitionSpec() {
      if ( getOrder() == null ) {
        setOrder(new OrderSpec());
      }
      if ( !getOrder().isPrefixedBy(getPartition()) ) {
        getOrder().prefixBy(getPartition());
      }
    }
  };

  /*
   * A WindowFrame specifies the Range on which a Window Function should
   * be applied for the 'current' row. Its is specified by a <i>start</i> and
   * <i>end</i> Boundary.
   */
  public static class WindowFrameSpec
  {
    BoundarySpec start;
    BoundarySpec end;

    public WindowFrameSpec() {
    }

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
      return String.format("window(start=%s, end=%s)", start, end);
    }

  }

  public static enum Direction
  {
    PRECEDING,
    CURRENT,
    FOLLOWING
  };

  /*
   * A Boundary specifies how many rows back/forward a WindowFrame extends from the
   * current row. A Boundary is specified as:
   * - Range Boundary :: as the number of rows to go forward or back from
                    the Current Row.
   * - Current Row :: which implies the Boundary is at the current row.
   * - Value Boundary :: which is specified as the amount the value of an
                    Expression must decrease/increase
   */
  public abstract static class BoundarySpec implements Comparable<BoundarySpec>
  {
    public static int UNBOUNDED_AMOUNT = Integer.MAX_VALUE;

    public abstract Direction getDirection();

  }

  public static class RangeBoundarySpec extends BoundarySpec
  {

    Direction direction;
    int amt;

    public RangeBoundarySpec() {
    }

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
      return String.format("range(%s %s)", (amt == UNBOUNDED_AMOUNT ? "Unbounded" : amt),
          direction);
    }

    public int compareTo(BoundarySpec other)
    {
      int c = direction.compareTo(other.getDirection());
      if (c != 0) {
        return c;
      }
      RangeBoundarySpec rb = (RangeBoundarySpec) other;
      return amt - rb.amt;
    }

  }

  public static class CurrentRowSpec extends BoundarySpec
  {
    public CurrentRowSpec() {
    }

    @Override
    public String toString()
    {
      return "currentRow";
    }

    @Override
    public Direction getDirection() {
      return Direction.CURRENT;
    }

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

    public ValueBoundarySpec() {
    }

    public ValueBoundarySpec(Direction direction, ASTNode expression,
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
      return String.format("value(%s %s %s)", expression.toStringTree(), amt, direction);
    }

    public int compareTo(BoundarySpec other)
    {
      int c = direction.compareTo(other.getDirection());
      if (c != 0) {
        return c;
      }
      ValueBoundarySpec vb = (ValueBoundarySpec) other;
      return amt - vb.amt;
    }

  }

}
