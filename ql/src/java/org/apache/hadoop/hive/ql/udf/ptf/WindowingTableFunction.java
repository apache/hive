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

package org.apache.hadoop.hive.ql.udf.ptf;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.PTFOperator;
import org.apache.hadoop.hive.ql.exec.PTFPartition;
import org.apache.hadoop.hive.ql.exec.PTFPartition.PTFPartitionIterator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.Order;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.BoundarySpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.Direction;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.ql.plan.ptf.BoundaryDef;
import org.apache.hadoop.hive.ql.plan.ptf.PTFExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.PartitionedTableFunctionDef;
import org.apache.hadoop.hive.ql.plan.ptf.ValueBoundaryDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFunctionDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowTableFunctionDef;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

@SuppressWarnings("deprecation")
public class WindowingTableFunction extends TableFunctionEvaluator {

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public void execute(PTFPartitionIterator<Object> pItr, PTFPartition outP) throws HiveException {
    ArrayList<List<?>> oColumns = new ArrayList<List<?>>();
    PTFPartition iPart = pItr.getPartition();
    StructObjectInspector inputOI;
    inputOI = (StructObjectInspector) iPart.getOutputOI();

    WindowTableFunctionDef wTFnDef = (WindowTableFunctionDef) getTableDef();
    Order order = wTFnDef.getOrder().getExpressions().get(0).getOrder();

    for(WindowFunctionDef wFn : wTFnDef.getWindowFunctions()) {
      boolean processWindow = processWindow(wFn);
      pItr.reset();
      if ( !processWindow ) {
        GenericUDAFEvaluator fEval = wFn.getWFnEval();
        Object[] args = new Object[wFn.getArgs() == null ? 0 : wFn.getArgs().size()];
        AggregationBuffer aggBuffer = fEval.getNewAggregationBuffer();
        while(pItr.hasNext()) {
          Object row = pItr.next();
          int i =0;
          if ( wFn.getArgs() != null ) {
            for(PTFExpressionDef arg : wFn.getArgs()) {
              args[i++] = arg.getExprEvaluator().evaluate(row);
            }
          }
          fEval.aggregate(aggBuffer, args);
        }
        Object out = fEval.evaluate(aggBuffer);
        if ( !wFn.isPivotResult()) {
          out = new SameList(iPart.size(), out);
        }
        oColumns.add((List<?>)out);
      } else {
        oColumns.add(executeFnwithWindow(getQueryDef(), wFn, iPart, order));
      }
    }

    /*
     * Output Columns in the following order
     * - the columns representing the output from Window Fns
     * - the input Rows columns
     */

    for(int i=0; i < iPart.size(); i++) {
      ArrayList oRow = new ArrayList();
      Object iRow = iPart.getAt(i);

      for(int j=0; j < oColumns.size(); j++) {
        oRow.add(oColumns.get(j).get(i));
      }

      for(StructField f : inputOI.getAllStructFieldRefs()) {
        oRow.add(inputOI.getStructFieldData(iRow, f));
      }

      outP.append(oRow);
    }
  }

  private boolean processWindow(WindowFunctionDef wFn) {
    WindowFrameDef frame = wFn.getWindowFrame();
    if ( frame == null ) {
      return false;
    }
    if ( frame.getStart().getAmt() == BoundarySpec.UNBOUNDED_AMOUNT &&
        frame.getEnd().getAmt() == BoundarySpec.UNBOUNDED_AMOUNT ) {
      return false;
    }
    return true;
  }

  public static class WindowingTableFunctionResolver extends TableFunctionResolver
  {
    /*
     * OI of object constructed from output of Wdw Fns; before it is put
     * in the Wdw Processing Partition. Set by Translator/Deserializer.
     */
    private transient StructObjectInspector wdwProcessingOutputOI;

    public StructObjectInspector getWdwProcessingOutputOI() {
      return wdwProcessingOutputOI;
    }

    public void setWdwProcessingOutputOI(StructObjectInspector wdwProcessingOutputOI) {
      this.wdwProcessingOutputOI = wdwProcessingOutputOI;
    }

    @Override
    protected TableFunctionEvaluator createEvaluator(PTFDesc ptfDesc, PartitionedTableFunctionDef tDef)
    {

      return new WindowingTableFunction();
    }

    @Override
    public void setupOutputOI() throws SemanticException {
      setOutputOI(wdwProcessingOutputOI);
    }

    /*
     * Setup the OI based on the:
     * - Input TableDef's columns
     * - the Window Functions.
     */
    @Override
    public void initializeOutputOI() throws HiveException {
      setupOutputOI();
    }


    @Override
    public boolean transformsRawInput() {
      return false;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.hive.ql.udf.ptf.TableFunctionResolver#carryForwardNames()
     * Setting to true is correct only for special internal Functions.
     */
    @Override
    public boolean carryForwardNames() {
      return true;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.hive.ql.udf.ptf.TableFunctionResolver#getOutputNames()
     * Set to null only because carryForwardNames is true.
     */
    @Override
    public ArrayList<String> getOutputColumnNames() {
      return null;
    }

  }

  ArrayList<Object> executeFnwithWindow(PTFDesc ptfDesc,
      WindowFunctionDef wFnDef,
      PTFPartition iPart,
      Order order)
    throws HiveException {
    ArrayList<Object> vals = new ArrayList<Object>();

    GenericUDAFEvaluator fEval = wFnDef.getWFnEval();

    Object[] args = new Object[wFnDef.getArgs() == null ? 0 : wFnDef.getArgs().size()];
    for(int i=0; i < iPart.size(); i++) {
      AggregationBuffer aggBuffer = fEval.getNewAggregationBuffer();
      Range rng = getRange(wFnDef, i, iPart, order);
      PTFPartitionIterator<Object> rItr = rng.iterator();
      PTFOperator.connectLeadLagFunctionsToPartition(ptfDesc, rItr);
      while(rItr.hasNext()) {
        Object row = rItr.next();
        int j = 0;
        if ( wFnDef.getArgs() != null ) {
          for(PTFExpressionDef arg : wFnDef.getArgs()) {
            args[j++] = arg.getExprEvaluator().evaluate(row);
          }
        }
        fEval.aggregate(aggBuffer, args);
      }
      Object out = fEval.evaluate(aggBuffer);
      out = ObjectInspectorUtils.copyToStandardObject(out, wFnDef.getOI());
      vals.add(out);
    }
    return vals;
  }

  Range getRange(WindowFunctionDef wFnDef, int currRow, PTFPartition p, Order order) throws HiveException
  {
    BoundaryDef startB = wFnDef.getWindowFrame().getStart();
    BoundaryDef endB = wFnDef.getWindowFrame().getEnd();
    boolean rowFrame = true;

    if ( startB instanceof ValueBoundaryDef || endB instanceof ValueBoundaryDef) {
      rowFrame = false;
    }

    int start, end;

    if (rowFrame) {
      start = getRowBoundaryStart(startB, currRow);
      end = getRowBoundaryEnd(endB, currRow, p);
    }
    else {
      ValueBoundaryScanner vbs;
      if ( startB instanceof ValueBoundaryDef ) {
        vbs = ValueBoundaryScanner.getScanner((ValueBoundaryDef)startB, order);
      }
      else {
        vbs = ValueBoundaryScanner.getScanner((ValueBoundaryDef)endB, order);
      }
      vbs.reset(startB);
      start =  vbs.computeStart(currRow, p);
      vbs.reset(endB);
      end =  vbs.computeEnd(currRow, p);
    }
    start = start < 0 ? 0 : start;
    end = end > p.size() ? p.size() : end;
    return new Range(start, end, p);
  }

  int getRowBoundaryStart(BoundaryDef b, int currRow) throws HiveException {
    Direction d = b.getDirection();
    int amt = b.getAmt();
    switch(d) {
    case PRECEDING:
      if (amt == BoundarySpec.UNBOUNDED_AMOUNT) {
        return 0;
      }
      else {
        return currRow - amt;
      }
    case CURRENT:
      return currRow;
    case FOLLOWING:
      return currRow + amt;
    }
    throw new HiveException("Unknown Start Boundary Direction: " + d);
  }

  int getRowBoundaryEnd(BoundaryDef b, int currRow, PTFPartition p) throws HiveException {
    Direction d = b.getDirection();
    int amt = b.getAmt();
    switch(d) {
    case PRECEDING:
      if ( amt == 0 ) {
        return currRow + 1;
      }
      return currRow - amt;
    case CURRENT:
      return currRow + 1;
    case FOLLOWING:
      if (amt == BoundarySpec.UNBOUNDED_AMOUNT) {
        return p.size();
      }
      else {
        return currRow + amt + 1;
      }
    }
    throw new HiveException("Unknown End Boundary Direction: " + d);
  }

  static class Range
  {
    int start;
    int end;
    PTFPartition p;

    public Range(int start, int end, PTFPartition p)
    {
      super();
      this.start = start;
      this.end = end;
      this.p = p;
    }

    public PTFPartitionIterator<Object> iterator()
    {
      return p.range(start, end);
    }
  }

  /*
   * - starting from the given rowIdx scan in the given direction until a row's expr
   * evaluates to an amt that crosses the 'amt' threshold specified in the ValueBoundaryDef.
   */
  static abstract class ValueBoundaryScanner
  {
    BoundaryDef bndDef;
    Order order;
    PTFExpressionDef expressionDef;

    public ValueBoundaryScanner(BoundaryDef bndDef, Order order, PTFExpressionDef expressionDef)
    {
      this.bndDef = bndDef;
      this.order = order;
      this.expressionDef = expressionDef;
    }

    public void reset(BoundaryDef bndDef) {
      this.bndDef = bndDef;
    }

    /*
|  Use | Boundary1.type | Boundary1. amt | Sort Key | Order | Behavior                          |
| Case |                |                |          |       |                                   |
|------+----------------+----------------+----------+-------+-----------------------------------|
|   1. | PRECEDING      | UNB            | ANY      | ANY   | start = 0                         |
|   2. | PRECEDING      | unsigned int   | NULL     | ASC   | start = 0                         |
|   3. |                |                |          | DESC  | scan backwards to row R2          |
|      |                |                |          |       | such that R2.sk is not null       |
|      |                |                |          |       | start = R2.idx + 1                |
|   4. | PRECEDING      | unsigned int   | not NULL | DESC  | scan backwards until row R2       |
|      |                |                |          |       | such that R2.sk - R.sk > amt      |
|      |                |                |          |       | start = R2.idx + 1                |
|   5. | PRECEDING      | unsigned int   | not NULL | ASC   | scan backward until row R2        |
|      |                |                |          |       | such that R.sk - R2.sk > bnd1.amt |
|      |                |                |          |       | start = R2.idx + 1                |
|   6. | CURRENT ROW    |                | NULL     | ANY   | scan backwards until row R2       |
|      |                |                |          |       | such that R2.sk is not null       |
|      |                |                |          |       | start = R2.idx + 1                |
|   7. | CURRENT ROW    |                | not NULL | ANY   | scan backwards until row R2       |
|      |                |                |          |       | such R2.sk != R.sk                |
|      |                |                |          |       | start = R2.idx + 1                |
|   8. | FOLLOWING      | UNB            | ANY      | ANY   | Error                             |
|   9. | FOLLOWING      | unsigned int   | NULL     | DESC  | start = partition.size            |
|  10. |                |                |          | ASC   | scan forward until R2             |
|      |                |                |          |       | such that R2.sk is not null       |
|      |                |                |          |       | start = R2.idx                    |
|  11. | FOLLOWING      | unsigned int   | not NULL | DESC  | scan forward until row R2         |
|      |                |                |          |       | such that R.sk - R2.sk > amt      |
|      |                |                |          |       | start = R2.idx                    |
|  12. |                |                |          | ASC   | scan forward until row R2         |
|      |                |                |          |       | such that R2.sk - R.sk > amt      |
|------+----------------+----------------+----------+-------+-----------------------------------|
     */
    protected int computeStart(int rowIdx, PTFPartition p) throws HiveException {
      switch(bndDef.getDirection()) {
      case PRECEDING:
        return computeStartPreceding(rowIdx, p);
      case CURRENT:
        return computeStartCurrentRow(rowIdx, p);
      case FOLLOWING:
        default:
          return computeStartFollowing(rowIdx, p);
      }
    }

    /*
     *
     */
    protected int computeStartPreceding(int rowIdx, PTFPartition p) throws HiveException {
      int amt = bndDef.getAmt();
      // Use Case 1.
      if ( amt == BoundarySpec.UNBOUNDED_AMOUNT ) {
        return 0;
      }
      Object sortKey = computeValue(p.getAt(rowIdx));

      if ( sortKey == null ) {
        // Use Case 2.
        if ( order == Order.ASC ) {
          return 0;
        }
        else { // Use Case 3.
          while ( sortKey == null && rowIdx >= 0 ) {
            --rowIdx;
            if ( rowIdx >= 0 ) {
              sortKey = computeValue(p.getAt(rowIdx));
            }
          }
          return rowIdx+1;
        }
      }

      Object rowVal = sortKey;
      int r = rowIdx;

      // Use Case 4.
      if ( order == Order.DESC ) {
        while (r >= 0 && !isGreater(rowVal, sortKey, amt) ) {
          r--;
          if ( r >= 0 ) {
            rowVal = computeValue(p.getAt(r));
          }
        }
        return r + 1;
      }
      else { // Use Case 5.
        while (r >= 0 && !isGreater(sortKey, rowVal, amt) ) {
          r--;
          if ( r >= 0 ) {
            rowVal = computeValue(p.getAt(r));
          }
        }
        return r + 1;
      }
    }

    protected int computeStartCurrentRow(int rowIdx, PTFPartition p) throws HiveException {
      Object sortKey = computeValue(p.getAt(rowIdx));

      // Use Case 6.
      if ( sortKey == null ) {
        while ( sortKey == null && rowIdx >= 0 ) {
          --rowIdx;
          if ( rowIdx >= 0 ) {
            sortKey = computeValue(p.getAt(rowIdx));
          }
        }
        return rowIdx+1;
      }

      Object rowVal = sortKey;
      int r = rowIdx;

      // Use Case 7.
      while (r >= 0 && isEqual(rowVal, sortKey) ) {
        r--;
        if ( r >= 0 ) {
          rowVal = computeValue(p.getAt(r));
        }
      }
      return r + 1;
    }

    protected int computeStartFollowing(int rowIdx, PTFPartition p) throws HiveException {
      int amt = bndDef.getAmt();
      Object sortKey = computeValue(p.getAt(rowIdx));

      Object rowVal = sortKey;
      int r = rowIdx;

      if ( sortKey == null ) {
        // Use Case 9.
        if ( order == Order.DESC) {
          return p.size();
        }
        else { // Use Case 10.
          while (r < p.size() && rowVal == null ) {
            r++;
            if ( r < p.size() ) {
              rowVal = computeValue(p.getAt(r));
            }
          }
          return r;
        }
      }

      // Use Case 11.
      if ( order == Order.DESC) {
        while (r < p.size() && !isGreater(sortKey, rowVal, amt) ) {
          r++;
          if ( r < p.size() ) {
            rowVal = computeValue(p.getAt(r));
          }
        }
        return r;
      }
      else { // Use Case 12.
        while (r < p.size() && !isGreater(rowVal, sortKey, amt) ) {
          r++;
          if ( r < p.size() ) {
            rowVal = computeValue(p.getAt(r));
          }
        }
        return r;
      }
    }

    /*
|  Use | Boundary2.type | Boundary2.amt | Sort Key | Order | Behavior                          |
| Case |                |               |          |       |                                   |
|------+----------------+---------------+----------+-------+-----------------------------------|
|   1. | PRECEDING      | UNB           | ANY      | ANY   | Error                             |
|   2. | PRECEDING      | unsigned int  | NULL     | DESC  | end = partition.size()            |
|   3. |                |               |          | ASC   | end = 0                           |
|   4. | PRECEDING      | unsigned int  | not null | DESC  | scan backward until row R2        |
|      |                |               |          |       | such that R2.sk - R.sk > bnd.amt  |
|      |                |               |          |       | end = R2.idx + 1                  |
|   5. | PRECEDING      | unsigned int  | not null | ASC   | scan backward until row R2        |
|      |                |               |          |       | such that R.sk -  R2.sk > bnd.amt |
|      |                |               |          |       | end = R2.idx + 1                  |
|   6. | CURRENT ROW    |               | NULL     | ANY   | scan forward until row R2         |
|      |                |               |          |       | such that R2.sk is not null       |
|      |                |               |          |       | end = R2.idx                      |
|   7. | CURRENT ROW    |               | not null | ANY   | scan forward until row R2         |
|      |                |               |          |       | such that R2.sk != R.sk           |
|      |                |               |          |       | end = R2.idx                      |
|   8. | FOLLOWING      | UNB           | ANY      | ANY   | end = partition.size()            |
|   9. | FOLLOWING      | unsigned int  | NULL     | DESC  | end = partition.size()            |
|  10. |                |               |          | ASC   | scan forward until row R2         |
|      |                |               |          |       | such that R2.sk is not null       |
|      |                |               |          |       | end = R2.idx                      |
|  11. | FOLLOWING      | unsigned int  | not NULL | DESC  | scan forward until row R2         |
|      |                |               |          |       | such R.sk - R2.sk > bnd.amt       |
|      |                |               |          |       | end = R2.idx                      |
|  12. |                |               |          | ASC   | scan forward until row R2         |
|      |                |               |          |       | such R2.sk - R2.sk > bnd.amt      |
|      |                |               |          |       | end = R2.idx                      |
|------+----------------+---------------+----------+-------+-----------------------------------|
     */
    protected int computeEnd(int rowIdx, PTFPartition p) throws HiveException {
      switch(bndDef.getDirection()) {
      case PRECEDING:
        return computeEndPreceding(rowIdx, p);
      case CURRENT:
        return computeEndCurrentRow(rowIdx, p);
      case FOLLOWING:
        default:
          return computeEndFollowing(rowIdx, p);
      }
    }

    protected int computeEndPreceding(int rowIdx, PTFPartition p) throws HiveException {
      int amt = bndDef.getAmt();
      // Use Case 1.
      // amt == UNBOUNDED, is caught during translation

      Object sortKey = computeValue(p.getAt(rowIdx));

      if ( sortKey == null ) {
        // Use Case 2.
        if ( order == Order.DESC ) {
          return p.size();
        }
        else { // Use Case 3.
          return 0;
        }
      }

      Object rowVal = sortKey;
      int r = rowIdx;

      // Use Case 4.
      if ( order == Order.DESC ) {
        while (r >= 0 && !isGreater(rowVal, sortKey, amt) ) {
          r--;
          if ( r >= 0 ) {
            rowVal = computeValue(p.getAt(r));
          }
        }
        return r + 1;
      }
      else { // Use Case 5.
        while (r >= 0 && !isGreater(sortKey, rowVal, amt) ) {
          r--;
          if ( r >= 0 ) {
            rowVal = computeValue(p.getAt(r));
          }
        }
        return r + 1;
      }
    }

    protected int computeEndCurrentRow(int rowIdx, PTFPartition p) throws HiveException {
      Object sortKey = computeValue(p.getAt(rowIdx));

      // Use Case 6.
      if ( sortKey == null ) {
        while ( sortKey == null && rowIdx < p.size() ) {
          ++rowIdx;
          if ( rowIdx < p.size() ) {
            sortKey = computeValue(p.getAt(rowIdx));
          }
        }
        return rowIdx;
      }

      Object rowVal = sortKey;
      int r = rowIdx;

      // Use Case 7.
      while (r < p.size() && isEqual(sortKey, rowVal) ) {
        r++;
        if ( r < p.size() ) {
          rowVal = computeValue(p.getAt(r));
        }
      }
      return r;
    }

    protected int computeEndFollowing(int rowIdx, PTFPartition p) throws HiveException {
      int amt = bndDef.getAmt();

      // Use Case 8.
      if ( amt == BoundarySpec.UNBOUNDED_AMOUNT ) {
        return p.size();
      }
      Object sortKey = computeValue(p.getAt(rowIdx));

      Object rowVal = sortKey;
      int r = rowIdx;

      if ( sortKey == null ) {
        // Use Case 9.
        if ( order == Order.DESC) {
          return p.size();
        }
        else { // Use Case 10.
          while (r < p.size() && rowVal == null ) {
            r++;
            if ( r < p.size() ) {
              rowVal = computeValue(p.getAt(r));
            }
          }
          return r;
        }
      }

      // Use Case 11.
      if ( order == Order.DESC) {
        while (r < p.size() && !isGreater(sortKey, rowVal, amt) ) {
          r++;
          if ( r < p.size() ) {
            rowVal = computeValue(p.getAt(r));
          }
        }
        return r;
      }
      else { // Use Case 12.
        while (r < p.size() && !isGreater(rowVal, sortKey, amt) ) {
          r++;
          if ( r < p.size() ) {
            rowVal = computeValue(p.getAt(r));
          }
        }
        return r;
      }
    }

    public Object computeValue(Object row) throws HiveException {
      Object o = expressionDef.getExprEvaluator().evaluate(row);
      return ObjectInspectorUtils.copyToStandardObject(o, expressionDef.getOI());
    }

    public abstract boolean isGreater(Object v1, Object v2, int amt);

    public abstract boolean isEqual(Object v1, Object v2);


    @SuppressWarnings("incomplete-switch")
    public static ValueBoundaryScanner getScanner(ValueBoundaryDef vbDef, Order order)
        throws HiveException {
      PrimitiveObjectInspector pOI = (PrimitiveObjectInspector) vbDef.getOI();
      switch(pOI.getPrimitiveCategory()) {
      case BYTE:
      case INT:
      case LONG:
      case SHORT:
      case TIMESTAMP:
        return new LongValueBoundaryScanner(vbDef, order, vbDef.getExpressionDef());
      case DOUBLE:
      case FLOAT:
        return new DoubleValueBoundaryScanner(vbDef, order, vbDef.getExpressionDef());
      case STRING:
        return new StringValueBoundaryScanner(vbDef, order, vbDef.getExpressionDef());
      }
      throw new HiveException(
          String.format("Internal Error: attempt to setup a Window for datatype %s",
              pOI.getPrimitiveCategory()));
    }
  }

  public static class LongValueBoundaryScanner extends ValueBoundaryScanner {
    public LongValueBoundaryScanner(BoundaryDef bndDef, Order order,
        PTFExpressionDef expressionDef) {
      super(bndDef,order,expressionDef);
    }

    @Override
    public boolean isGreater(Object v1, Object v2, int amt) {
      long l1 = PrimitiveObjectInspectorUtils.getLong(v1,
          (PrimitiveObjectInspector) expressionDef.getOI());
      long l2 = PrimitiveObjectInspectorUtils.getLong(v2,
          (PrimitiveObjectInspector) expressionDef.getOI());
      return (l1 -l2) > amt;
    }

    @Override
    public boolean isEqual(Object v1, Object v2) {
      long l1 = PrimitiveObjectInspectorUtils.getLong(v1,
          (PrimitiveObjectInspector) expressionDef.getOI());
      long l2 = PrimitiveObjectInspectorUtils.getLong(v2,
          (PrimitiveObjectInspector) expressionDef.getOI());
      return l1 == l2;
    }
  }

  public static class DoubleValueBoundaryScanner extends ValueBoundaryScanner {
    public DoubleValueBoundaryScanner(BoundaryDef bndDef, Order order,
        PTFExpressionDef expressionDef) {
      super(bndDef,order,expressionDef);
    }

    @Override
    public boolean isGreater(Object v1, Object v2, int amt) {
      double d1 = PrimitiveObjectInspectorUtils.getDouble(v1,
          (PrimitiveObjectInspector) expressionDef.getOI());
      double d2 = PrimitiveObjectInspectorUtils.getDouble(v2,
          (PrimitiveObjectInspector) expressionDef.getOI());
      return (d1 -d2) > amt;
    }

    @Override
    public boolean isEqual(Object v1, Object v2) {
      double d1 = PrimitiveObjectInspectorUtils.getDouble(v1,
          (PrimitiveObjectInspector) expressionDef.getOI());
      double d2 = PrimitiveObjectInspectorUtils.getDouble(v2,
          (PrimitiveObjectInspector) expressionDef.getOI());
      return d1 == d2;
    }
  }

  public static class StringValueBoundaryScanner extends ValueBoundaryScanner {
    public StringValueBoundaryScanner(BoundaryDef bndDef, Order order,
        PTFExpressionDef expressionDef) {
      super(bndDef,order,expressionDef);
    }

    @Override
    public boolean isGreater(Object v1, Object v2, int amt) {
      String s1 = PrimitiveObjectInspectorUtils.getString(v1,
          (PrimitiveObjectInspector) expressionDef.getOI());
      String s2 = PrimitiveObjectInspectorUtils.getString(v2,
          (PrimitiveObjectInspector) expressionDef.getOI());
      return s1 != null && s2 != null && s1.compareTo(s2) > 0;
    }

    @Override
    public boolean isEqual(Object v1, Object v2) {
      String s1 = PrimitiveObjectInspectorUtils.getString(v1,
          (PrimitiveObjectInspector) expressionDef.getOI());
      String s2 = PrimitiveObjectInspectorUtils.getString(v2,
          (PrimitiveObjectInspector) expressionDef.getOI());
      return (s1 == null && s2 == null) || s1.equals(s2);
    }
  }

  public static class SameList<E> extends AbstractList<E> {
    int sz;
    E val;

    public SameList(int sz, E val) {
      this.sz = sz;
      this.val = val;
    }

    @Override
    public E get(int index) {
      return val;
    }

    @Override
    public int size() {
      return sz;
    }

  }

}
