package org.apache.hadoop.hive.ql.udf.ptf;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.PTFOperator;
import org.apache.hadoop.hive.ql.exec.PTFPartition;
import org.apache.hadoop.hive.ql.exec.PTFPartition.PTFPartitionIterator;
import org.apache.hadoop.hive.ql.exec.WindowFunctionInfo;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.PTFSpec.WindowFrameSpec.BoundarySpec;
import org.apache.hadoop.hive.ql.parse.PTFSpec.WindowFrameSpec.Direction;
import org.apache.hadoop.hive.ql.parse.PTFSpec.WindowFunctionSpec;
import org.apache.hadoop.hive.ql.parse.PTFTranslator;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.PTFDef;
import org.apache.hadoop.hive.ql.plan.PTFDef.ArgDef;
import org.apache.hadoop.hive.ql.plan.PTFDef.SelectDef;
import org.apache.hadoop.hive.ql.plan.PTFDef.TableFuncDef;
import org.apache.hadoop.hive.ql.plan.PTFDef.WindowFrameDef.BoundaryDef;
import org.apache.hadoop.hive.ql.plan.PTFDef.WindowFrameDef.CurrentRowDef;
import org.apache.hadoop.hive.ql.plan.PTFDef.WindowFrameDef.RangeBoundaryDef;
import org.apache.hadoop.hive.ql.plan.PTFDef.WindowFrameDef.ValueBoundaryDef;
import org.apache.hadoop.hive.ql.plan.PTFDef.WindowFunctionDef;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

public class WindowingTableFunction extends TableFunctionEvaluator
{
  ArrayList<WindowFunctionDef> wFnDefs;

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public void execute(PTFPartitionIterator<Object> pItr, PTFPartition outP) throws HiveException
  {
    ArrayList<List<?>> oColumns = new ArrayList<List<?>>();
    PTFPartition iPart = pItr.getPartition();
    StructObjectInspector inputOI;
    try {
      inputOI = (StructObjectInspector) iPart.getSerDe().getObjectInspector();
    } catch (SerDeException se) {
      throw new HiveException(se);
    }

    for(WindowFunctionDef wFn : wFnDefs)
    {
      boolean processWindow = wFn.getWindow() != null && wFn.getWindow().getWindow() != null;
      pItr.reset();
      if ( !processWindow )
      {
        GenericUDAFEvaluator fEval = wFn.getEvaluator();
        Object[] args = new Object[wFn.getArgs() == null ? 0 : wFn.getArgs().size()];
        AggregationBuffer aggBuffer = fEval.getNewAggregationBuffer();
        while(pItr.hasNext())
        {
          Object row = pItr.next();
          int i =0;
          if ( wFn.getArgs() != null ) {
            for(ArgDef arg : wFn.getArgs())
            {
              args[i++] = arg.getExprEvaluator().evaluate(row);
            }
          }
          fEval.aggregate(aggBuffer, args);
        }
        Object out = fEval.evaluate(aggBuffer);
        WindowFunctionInfo wFnInfo = FunctionRegistry.getWindowFunctionInfo(wFn.getSpec().getName());
        if ( !wFnInfo.isPivotResult())
        {
          out = new SameList(iPart.size(), out);
        }
        oColumns.add((List<?>)out);
      }
      else
      {
        oColumns.add(executeFnwithWindow(getQueryDef(), wFn, iPart));
      }
    }

    /*
     * Output Columns in the following order
     * - the columns representing the output from Window Fns
     * - the input Rows columns
     */

    for(int i=0; i < iPart.size(); i++)
    {
      ArrayList oRow = new ArrayList();
      Object iRow = iPart.getAt(i);

      for(int j=0; j < oColumns.size(); j++)
      {
        oRow.add(oColumns.get(j).get(i));
      }

      for(StructField f : inputOI.getAllStructFieldRefs())
      {
        oRow.add(inputOI.getStructFieldData(iRow, f));
      }

      outP.append(oRow);
    }
  }

  public static class WindowingTableFunctionResolver extends TableFunctionResolver
  {

    @Override
    protected TableFunctionEvaluator createEvaluator(PTFDef qDef, TableFuncDef tDef)
    {

      return new WindowingTableFunction();
    }

    @Override
    public void setupOutputOI() throws SemanticException
    {
      ArrayList<WindowFunctionDef> wFnDefs = new ArrayList<WindowFunctionDef>();
      PTFDef qDef = getQueryDef();
      SelectDef select = qDef.getSelectList();
      ArrayList<WindowFunctionSpec> wFnSpecs = qDef.getSpec().getSelectList().getWindowFuncs();
      ArrayList<String> aliases = new ArrayList<String>();
      ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

      /*
       * Setup the columns in the OI in the following order
       * - the columns representing the Window Fns
       * - the columns from the input
       * Why?
       * - during translation the input contains Virtual columns that are not represent during runtime
       * - this messes with the Column Numbers (and hence internal Names) if we add the columns in a different order.
       */

      for(WindowFunctionSpec wFnS : wFnSpecs)
      {
          WindowFunctionDef wFnDef = PTFTranslator.translate(qDef, getEvaluator().getTableDef(), wFnS);
          WindowFunctionInfo wFnInfo = FunctionRegistry.getWindowFunctionInfo(wFnS.getName());
          wFnDefs.add(wFnDef);
          aliases.add(wFnS.getAlias());
          if ( wFnInfo.isPivotResult())
          {
            ListObjectInspector lOI = (ListObjectInspector) wFnDef.getOI();
            fieldOIs.add(lOI.getListElementObjectInspector());
          }
          else
          {
            fieldOIs.add(wFnDef.getOI());
          }
      }

      PTFTranslator.addInputColumnsToList(qDef, getEvaluator().getTableDef(), aliases, fieldOIs);

      select.setWindowFuncs(wFnDefs);
      WindowingTableFunction wTFn = (WindowingTableFunction) getEvaluator();
      wTFn.wFnDefs = wFnDefs;

      StructObjectInspector OI = ObjectInspectorFactory.getStandardStructObjectInspector(aliases, fieldOIs);
      setOutputOI(OI);
    }

    /*
     * Setup the OI based on the:
     * - Input TableDef's columns
     * - the Window Functions.
     */
    @Override
    public void initializeOutputOI() throws HiveException
    {
      PTFDef qDef = getQueryDef();
      TableFuncDef tblFuncDef = evaluator.getTableDef();
      WindowingTableFunction wTFn = (WindowingTableFunction) tblFuncDef.getFunction();
      ArrayList<WindowFunctionDef> wFnDefs = qDef.getSelectList().getWindowFuncs();
      ArrayList<String> aliases = new ArrayList<String>();
      ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

      /*
       * Setup the columns in the OI in the following order
       * - the columns representing the Window Fns
       * - the columns from the input
       * Why?
       * - during translation the input contains Virtual columns that are not present during runtime
       * - this messes with the Column Numbers (and hence internal Names) if we add the columns in a different order.
       */

      for (WindowFunctionDef wFnDef : wFnDefs) {
        WindowFunctionSpec wFnS = wFnDef.getSpec();
        WindowFunctionInfo wFnInfo = FunctionRegistry.getWindowFunctionInfo(wFnS.getName());
        aliases.add(wFnS.getAlias());
        if ( wFnInfo.isPivotResult())
        {
          ListObjectInspector lOI = (ListObjectInspector) wFnDef.getOI();
          fieldOIs.add(lOI.getListElementObjectInspector());
        }
        else
        {
          fieldOIs.add(wFnDef.getOI());
        }

      }
      PTFTranslator.addInputColumnsToList(qDef, getEvaluator().getTableDef(), aliases, fieldOIs);

      wTFn.wFnDefs = wFnDefs;
      StructObjectInspector OI = ObjectInspectorFactory.getStandardStructObjectInspector(aliases, fieldOIs);
      setOutputOI(OI);
    }


    @Override
    public boolean transformsRawInput()
    {
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

  static ArrayList<Object> executeFnwithWindow(PTFDef qDef, WindowFunctionDef wFnDef, PTFPartition iPart)
    throws HiveException
  {
    ArrayList<Object> vals = new ArrayList<Object>();

    GenericUDAFEvaluator fEval = wFnDef.getEvaluator();
    Object[] args = new Object[wFnDef.getArgs().size()];
    for(int i=0; i < iPart.size(); i++)
    {
      AggregationBuffer aggBuffer = fEval.getNewAggregationBuffer();
      Range rng = getRange(wFnDef, i, iPart);
      PTFPartitionIterator<Object> rItr = rng.iterator();
      PTFOperator.connectLeadLagFunctionsToPartition(qDef, rItr);
      while(rItr.hasNext())
      {
        Object row = rItr.next();
        int j = 0;
        for(ArgDef arg : wFnDef.getArgs())
        {
          args[j++] = arg.getExprEvaluator().evaluate(row);
        }
        fEval.aggregate(aggBuffer, args);
      }
      Object out = fEval.evaluate(aggBuffer);
      out = ObjectInspectorUtils.copyToStandardObject(out, wFnDef.getOI());
      vals.add(out);
    }
    return vals;
  }

  static Range getRange(WindowFunctionDef wFnDef, int currRow, PTFPartition p) throws HiveException
  {
    BoundaryDef startB = wFnDef.getWindow().getWindow().getStart();
    BoundaryDef endB = wFnDef.getWindow().getWindow().getEnd();

    int start = getIndex(startB, currRow, p, false);
    int end = getIndex(endB, currRow, p, true);

    return new Range(start, end, p);
  }

  static int getIndex(BoundaryDef bDef, int currRow, PTFPartition p, boolean end) throws HiveException
  {
    if ( bDef instanceof CurrentRowDef)
    {
      return currRow + (end ? 1 : 0);
    }
    else if ( bDef instanceof RangeBoundaryDef)
    {
      RangeBoundaryDef rbDef = (RangeBoundaryDef) bDef;
      int amt = rbDef.getAmt();

      if ( amt == BoundarySpec.UNBOUNDED_AMOUNT )
      {
        return rbDef.getDirection() == Direction.PRECEDING ? 0 : p.size();
      }

      amt = rbDef.getDirection() == Direction.PRECEDING ?  -amt : amt;
      int idx = currRow + amt;
      idx = idx < 0 ? 0 : (idx > p.size() ? p.size() : idx);
      return idx + (end && idx < p.size() ? 1 : 0);
    }
    else
    {
      ValueBoundaryScanner vbs = ValueBoundaryScanner.getScanner((ValueBoundaryDef)bDef);
      return vbs.computeBoundaryRange(currRow, p);
    }
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
    ValueBoundaryDef bndDef;

    public ValueBoundaryScanner(ValueBoundaryDef bndDef)
    {
      this.bndDef = bndDef;
    }

    /*
     * return the other end of the Boundary
     * - when scanning backwards: go back until you reach a row where the
     * startingValue - rowValue >= amt
     * - when scanning forward:  go forward go back until you reach a row where the
     *  rowValue - startingValue >= amt
     */
    public int computeBoundaryRange(int rowIdx, PTFPartition p) throws HiveException
    {
      int r = rowIdx;
      Object rowValue = computeValue(p.getAt(r));
      int amt = bndDef.getAmt();

      if ( amt == BoundarySpec.UNBOUNDED_AMOUNT )
      {
        return bndDef.getDirection() == Direction.PRECEDING ? 0 : p.size();
      }

      Direction d = bndDef.getDirection();
      boolean scanNext = rowValue != null;
      while ( scanNext )
      {
        if ( d == Direction.PRECEDING ) {
          r = r - 1;
        }
        else {
          r = r + 1;
        }

        if ( r < 0 || r >= p.size() )
        {
          scanNext = false;
          break;
        }

        Object currVal = computeValue(p.getAt(r));
        if ( currVal == null )
        {
          scanNext = false;
          break;
        }

        switch(d)
        {
        case PRECEDING:
          scanNext = !isGreater(rowValue, currVal, amt);
        break;
        case FOLLOWING:
          scanNext = !isGreater(currVal, rowValue, amt);
        case CURRENT:
        default:
          break;
        }
      }
      /*
       * if moving backwards, then r is at a row that failed the range test. So incr r, so that
       * Range starts from a row where the test succeeds.
       * Whereas when moving forward, leave r as is; because the Range's end value should be the
       * row idx not in the Range.
       */
      if ( d == Direction.PRECEDING ) {
        r = r + 1;
      }
      r = r < 0 ? 0 : (r >= p.size() ? p.size() : r);
      return r;
    }

    public Object computeValue(Object row) throws HiveException
    {
      Object o = bndDef.getExprEvaluator().evaluate(row);
      return ObjectInspectorUtils.copyToStandardObject(o, bndDef.getOI());
    }

    public abstract boolean isGreater(Object v1, Object v2, int amt);


    public static ValueBoundaryScanner getScanner(ValueBoundaryDef vbDef)
    {
      PrimitiveObjectInspector pOI = (PrimitiveObjectInspector) vbDef.getOI();
      switch(pOI.getPrimitiveCategory())
      {
      case BYTE:
      case INT:
      case LONG:
      case SHORT:
      case TIMESTAMP:
        return new LongValueBoundaryScanner(vbDef);
      case DOUBLE:
      case FLOAT:
        return new DoubleValueBoundaryScanner(vbDef);
      }
      return null;
    }
  }

  public static class LongValueBoundaryScanner extends ValueBoundaryScanner
  {
    public LongValueBoundaryScanner(ValueBoundaryDef bndDef)
    {
      super(bndDef);
    }

    @Override
    public boolean isGreater(Object v1, Object v2, int amt)
    {
      long l1 = PrimitiveObjectInspectorUtils.getLong(v1,
          (PrimitiveObjectInspector) bndDef.getOI());
      long l2 = PrimitiveObjectInspectorUtils.getLong(v2,
          (PrimitiveObjectInspector) bndDef.getOI());
      return (l1 -l2) >= amt;
    }
  }

  public static class DoubleValueBoundaryScanner extends ValueBoundaryScanner
  {
    public DoubleValueBoundaryScanner(ValueBoundaryDef bndDef)
    {
      super(bndDef);
    }

    @Override
    public boolean isGreater(Object v1, Object v2, int amt)
    {
      double d1 = PrimitiveObjectInspectorUtils.getDouble(v1,
          (PrimitiveObjectInspector) bndDef.getOI());
      double d2 = PrimitiveObjectInspectorUtils.getDouble(v2,
          (PrimitiveObjectInspector) bndDef.getOI());
      return (d1 -d2) >= amt;
    }
  }

  public static class SameList<E> extends AbstractList<E>
  {
    int sz;
    E val;

    public SameList(int sz, E val)
    {
      this.sz = sz;
      this.val = val;
    }

    @Override
    public E get(int index)
    {
      return val;
    }

    @Override
    public int size()
    {
      return sz;
    }

  }

}
