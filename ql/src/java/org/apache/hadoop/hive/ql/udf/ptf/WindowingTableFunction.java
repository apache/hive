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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.PTFOperator;
import org.apache.hadoop.hive.ql.exec.PTFPartition;
import org.apache.hadoop.hive.ql.exec.WindowFunctionInfo;
import org.apache.hadoop.hive.ql.exec.PTFPartition.PTFPartitionIterator;
import org.apache.hadoop.hive.ql.exec.PTFRollingPartition;
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
import org.apache.hadoop.hive.ql.udf.generic.ISupportStreamingModeForWindowing;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

@SuppressWarnings("deprecation")
public class WindowingTableFunction extends TableFunctionEvaluator {
  public static final Log LOG =LogFactory.getLog(WindowingTableFunction.class.getName());
  static class WindowingFunctionInfoHelper {
    private boolean supportsWindow;

    WindowingFunctionInfoHelper() {
    }

    public WindowingFunctionInfoHelper(boolean supportsWindow) {
      this.supportsWindow = supportsWindow;
    }

    public boolean isSupportsWindow() {
      return supportsWindow;
    }
    public void setSupportsWindow(boolean supportsWindow) {
      this.supportsWindow = supportsWindow;
    }
  }

  StreamingState streamingState;
  RankLimit rnkLimitDef;

  // There is some information about the windowing functions that needs to be initialized
  // during query compilation time, and made available to during the map/reduce tasks via
  // plan serialization.
  Map<String, WindowingFunctionInfoHelper> windowingFunctionHelpers = null;
  
  public Map<String, WindowingFunctionInfoHelper> getWindowingFunctionHelpers() {
    return windowingFunctionHelpers;
  }

  public void setWindowingFunctionHelpers(
      Map<String, WindowingFunctionInfoHelper> windowingFunctionHelpers) {
    this.windowingFunctionHelpers = windowingFunctionHelpers;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public void execute(PTFPartitionIterator<Object> pItr, PTFPartition outP) throws HiveException {
    ArrayList<List<?>> oColumns = new ArrayList<List<?>>();
    PTFPartition iPart = pItr.getPartition();
    StructObjectInspector inputOI = iPart.getOutputOI();

    WindowTableFunctionDef wTFnDef = (WindowTableFunctionDef) getTableDef();
    Order order = wTFnDef.getOrder().getExpressions().get(0).getOrder();

    for(WindowFunctionDef wFn : wTFnDef.getWindowFunctions()) {
      boolean processWindow = processWindow(wFn);
      pItr.reset();
      if ( !processWindow ) {
        Object out = evaluateWindowFunction(wFn, pItr);
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

  Object evaluateWindowFunction(WindowFunctionDef wFn,
      PTFPartitionIterator<Object> pItr) throws HiveException {
    GenericUDAFEvaluator fEval = wFn.getWFnEval();
    Object[] args = new Object[wFn.getArgs() == null ? 0 : wFn.getArgs().size()];
    AggregationBuffer aggBuffer = fEval.getNewAggregationBuffer();
    while(pItr.hasNext())
    {
      Object row = pItr.next();
      int i =0;
      if ( wFn.getArgs() != null ) {
        for(PTFExpressionDef arg : wFn.getArgs())
        {
          args[i++] = arg.getExprEvaluator().evaluate(row);
        }
      }
      fEval.aggregate(aggBuffer, args);
    }
    Object out = fEval.evaluate(aggBuffer);
    out = ObjectInspectorUtils.copyToStandardObject(out, wFn.getOI());
    return out;
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

  private boolean streamingPossible(Configuration cfg, WindowFunctionDef wFnDef)
      throws HiveException {
    WindowFrameDef wdwFrame = wFnDef.getWindowFrame();

    WindowingFunctionInfoHelper wFnInfo = getWindowingFunctionInfoHelper(wFnDef.getName());
    if (!wFnInfo.isSupportsWindow()) {
      return true;
    }

    BoundaryDef start = wdwFrame.getStart();
    BoundaryDef end = wdwFrame.getEnd();

    /*
     * Currently we are not handling dynamic sized windows implied by range
     * based windows.
     */
    if (start instanceof ValueBoundaryDef || end instanceof ValueBoundaryDef) {
      return false;
    }

    /*
     * Windows that are unbounded following don't benefit from Streaming.
     */
    if (end.getAmt() == BoundarySpec.UNBOUNDED_AMOUNT) {
      return false;
    }

    /*
     * let function decide if it can handle this special case.
     */
    if (start.getAmt() == BoundarySpec.UNBOUNDED_AMOUNT) {
      return true;
    }

    int windowLimit = HiveConf.getIntVar(cfg, ConfVars.HIVEJOINCACHESIZE);

    if (windowLimit < (start.getAmt() + end.getAmt() + 1)) {
      return false;
    }

    return true;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hive.ql.udf.ptf.TableFunctionEvaluator#canAcceptInputAsStream
   * ()
   * 
   * WindowTableFunction supports streaming if all functions meet one of these
   * conditions: 1. The Function implements ISupportStreamingModeForWindowing 2.
   * Or returns a non null Object for the getWindowingEvaluator, that implements
   * ISupportStreamingModeForWindowing. 3. Is an invocation on a 'fixed' window.
   * So no Unbounded Preceding or Following.
   */
  @SuppressWarnings("resource")
  private int[] setCanAcceptInputAsStream(Configuration cfg) throws HiveException {

    canAcceptInputAsStream = false;

    if (ptfDesc.getLlInfo().getLeadLagExprs() != null) {
      return null;
    }

    WindowTableFunctionDef tabDef = (WindowTableFunctionDef) getTableDef();
    int precedingSpan = 0;
    int followingSpan = 0;

    for (int i = 0; i < tabDef.getWindowFunctions().size(); i++) {
      WindowFunctionDef wFnDef = tabDef.getWindowFunctions().get(i);
      WindowFrameDef wdwFrame = wFnDef.getWindowFrame();
      GenericUDAFEvaluator fnEval = wFnDef.getWFnEval();
      boolean streamingPossible = streamingPossible(cfg, wFnDef);
      GenericUDAFEvaluator streamingEval = streamingPossible ? fnEval
          .getWindowingEvaluator(wdwFrame) : null;
      if (streamingEval != null
          && streamingEval instanceof ISupportStreamingModeForWindowing) {
        continue;
      }
      BoundaryDef start = wdwFrame.getStart();
      BoundaryDef end = wdwFrame.getEnd();
      if (!(end instanceof ValueBoundaryDef)
          && !(start instanceof ValueBoundaryDef)) {
        if (end.getAmt() != BoundarySpec.UNBOUNDED_AMOUNT
            && start.getAmt() != BoundarySpec.UNBOUNDED_AMOUNT
            && end.getDirection() != Direction.PRECEDING
            && start.getDirection() != Direction.FOLLOWING) {

          int amt = wdwFrame.getStart().getAmt();
          if (amt > precedingSpan) {
            precedingSpan = amt;
          }

          amt = wdwFrame.getEnd().getAmt();
          if (amt > followingSpan) {
            followingSpan = amt;
          }
          continue;
        }
      }
      return null;
    }
    
    int windowLimit = HiveConf.getIntVar(cfg, ConfVars.HIVEJOINCACHESIZE);

    if (windowLimit < (followingSpan + precedingSpan + 1)) {
      return null;
    }

    canAcceptInputAsStream = true;
    return new int[] {precedingSpan, followingSpan};
  }

  private void initializeWindowingFunctionInfoHelpers() throws SemanticException {
    // getWindowFunctionInfo() cannot be called during map/reduce tasks. So cache necessary
    // values during query compilation, and rely on plan serialization to bring this info
    // to the object during the map/reduce tasks.
    if (windowingFunctionHelpers != null) {
      return;
    }

    windowingFunctionHelpers = new HashMap<String, WindowingFunctionInfoHelper>();
    WindowTableFunctionDef tabDef = (WindowTableFunctionDef) getTableDef();
    for (int i = 0; i < tabDef.getWindowFunctions().size(); i++) {
      WindowFunctionDef wFn = tabDef.getWindowFunctions().get(i);
      GenericUDAFEvaluator fnEval = wFn.getWFnEval();
      WindowFunctionInfo wFnInfo = FunctionRegistry.getWindowFunctionInfo(wFn.getName());
      boolean supportsWindow = wFnInfo.isSupportsWindow();
      windowingFunctionHelpers.put(wFn.getName(), new WindowingFunctionInfoHelper(supportsWindow));
    }
  }

  @Override
  protected void setOutputOI(StructObjectInspector outputOI) {
    super.setOutputOI(outputOI);
    // Call here because at this point the WindowTableFunctionDef has been set
    try {
      initializeWindowingFunctionInfoHelpers();
    } catch (SemanticException err) {
      throw new RuntimeException("Unexpected error while setting up windowing function", err);
    }
  }

  private WindowingFunctionInfoHelper getWindowingFunctionInfoHelper(String fnName) {
    WindowingFunctionInfoHelper wFnInfoHelper = windowingFunctionHelpers.get(fnName);
    if (wFnInfoHelper == null) {
      // Should not happen
      throw new RuntimeException("No cached WindowingFunctionInfoHelper for " + fnName);
    }
    return wFnInfoHelper;
  }

  @Override
  public void initializeStreaming(Configuration cfg,
      StructObjectInspector inputOI, boolean isMapSide) throws HiveException {

    int[] span = setCanAcceptInputAsStream(cfg);
    if (!canAcceptInputAsStream) {
      return;
    }

    WindowTableFunctionDef tabDef = (WindowTableFunctionDef) getTableDef();

    for (int i = 0; i < tabDef.getWindowFunctions().size(); i++) {
      WindowFunctionDef wFnDef = tabDef.getWindowFunctions().get(i);
      WindowFrameDef wdwFrame = wFnDef.getWindowFrame();
      GenericUDAFEvaluator fnEval = wFnDef.getWFnEval();
      GenericUDAFEvaluator streamingEval = fnEval
          .getWindowingEvaluator(wdwFrame);
      if (streamingEval != null) {
        wFnDef.setWFnEval(streamingEval);
        if (wFnDef.isPivotResult()) {
          ListObjectInspector listOI = (ListObjectInspector) wFnDef.getOI();
          wFnDef.setOI(listOI.getListElementObjectInspector());
        }
      }
    }

    if ( tabDef.getRankLimit() != -1 ) {
      rnkLimitDef = new RankLimit(tabDef.getRankLimit(), 
          tabDef.getRankLimitFunction(), tabDef.getWindowFunctions());
    }
    
    streamingState = new StreamingState(cfg, inputOI, isMapSide, tabDef,
        span[0], span[1]);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hive.ql.udf.ptf.TableFunctionEvaluator#startPartition()
   */
  @Override
  public void startPartition() throws HiveException {
    WindowTableFunctionDef tabDef = (WindowTableFunctionDef) getTableDef();
    streamingState.reset(tabDef);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hive.ql.udf.ptf.TableFunctionEvaluator#processRow(java
   * .lang.Object)
   * 
   * - hand row to each Function, provided there are enough rows for Function's
   * window. - call getNextObject on each Function. - output as many rows as
   * possible, based on minimum sz of Output List
   */
  @Override
  public List<Object> processRow(Object row) throws HiveException {

    /*
     * Once enough rows have been output, there is no need to process input rows.
     */
    if ( streamingState.rankLimitReached() ) {
      return null;
    }

    streamingState.rollingPart.append(row);
    row = streamingState.rollingPart
        .getAt(streamingState.rollingPart.size() - 1);

    WindowTableFunctionDef tabDef = (WindowTableFunctionDef) getTableDef();

    for (int i = 0; i < tabDef.getWindowFunctions().size(); i++) {
      WindowFunctionDef wFn = tabDef.getWindowFunctions().get(i);
      GenericUDAFEvaluator fnEval = wFn.getWFnEval();

      int a = 0;
      if (wFn.getArgs() != null) {
        for (PTFExpressionDef arg : wFn.getArgs()) {
          streamingState.funcArgs[i][a++] = arg.getExprEvaluator().evaluate(row);
        }
      }

      if (fnEval instanceof ISupportStreamingModeForWindowing) {
        fnEval.aggregate(streamingState.aggBuffers[i], streamingState.funcArgs[i]);
        Object out = ((ISupportStreamingModeForWindowing) fnEval)
            .getNextResult(streamingState.aggBuffers[i]);
        if (out != null) {
          streamingState.fnOutputs[i]
              .add(out == ISupportStreamingModeForWindowing.NULL_RESULT ? null
                  : out);
        }
      } else {
        int rowToProcess = streamingState.rollingPart.rowToProcess(wFn);
        if (rowToProcess >= 0) {
          Range rng = getRange(wFn, rowToProcess, streamingState.rollingPart,
              streamingState.order);
          PTFPartitionIterator<Object> rItr = rng.iterator();
          PTFOperator.connectLeadLagFunctionsToPartition(ptfDesc, rItr);
          Object out = evaluateWindowFunction(wFn, rItr);
          streamingState.fnOutputs[i].add(out);
        }
      }
    }

    List<Object> oRows = new ArrayList<Object>();
    while (true) {
      boolean hasRow = streamingState.hasOutputRow();

      if (!hasRow) {
        break;
      }

      oRows.add(streamingState.nextOutputRow());
    }

    return oRows.size() == 0 ? null : oRows;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hive.ql.udf.ptf.TableFunctionEvaluator#finishPartition()
   * 
   * for fns that are not ISupportStreamingModeForWindowing give them the
   * remaining rows (rows whose span went beyond the end of the partition) for
   * rest of the functions invoke terminate.
   * 
   * while numOutputRows < numInputRows for each Fn that doesn't have enough o/p
   * invoke getNextObj if there is no O/p then flag this as an error.
   */
  @Override
  public List<Object> finishPartition() throws HiveException {

    /*
     * Once enough rows have been output, there is no need to generate more output.
     */
    if ( streamingState.rankLimitReached() ) {
      return null;
    }

    WindowTableFunctionDef tabDef = (WindowTableFunctionDef) getTableDef();
    for (int i = 0; i < tabDef.getWindowFunctions().size(); i++) {
      WindowFunctionDef wFn = tabDef.getWindowFunctions().get(i);
      GenericUDAFEvaluator fnEval = wFn.getWFnEval();

      int numRowsRemaining = wFn.getWindowFrame().getEnd().getAmt();
      if (fnEval instanceof ISupportStreamingModeForWindowing) {
        fnEval.terminate(streamingState.aggBuffers[i]);

        WindowingFunctionInfoHelper wFnInfo = getWindowingFunctionInfoHelper(wFn.getName());
        if (!wFnInfo.isSupportsWindow()) {
          numRowsRemaining = ((ISupportStreamingModeForWindowing) fnEval)
              .getRowsRemainingAfterTerminate();
        }

        if (numRowsRemaining != BoundarySpec.UNBOUNDED_AMOUNT) {
          while (numRowsRemaining > 0) {
            Object out = ((ISupportStreamingModeForWindowing) fnEval)
                .getNextResult(streamingState.aggBuffers[i]);
            if (out != null) {
              streamingState.fnOutputs[i]
                  .add(out == ISupportStreamingModeForWindowing.NULL_RESULT ? null
                      : out);
            }
            numRowsRemaining--;
          }
        }
      } else {
        while (numRowsRemaining > 0) {
          int rowToProcess = streamingState.rollingPart.size()
              - numRowsRemaining;
          Range rng = getRange(wFn, rowToProcess, streamingState.rollingPart,
              streamingState.order);
          PTFPartitionIterator<Object> rItr = rng.iterator();
          PTFOperator.connectLeadLagFunctionsToPartition(ptfDesc, rItr);
          Object out = evaluateWindowFunction(wFn, rItr);
          streamingState.fnOutputs[i].add(out);
          numRowsRemaining--;
        }
      }

    }

    List<Object> oRows = new ArrayList<Object>();

    while (!streamingState.rollingPart.processedAllRows() && 
        !streamingState.rankLimitReached() ) {
      boolean hasRow = streamingState.hasOutputRow();

      if (!hasRow && !streamingState.rankLimitReached() ) {
        throw new HiveException(
            "Internal Error: cannot generate all output rows for a Partition");
      }
      if ( hasRow ) {
        oRows.add(streamingState.nextOutputRow());
      } 
    }

    return oRows.size() == 0 ? null : oRows;
  }

  @Override
  public boolean canIterateOutput() {
    return true;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public Iterator<Object> iterator(PTFPartitionIterator<Object> pItr) throws HiveException {
    WindowTableFunctionDef wTFnDef = (WindowTableFunctionDef) getTableDef();
    ArrayList<Object> output = new ArrayList<Object>();
    List<?>[] outputFromPivotFunctions = new List<?>[wTFnDef.getWindowFunctions().size()];
    ArrayList<Integer> wFnsWithWindows = new ArrayList<Integer>();
    PTFPartition iPart = pItr.getPartition();

    int i=0;
    for(WindowFunctionDef wFn : wTFnDef.getWindowFunctions()) {
      boolean processWindow = processWindow(wFn);
      pItr.reset();
      if ( !processWindow && !wFn.isPivotResult() ) {
        Object out = evaluateWindowFunction(wFn, pItr);
        output.add(out);
      } else if (wFn.isPivotResult()) {
        GenericUDAFEvaluator streamingEval = wFn.getWFnEval().getWindowingEvaluator(wFn.getWindowFrame());
        if ( streamingEval != null && streamingEval instanceof ISupportStreamingModeForWindowing ) {
          ISupportStreamingModeForWindowing strEval = (ISupportStreamingModeForWindowing) streamingEval;
          if ( strEval.getRowsRemainingAfterTerminate() == 0 ) {
            wFn.setWFnEval(streamingEval);
            if ( wFn.getOI() instanceof ListObjectInspector ) {
              ListObjectInspector listOI = (ListObjectInspector) wFn.getOI();
              wFn.setOI(listOI.getListElementObjectInspector());
            }
            output.add(null);
            wFnsWithWindows.add(i);
          } else {
            outputFromPivotFunctions[i] = (List) evaluateWindowFunction(wFn,
                pItr);
            output.add(null);
          }
        } else {
          outputFromPivotFunctions[i] = (List) evaluateWindowFunction(wFn, pItr);
          output.add(null);
        }
      } else {
        output.add(null);
        wFnsWithWindows.add(i);
      }
      i++;
    }

    for(i=0; i < iPart.getOutputOI().getAllStructFieldRefs().size(); i++) {
      output.add(null);
    }

    if ( wTFnDef.getRankLimit() != -1 ) {
      rnkLimitDef = new RankLimit(wTFnDef.getRankLimit(), 
          wTFnDef.getRankLimitFunction(), wTFnDef.getWindowFunctions());
    }

    return new WindowingIterator(iPart, output, outputFromPivotFunctions,
        ArrayUtils.toPrimitive(wFnsWithWindows.toArray(new Integer[wFnsWithWindows.size()])));
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
    for(int i=0; i < iPart.size(); i++) {
      Range rng = getRange(wFnDef, i, iPart, order);
      PTFPartitionIterator<Object> rItr = rng.iterator();
      PTFOperator.connectLeadLagFunctionsToPartition(ptfDesc, rItr);
      Object out = evaluateWindowFunction(wFnDef, rItr);
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
      case DECIMAL:
        return new HiveDecimalValueBoundaryScanner(vbDef, order, vbDef.getExpressionDef());
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

  public static class HiveDecimalValueBoundaryScanner extends ValueBoundaryScanner {
    public HiveDecimalValueBoundaryScanner(BoundaryDef bndDef, Order order,
        PTFExpressionDef expressionDef) {
      super(bndDef,order,expressionDef);
    }

    @Override
    public boolean isGreater(Object v1, Object v2, int amt) {
      HiveDecimal d1 = PrimitiveObjectInspectorUtils.getHiveDecimal(v1,
          (PrimitiveObjectInspector) expressionDef.getOI());
      HiveDecimal d2 = PrimitiveObjectInspectorUtils.getHiveDecimal(v2,
          (PrimitiveObjectInspector) expressionDef.getOI());
      if ( d1 == null || d2 == null ) {
        return false;
      }
      return d1.subtract(d2).intValue() > amt;
    }

    @Override
    public boolean isEqual(Object v1, Object v2) {
      HiveDecimal d1 = PrimitiveObjectInspectorUtils.getHiveDecimal(v1,
          (PrimitiveObjectInspector) expressionDef.getOI());
      HiveDecimal d2 = PrimitiveObjectInspectorUtils.getHiveDecimal(v2,
          (PrimitiveObjectInspector) expressionDef.getOI());
      if ( d1 == null || d2 == null ) {
        return false;
      }
      return d1.equals(d2);
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
      return (s1 == null && s2 == null) || (s1 != null && s1.equals(s2));
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

  public class WindowingIterator implements Iterator<Object> {

    ArrayList<Object> output;
    List<?>[] outputFromPivotFunctions;
    int currIdx;
    PTFPartition iPart;
    /*
     * these are the functions that have a Window.
     * Fns w/o a Window have already been processed.
     */
    int[] wFnsToProcess;
    WindowTableFunctionDef wTFnDef;
    Order order;
    PTFDesc ptfDesc;
    StructObjectInspector inputOI;
    AggregationBuffer[] aggBuffers;
    Object[][] args;
    RankLimit rnkLimit;

    WindowingIterator(PTFPartition iPart, ArrayList<Object> output,
        List<?>[] outputFromPivotFunctions, int[] wFnsToProcess) {
      this.iPart = iPart;
      this.output = output;
      this.outputFromPivotFunctions = outputFromPivotFunctions;
      this.wFnsToProcess = wFnsToProcess;
      this.currIdx = 0;
      wTFnDef = (WindowTableFunctionDef) getTableDef();
      order = wTFnDef.getOrder().getExpressions().get(0).getOrder();
      ptfDesc = getQueryDef();
      inputOI = iPart.getOutputOI();

      aggBuffers = new AggregationBuffer[wTFnDef.getWindowFunctions().size()];
      args = new Object[wTFnDef.getWindowFunctions().size()][];
      try {
        for (int j : wFnsToProcess) {
          WindowFunctionDef wFn = wTFnDef.getWindowFunctions().get(j);
          aggBuffers[j] = wFn.getWFnEval().getNewAggregationBuffer();
          args[j] = new Object[wFn.getArgs() == null ? 0 : wFn.getArgs().size()];
        }
      } catch (HiveException he) {
        throw new RuntimeException(he);
      }
      if ( WindowingTableFunction.this.rnkLimitDef != null ) {
        rnkLimit = new RankLimit(WindowingTableFunction.this.rnkLimitDef);
      }
    }

    @Override
    public boolean hasNext() {

      if ( rnkLimit != null && rnkLimit.limitReached() ) {
        return false;
      }
      return currIdx < iPart.size();
    }

    @Override
    public Object next() {
      int i;
      for(i = 0; i < outputFromPivotFunctions.length; i++ ) {
        if ( outputFromPivotFunctions[i] != null ) {
          output.set(i, outputFromPivotFunctions[i].get(currIdx));
        }
      }

      try {
        for (int j : wFnsToProcess) {
          WindowFunctionDef wFn = wTFnDef.getWindowFunctions().get(j);
          if (wFn.getWFnEval() instanceof ISupportStreamingModeForWindowing) {
            Object iRow = iPart.getAt(currIdx);
            int a = 0;
            if (wFn.getArgs() != null) {
              for (PTFExpressionDef arg : wFn.getArgs()) {
                args[j][a++] = arg.getExprEvaluator().evaluate(iRow);
              }
            }
            wFn.getWFnEval().aggregate(aggBuffers[j], args[j]);
            Object out = ((ISupportStreamingModeForWindowing) wFn.getWFnEval())
                .getNextResult(aggBuffers[j]);
            out = ObjectInspectorUtils.copyToStandardObject(out, wFn.getOI());
            output.set(j, out);
          } else {
            Range rng = getRange(wFn, currIdx, iPart, order);
            PTFPartitionIterator<Object> rItr = rng.iterator();
            PTFOperator.connectLeadLagFunctionsToPartition(ptfDesc, rItr);
            output.set(j, evaluateWindowFunction(wFn, rItr));
          }
        }

        Object iRow = iPart.getAt(currIdx);
        i = wTFnDef.getWindowFunctions().size();
        for (StructField f : inputOI.getAllStructFieldRefs()) {
          output.set(i++, inputOI.getStructFieldData(iRow, f));
        }

      } catch (HiveException he) {
        throw new RuntimeException(he);
      }

      if ( rnkLimit != null ) {
        rnkLimit.updateRank(output);
      }
      currIdx++;
      return output;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

  }

  class StreamingState {
    PTFRollingPartition rollingPart;
    List<Object>[] fnOutputs;
    AggregationBuffer[] aggBuffers;
    Object[][] funcArgs;
    Order order;
    RankLimit rnkLimit;

    @SuppressWarnings("unchecked")
    StreamingState(Configuration cfg, StructObjectInspector inputOI,
        boolean isMapSide, WindowTableFunctionDef tabDef, int precedingSpan,
        int followingSpan) throws HiveException {
      SerDe serde = isMapSide ? tabDef.getInput().getOutputShape().getSerde()
          : tabDef.getRawInputShape().getSerde();
      StructObjectInspector outputOI = isMapSide ? tabDef.getInput()
          .getOutputShape().getOI() : tabDef.getRawInputShape().getOI();
      rollingPart = PTFPartition.createRolling(cfg, serde, inputOI, outputOI,
          precedingSpan, followingSpan);

      order = tabDef.getOrder().getExpressions().get(0).getOrder();

      int numFns = tabDef.getWindowFunctions().size();
      fnOutputs = new ArrayList[numFns];

      aggBuffers = new AggregationBuffer[numFns];
      funcArgs = new Object[numFns][];
      for (int i = 0; i < numFns; i++) {
        fnOutputs[i] = new ArrayList<Object>();
        WindowFunctionDef wFn = tabDef.getWindowFunctions().get(i);
        funcArgs[i] = new Object[wFn.getArgs() == null ? 0 : wFn.getArgs().size()];
        aggBuffers[i] = wFn.getWFnEval().getNewAggregationBuffer();
      }
      if ( WindowingTableFunction.this.rnkLimitDef != null ) {
        rnkLimit = new RankLimit(WindowingTableFunction.this.rnkLimitDef);
      }
    }

    void reset(WindowTableFunctionDef tabDef) throws HiveException {
      int numFns = tabDef.getWindowFunctions().size();
      rollingPart.reset();
      for (int i = 0; i < fnOutputs.length; i++) {
        fnOutputs[i].clear();
      }

      for (int i = 0; i < numFns; i++) {
        WindowFunctionDef wFn = tabDef.getWindowFunctions().get(i);
        aggBuffers[i] = wFn.getWFnEval().getNewAggregationBuffer();
      }

      if ( rnkLimit != null ) {
        rnkLimit.reset();
      }
    }

    boolean hasOutputRow() {
      if ( rankLimitReached() ) {
        return false;
      }

      for (int i = 0; i < fnOutputs.length; i++) {
        if (fnOutputs[i].size() == 0) {
          return false;
        }
      }
      return true;
    }

    List<Object> nextOutputRow() throws HiveException {
      List<Object> oRow = new ArrayList<Object>();
      Object iRow = rollingPart.nextOutputRow();
      int i = 0;
      for (; i < fnOutputs.length; i++) {
        oRow.add(fnOutputs[i].remove(0));
      }
      for (StructField f : rollingPart.getOutputOI().getAllStructFieldRefs()) {
        oRow.add(rollingPart.getOutputOI().getStructFieldData(iRow, f));
      }
      if ( rnkLimit != null ) {
        rnkLimit.updateRank(oRow);
      }
      return oRow;
    }

    boolean rankLimitReached() {
      return rnkLimit != null  && rnkLimit.limitReached();
    }
  }
  
  static class RankLimit {
    
    /*
     * Rows with a rank <= rankLimit are output.
     * Only the first row with rank = rankLimit is output.
     */
    final int rankLimit;
    
    /*
     * the rankValue of the last row output.
     */
    int currentRank;
    
    /*
     * index of Rank function.
     */
    final int rankFnIdx;
    
    final PrimitiveObjectInspector fnOutOI;
    
    RankLimit(int rankLimit, int rankFnIdx, List<WindowFunctionDef> wdwFnDefs) {
      this.rankLimit = rankLimit;
      this.rankFnIdx = rankFnIdx;
      this.fnOutOI = (PrimitiveObjectInspector) wdwFnDefs.get(rankFnIdx).getOI();
      this.currentRank = -1;
    }
    
    RankLimit(RankLimit rl) {
      this.rankLimit = rl.rankLimit;
      this.rankFnIdx = rl.rankFnIdx;
      this.fnOutOI = rl.fnOutOI;
      this.currentRank = -1;
    }
    
    void reset() {
      this.currentRank = -1;
    }
    
    void updateRank(List<Object> oRow) {
      int r = (Integer) fnOutOI.getPrimitiveJavaObject(oRow.get(rankFnIdx));
      if ( r > currentRank ) {
        currentRank = r;
      }
    }
    
    boolean limitReached() {
      return currentRank >= rankLimit;
    }
  }

}
