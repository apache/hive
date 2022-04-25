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

package org.apache.hadoop.hive.ql.udf.generic;

import static java.util.Arrays.asList;
import static org.apache.hadoop.hive.ql.util.DirectionUtils.ASCENDING_CODE;
import static org.apache.hadoop.hive.ql.util.DirectionUtils.DESCENDING_CODE;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.
        writableLongObjectInspector;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.ql.util.NullOrdering;
import org.apache.hadoop.hive.serde2.objectinspector.FullMapEqualComparer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.WindowFunctionDescription;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

@Description(
        name = "rank",
        value = "_FUNC_(x)")
@WindowFunctionDescription(
        supportsWindow = false,
        pivotResult = true,
        rankingFunction = true,
        orderedAggregate = true)
public class GenericUDAFRank extends AbstractGenericUDAFResolver {

  static final Logger LOG = LoggerFactory.getLogger(GenericUDAFRank.class.getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
    if (info.isWindowing()) {
      return getWindowingEvaluator(info.getParameterObjectInspectors());
    }
    return getHypotheticalSetEvaluator(info.getParameterObjectInspectors());
  }

  private GenericUDAFEvaluator getWindowingEvaluator(ObjectInspector[] parameters) throws SemanticException {
    if (parameters.length < 1) {
      throw new UDFArgumentTypeException(parameters.length - 1,
        "One or more arguments are expected.");
    }
    for (int i = 0; i < parameters.length; i++) {
      supportsCompare(parameters, i);
    }
    return createWindowingEvaluator();
  }

  protected GenericUDAFAbstractRankEvaluator createWindowingEvaluator() {
    return new GenericUDAFRankEvaluator();
  }

  private GenericUDAFEvaluator getHypotheticalSetEvaluator(ObjectInspector[] parameters) throws SemanticException {
    if (parameters.length % 4 != 0) {
      throw new UDFArgumentTypeException(parameters.length,
              "Invalid number of parameters: " +
                      "the number of hypothetical direct arguments must match the number of ordering columns");
    }

    for (int i = 0; i < parameters.length / 4; ++i) {
      supportsCompare(parameters, 4 * i);
      supportsCompare(parameters, 4 * i + 1);
    }

    return createHypotheticalSetEvaluator();
  }

  protected GenericUDAFHypotheticalSetRankEvaluator createHypotheticalSetEvaluator() {
    return new GenericUDAFHypotheticalSetRankEvaluator();
  }

  private void supportsCompare(ObjectInspector[] parameters, int i2) throws UDFArgumentTypeException {
    ObjectInspector oi = parameters[i2];
    if (!ObjectInspectorUtils.compareSupported(oi)) {
      throw new UDFArgumentTypeException(i2,
              "Cannot support comparison of map<> type or complex type containing map<>.");
    }
  }

  static class RankBuffer implements AggregationBuffer {

    ArrayList<IntWritable> rowNums;
    int currentRowNum;
    Object[] currVal;
    int currentRank;
    int numParams;
    boolean supportsStreaming;

    RankBuffer(int numParams, boolean supportsStreaming) {
      this.numParams = numParams;
      this.supportsStreaming = supportsStreaming;
      init();
    }

    void init() {
      rowNums = new ArrayList<IntWritable>();
      currentRowNum = 0;
      currentRank = 0;
      currVal = new Object[numParams];
      if (supportsStreaming) {
        /* initialize rowNums to have 1 row */
        rowNums.add(null);
      }
    }

    void incrRowNum() { currentRowNum++; }

    void addRank() {
      if (supportsStreaming) {
        rowNums.set(0, new IntWritable(currentRank));
      } else {
        rowNums.add(new IntWritable(currentRank));
      }
    }
  }

  public static abstract class GenericUDAFAbstractRankEvaluator extends GenericUDAFEvaluator {

    ObjectInspector[] inputOI;
    ObjectInspector[] outputOI;
    boolean isStreamingMode = false;

    protected boolean isStreaming() {
      return isStreamingMode;
    }

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);
      if (m != Mode.COMPLETE) {
        throw new HiveException("Only COMPLETE mode supported for Rank function");
      }
      inputOI = parameters;
      outputOI = new ObjectInspector[inputOI.length];
      for (int i = 0; i < inputOI.length; i++) {
        outputOI[i] = ObjectInspectorUtils.getStandardObjectInspector(inputOI[i],
          ObjectInspectorCopyOption.JAVA);
      }
      return ObjectInspectorFactory.getStandardListObjectInspector(
        PrimitiveObjectInspectorFactory.writableIntObjectInspector);
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      return new RankBuffer(inputOI.length, isStreamingMode);
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((RankBuffer) agg).init();
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      RankBuffer rb = (RankBuffer) agg;
      int c = GenericUDAFRank.compare(rb.currVal, outputOI, parameters, inputOI);
      rb.incrRowNum();
      if (rb.currentRowNum == 1 || c != 0) {
        nextRank(rb);
        rb.currVal =
          GenericUDAFRank.copyToStandardObject(parameters, inputOI, ObjectInspectorCopyOption.JAVA);
      }
      rb.addRank();
    }

    /*
     * Called when the value in the partition has changed. Update the currentRank
     */
    protected void nextRank(RankBuffer rb) {
      rb.currentRank = rb.currentRowNum;
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      throw new HiveException("terminatePartial not supported");
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      throw new HiveException("merge not supported");
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      return ((RankBuffer) agg).rowNums;
    }

  }

  public static class GenericUDAFRankEvaluator extends GenericUDAFAbstractRankEvaluator
    implements ISupportStreamingModeForWindowing {

    @Override
    public Object getNextResult(AggregationBuffer agg) throws HiveException {
      return ((RankBuffer) agg).rowNums.get(0);
    }

    @Override
    public GenericUDAFEvaluator getWindowingEvaluator(WindowFrameDef wFrmDef) {
      isStreamingMode = true;
      return this;
    }

    @Override
    public int getRowsRemainingAfterTerminate() throws HiveException {
      return 0;
    }
  }

  public static int compare(Object[] o1, ObjectInspector[] oi1, Object[] o2,
    ObjectInspector[] oi2) {
    int c = 0;
    for (int i = 0; i < oi1.length; i++) {
      c = ObjectInspectorUtils.compare(o1[i], oi1[i], o2[i], oi2[i]);
      if (c != 0) {
        return c;
      }
    }
    return c;
  }

  public static Object[] copyToStandardObject(Object[] o, ObjectInspector[] oi,
    ObjectInspectorCopyOption objectInspectorOption) {
    Object[] out = new Object[o.length];
    for (int i = 0; i < oi.length; i++) {
      out[i] = ObjectInspectorUtils.copyToStandardObject(o[i], oi[i], objectInspectorOption);
    }
    return out;
  }



  /**
   *  Hypothetical rank calculation.
   *  Calculates the rank of a hypothetical row specified by the arguments of the
   *  function in a group of values specified by the order by clause.
   *  SELECT rank(expression1[, expressionn]*) WITHIN GROUP (ORDER BY col1[, coln]*)
   *  (the number of rows where col1 &lt; expression1 [and coln &lt; expressionn]*) + 1
   */
  public static class GenericUDAFHypotheticalSetRankEvaluator extends GenericUDAFEvaluator {
    public static final String RANK_FIELD = "rank";
    public static final String COUNT_FIELD = "count";
    public static final ObjectInspector PARTIAL_RANK_OI = ObjectInspectorFactory.getStandardStructObjectInspector(
            asList(RANK_FIELD, COUNT_FIELD),
            asList(writableLongObjectInspector,
                    writableLongObjectInspector));

    protected static class HypotheticalSetRankBuffer extends AbstractAggregationBuffer {
      protected long rank = 0;
      protected long rowCount = 0;

      @Override
      public int estimate() {
        return JavaDataModel.PRIMITIVES2 * 2;
      }
    }

    protected static class RankAssets {
      private final ObjectInspector commonInputOI;
      private final ObjectInspectorConverters.Converter directArgumentConverter;
      private final ObjectInspectorConverters.Converter inputConverter;
      protected final int order;
      private final NullOrdering nullOrdering;

      public RankAssets(ObjectInspector commonInputOI,
                        ObjectInspectorConverters.Converter directArgumentConverter,
                        ObjectInspectorConverters.Converter inputConverter,
                        int order, NullOrdering nullOrdering) {
        this.commonInputOI = commonInputOI;
        this.directArgumentConverter = directArgumentConverter;
        this.inputConverter = inputConverter;
        this.order = order;
        this.nullOrdering = nullOrdering;
      }

      public int compare(Object inputValue, Object directArgumentValue) {
        return ObjectInspectorUtils.compare(inputConverter.convert(inputValue), commonInputOI,
                directArgumentConverter.convert(directArgumentValue), commonInputOI,
                new FullMapEqualComparer(), nullOrdering.getNullValueOption());
      }
    }

    public GenericUDAFHypotheticalSetRankEvaluator() {
      this(false, PARTIAL_RANK_OI, writableLongObjectInspector);
    }

    public GenericUDAFHypotheticalSetRankEvaluator(
            boolean allowEquality, ObjectInspector partialOutputOI, ObjectInspector finalOI) {
      this.allowEquality = allowEquality;
      this.partialOutputOI = partialOutputOI;
      this.finalOI = finalOI;
    }

    private final transient boolean allowEquality;
    private final transient ObjectInspector partialOutputOI;
    private final transient ObjectInspector finalOI;
    private transient List<RankAssets> rankAssetsList;
    private transient StructObjectInspector partialInputOI;
    private transient StructField partialInputRank;
    private transient StructField partialInputCount;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);

      if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
        rankAssetsList = new ArrayList<>(parameters.length / 4);
        for (int i = 0; i < parameters.length / 4; ++i) {
          TypeInfo directArgumentType = TypeInfoUtils.getTypeInfoFromObjectInspector(parameters[4 * i]);
          TypeInfo inputType = TypeInfoUtils.getTypeInfoFromObjectInspector(parameters[4 * i + 1]);
          TypeInfo commonTypeInfo = FunctionRegistry.getCommonClassForComparison(inputType, directArgumentType);
          ObjectInspector commonInputOI = TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(commonTypeInfo);
          rankAssetsList.add(new RankAssets(
                  commonInputOI,
                  ObjectInspectorConverters.getConverter(parameters[4 * i], commonInputOI),
                  ObjectInspectorConverters.getConverter(parameters[4 * i + 1], commonInputOI),
                  ((WritableConstantIntObjectInspector) parameters[4 * i + 2]).
                          getWritableConstantValue().get(),
                  NullOrdering.fromCode(((WritableConstantIntObjectInspector) parameters[4 * i + 3]).
                          getWritableConstantValue().get())));
        }
      } else {
        initPartial2AndFinalOI(parameters);
      }

      if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
        return partialOutputOI;
      }

      return finalOI;
    }

    protected void initPartial2AndFinalOI(ObjectInspector[] parameters) {
      partialInputOI = (StructObjectInspector) parameters[0];
      partialInputRank = partialInputOI.getStructFieldRef(RANK_FIELD);
      partialInputCount = partialInputOI.getStructFieldRef(COUNT_FIELD);
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      return new HypotheticalSetRankBuffer();
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      HypotheticalSetRankBuffer rankBuffer = (HypotheticalSetRankBuffer) agg;
      rankBuffer.rank = 0;
      rankBuffer.rowCount = 0;
    }

    protected static class CompareResult {
      private final int compareResult;
      private final int order;

      public CompareResult(int compareResult, int order) {
        this.compareResult = compareResult;
        this.order = order;
      }

      public int getCompareResult() {
        return compareResult;
      }

      public int getOrder() {
        return order;
      }
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      HypotheticalSetRankBuffer rankBuffer = (HypotheticalSetRankBuffer) agg;
      rankBuffer.rowCount++;

      CompareResult compareResult = compare(parameters);

      if (compareResult.getCompareResult() == 0) {
        if (allowEquality) {
          rankBuffer.rank++;
        }
        return;
      }

      if (compareResult.getOrder() == ASCENDING_CODE && compareResult.getCompareResult() < 0 ||
              compareResult.getOrder() == DESCENDING_CODE && compareResult.getCompareResult() > 0) {
        rankBuffer.rank++;
      }
    }

    protected CompareResult compare(Object[] parameters) {
      int i = 0;
      int c = 0;
      for (RankAssets rankAssets : rankAssetsList) {
        c = rankAssets.compare(parameters[4 * i + 1], parameters[4 * i]);
        if (c != 0) {
          break;
        }
        ++i;
      }

      if (c == 0) {
        return new CompareResult(c, -1);
      }

      return new CompareResult(c, rankAssetsList.get(i).order);
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      HypotheticalSetRankBuffer rankBuffer = (HypotheticalSetRankBuffer) agg;
      LongWritable[] result = new LongWritable[2];
      result[0] = new LongWritable(rankBuffer.rank + 1);
      result[1] = new LongWritable(rankBuffer.rowCount);
      return result;
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (partial == null) {
        return;
      }

      Object objRank = partialInputOI.getStructFieldData(partial, partialInputRank);
      Object objCount = partialInputOI.getStructFieldData(partial, partialInputCount);

      HypotheticalSetRankBuffer rankBuffer = (HypotheticalSetRankBuffer) agg;
      rankBuffer.rank += ((LongWritable)objRank).get() - 1;
      rankBuffer.rowCount += ((LongWritable)objCount).get();
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      HypotheticalSetRankBuffer rankBuffer = (HypotheticalSetRankBuffer) agg;
      return new LongWritable(rankBuffer.rank + 1);
    }
  }
}

