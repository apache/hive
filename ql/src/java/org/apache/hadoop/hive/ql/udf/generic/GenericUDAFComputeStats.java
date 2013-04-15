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
package org.apache.hadoop.hive.ql.udf.generic;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;

/**
 * GenericUDAFComputeStats
 *
 */
@Description(name = "compute_stats",
      value = "_FUNC_(x) - Returns the statistical summary of a set of primitive type values.")
public class GenericUDAFComputeStats extends AbstractGenericUDAFResolver {

  static final Log LOG = LogFactory.getLog(GenericUDAFComputeStats.class.getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {
    if (parameters.length != 2 ) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Exactly two arguments are expected.");
    }

    if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0,
          "Only primitive type arguments are accepted but "
          + parameters[0].getTypeName() + " is passed.");
    }
    switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
    case BOOLEAN:
      return new GenericUDAFBooleanStatsEvaluator();
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
    case TIMESTAMP:
      return new GenericUDAFLongStatsEvaluator();
    case FLOAT:
    case DOUBLE:
      return new GenericUDAFDoubleStatsEvaluator();
    case STRING:
      return new GenericUDAFStringStatsEvaluator();
    case BINARY:
      return new GenericUDAFBinaryStatsEvaluator();
    default:
      throw new UDFArgumentTypeException(0,
          "Only integer/long/timestamp/float/double/string/binary/boolean type argument " +
          "is accepted but "
          + parameters[0].getTypeName() + " is passed.");
    }
  }

  /**
   * GenericUDAFBooleanStatsEvaluator.
   *
   */
  public static class GenericUDAFBooleanStatsEvaluator extends GenericUDAFEvaluator {

    /* Object Inspector corresponding to the input parameter.
     */
    private PrimitiveObjectInspector inputOI;

    /* Partial aggregation result returned by TerminatePartial. Partial result is a struct
     * containing a long field named "count".
     */
    private Object[] partialResult;

    /* Object Inspectors corresponding to the struct returned by TerminatePartial and the long
     * field within the struct - "count"
     */
    private StructObjectInspector soi;

    private StructField columnTypeField;
    private WritableStringObjectInspector columnTypeFieldOI;

    private StructField countTruesField;
    private WritableLongObjectInspector countTruesFieldOI;

    private StructField countFalsesField;
    private WritableLongObjectInspector countFalsesFieldOI;

    private StructField countNullsField;
    private WritableLongObjectInspector countNullsFieldOI;

    /* Output of final result of the aggregation
     */
    private Object[] result;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
       super.init(m, parameters);

      // initialize input
      if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
        inputOI = (PrimitiveObjectInspector) parameters[0];
      } else {
        soi = (StructObjectInspector) parameters[0];

        columnTypeField = soi.getStructFieldRef("ColumnType");
        columnTypeFieldOI = (WritableStringObjectInspector)
                              columnTypeField.getFieldObjectInspector();

        countTruesField = soi.getStructFieldRef("CountTrues");
        countTruesFieldOI = (WritableLongObjectInspector)
                               countTruesField.getFieldObjectInspector();

        countFalsesField = soi.getStructFieldRef("CountFalses");
        countFalsesFieldOI = (WritableLongObjectInspector)
                                countFalsesField.getFieldObjectInspector();

        countNullsField = soi.getStructFieldRef("CountNulls");
        countNullsFieldOI = (WritableLongObjectInspector) countNullsField.getFieldObjectInspector();
      }

      // initialize output
      List<ObjectInspector> foi = new ArrayList<ObjectInspector>();
      foi.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
      foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
      foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
      foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);

      List<String> fname = new ArrayList<String>();
      fname.add("ColumnType");
      fname.add("CountTrues");
      fname.add("CountFalses");
      fname.add("CountNulls");

      partialResult = new Object[4];
      partialResult[0] = new Text();
      partialResult[1] = new LongWritable(0);
      partialResult[2] = new LongWritable(0);
      partialResult[3] = new LongWritable(0);

      result = new Object[4];
      result[0] = new Text();
      result[1] = new LongWritable(0);
      result[2] = new LongWritable(0);
      result[3] = new LongWritable(0);

      return ObjectInspectorFactory.getStandardStructObjectInspector(fname,
            foi);
    }

    public static class BooleanStatsAgg implements AggregationBuffer {
      public String columnType;                        /* Datatype of column */
      public long countTrues;  /* Count of number of true values seen so far */
      public long countFalses; /* Count of number of false values seen so far */
      public long countNulls;  /* Count of number of null values seen so far */
    };

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      BooleanStatsAgg result = new BooleanStatsAgg();
      reset(result);
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      BooleanStatsAgg myagg = (BooleanStatsAgg) agg;
      myagg.columnType = new String("Boolean");
      myagg.countTrues = 0;
      myagg.countFalses = 0;
      myagg.countNulls = 0;
    }

    private void printDebugOutput(String functionName, AggregationBuffer agg) {
      BooleanStatsAgg myagg = (BooleanStatsAgg) agg;

      LOG.debug(functionName);

      LOG.debug("Count of True Values:");
      LOG.debug(myagg.countTrues);

      LOG.debug("Count of False Values:");
      LOG.debug(myagg.countFalses);

      LOG.debug("Count of Null Values:");
      LOG.debug(myagg.countNulls);
    }

    boolean warned = false;

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      Object p = parameters[0];
      BooleanStatsAgg myagg = (BooleanStatsAgg) agg;
      boolean emptyTable = false;

      if (parameters[1] == null) {
        emptyTable = true;
      }

      if (!emptyTable) {
        if (p == null) {
          myagg.countNulls++;
        }
        else {
          try {
            boolean v = PrimitiveObjectInspectorUtils.getBoolean(p, inputOI);
            if (v == false) {
              myagg.countFalses++;
            } else if (v == true){
              myagg.countTrues++;
            }
          } catch (NumberFormatException e) {
            if (!warned) {
              warned = true;
              LOG.warn(getClass().getSimpleName() + " "
                  + StringUtils.stringifyException(e));
              LOG.warn(getClass().getSimpleName()
                  + " ignoring similar exceptions.");
            }
          }
        }
      }
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      BooleanStatsAgg myagg = (BooleanStatsAgg) agg;
      ((Text) partialResult[0]).set(myagg.columnType);
      ((LongWritable) partialResult[1]).set(myagg.countTrues);
      ((LongWritable) partialResult[2]).set(myagg.countFalses);
      ((LongWritable) partialResult[3]).set(myagg.countNulls);
      return partialResult;
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (partial != null) {
        BooleanStatsAgg myagg = (BooleanStatsAgg) agg;

        Object partialCount = soi.getStructFieldData(partial, countTruesField);
        myagg.countTrues += countTruesFieldOI.get(partialCount);

        partialCount = soi.getStructFieldData(partial, countFalsesField);
        myagg.countFalses += countFalsesFieldOI.get(partialCount);

        partialCount = soi.getStructFieldData(partial, countNullsField);
        myagg.countNulls += countNullsFieldOI.get(partialCount);
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      BooleanStatsAgg myagg = (BooleanStatsAgg) agg;
      ((Text)result[0]).set(myagg.columnType);
      ((LongWritable)result[1]).set(myagg.countTrues);
      ((LongWritable)result[2]).set(myagg.countFalses);
      ((LongWritable)result[3]).set(myagg.countNulls);
      return result;
    }
  }

  /**
   * GenericUDAFLongStatsEvaluator.
   *
   */
  public static class GenericUDAFLongStatsEvaluator extends GenericUDAFEvaluator {

    /* Object Inspector corresponding to the input parameter.
     */
    private PrimitiveObjectInspector inputOI;
    private PrimitiveObjectInspector numVectorsOI;

    /* Partial aggregation result returned by TerminatePartial. Partial result is a struct
     * containing a long field named "count".
     */
    private Object[] partialResult;

    /* Object Inspectors corresponding to the struct returned by TerminatePartial and the long
     * field within the struct - "count"
     */
    private StructObjectInspector soi;

    private StructField columnType;
    private WritableStringObjectInspector columnTypeFieldOI;

    private StructField minField;
    private WritableLongObjectInspector minFieldOI;

    private StructField maxField;
    private WritableLongObjectInspector maxFieldOI;

    private StructField countNullsField;
    private WritableLongObjectInspector countNullsFieldOI;

    private StructField ndvField;
    private WritableStringObjectInspector ndvFieldOI;

    private StructField numBitVectorsField;
    private WritableIntObjectInspector numBitVectorsFieldOI;

    /* Output of final result of the aggregation
     */
    private Object[] result;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);

      // initialize input
      if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
        inputOI = (PrimitiveObjectInspector) parameters[0];
        numVectorsOI = (PrimitiveObjectInspector) parameters[1];
      } else {
        soi = (StructObjectInspector) parameters[0];

        columnType = soi.getStructFieldRef("ColumnType");
        columnTypeFieldOI = (WritableStringObjectInspector) columnType.getFieldObjectInspector();

        minField = soi.getStructFieldRef("Min");
        minFieldOI = (WritableLongObjectInspector) minField.getFieldObjectInspector();

        maxField = soi.getStructFieldRef("Max");
        maxFieldOI = (WritableLongObjectInspector) maxField.getFieldObjectInspector();

        countNullsField = soi.getStructFieldRef("CountNulls");
        countNullsFieldOI = (WritableLongObjectInspector) countNullsField.getFieldObjectInspector();

        ndvField = soi.getStructFieldRef("BitVector");
        ndvFieldOI = (WritableStringObjectInspector) ndvField.getFieldObjectInspector();

        numBitVectorsField = soi.getStructFieldRef("NumBitVectors");
        numBitVectorsFieldOI = (WritableIntObjectInspector)
                                  numBitVectorsField.getFieldObjectInspector();
      }

      // initialize output
      if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
        List<ObjectInspector> foi = new ArrayList<ObjectInspector>();
        foi.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);

        List<String> fname = new ArrayList<String>();
        fname.add("ColumnType");
        fname.add("Min");
        fname.add("Max");
        fname.add("CountNulls");
        fname.add("BitVector");
        fname.add("NumBitVectors");

        partialResult = new Object[6];
        partialResult[0] = new Text();
        partialResult[1] = new LongWritable(0);
        partialResult[2] = new LongWritable(0);
        partialResult[3] = new LongWritable(0);
        partialResult[4] = new Text();
        partialResult[5] = new IntWritable(0);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fname,
          foi);
      } else {
        List<ObjectInspector> foi = new ArrayList<ObjectInspector>();
        foi.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);

        List<String> fname = new ArrayList<String>();
        fname.add("ColumnType");
        fname.add("Min");
        fname.add("Max");
        fname.add("CountNulls");
        fname.add("NumDistinctValues");

        result = new Object[5];
        result[0] = new Text();
        result[1] = new LongWritable(0);
        result[2] = new LongWritable(0);
        result[3] = new LongWritable(0);
        result[4] = new LongWritable(0);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fname,
            foi);
      }
    }

    public static class LongStatsAgg implements AggregationBuffer {
      public String columnType;
      public long min;                              /* Minimum value seen so far */
      public long max;                              /* Maximum value seen so far */
      public long countNulls;      /* Count of number of null values seen so far */
      public LongNumDistinctValueEstimator numDV;    /* Distinct value estimator */
      public boolean firstItem;                     /* First item in the aggBuf? */
      public int numBitVectors;
    };

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      LongStatsAgg result = new LongStatsAgg();
      reset(result);
      return result;
    }
    public void initNDVEstimator(LongStatsAgg aggBuffer, int numBitVectors) {
      aggBuffer.numDV = new LongNumDistinctValueEstimator(numBitVectors);
      aggBuffer.numDV.reset();
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      LongStatsAgg myagg = (LongStatsAgg) agg;
      myagg.columnType = new String("Long");
      myagg.min = 0;
      myagg.max = 0;
      myagg.countNulls = 0;
      myagg.firstItem = true;
    }

    boolean warned = false;

    private void printDebugOutput(String functionName, AggregationBuffer agg) {
      LongStatsAgg myagg = (LongStatsAgg) agg;

      LOG.debug(functionName);

      LOG.debug("Max Value:");
      LOG.debug(myagg.max);

      LOG.debug("Min Value:");
      LOG.debug(myagg.min);

      LOG.debug("Count of Null Values:");
      LOG.debug(myagg.countNulls);

      myagg.numDV.printNumDistinctValueEstimator();
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      Object p = parameters[0];
      LongStatsAgg myagg = (LongStatsAgg) agg;
      boolean emptyTable = false;

      if (parameters[1] == null) {
        emptyTable = true;
      }

      if (myagg.firstItem) {
        int numVectors = 0;
        if (!emptyTable) {
          numVectors = PrimitiveObjectInspectorUtils.getInt(parameters[1], numVectorsOI);
        }
        initNDVEstimator(myagg, numVectors);
        myagg.firstItem = false;
        myagg.numBitVectors = numVectors;
      }

      if (!emptyTable) {

      //Update null counter if a null value is seen
      if (p == null) {
        myagg.countNulls++;
      }
      else {
        try {
          long v = PrimitiveObjectInspectorUtils.getLong(p, inputOI);

          //Update min counter if new value is less than min seen so far
          if (v < myagg.min) {
            myagg.min = v;
          }

          //Update max counter if new value is greater than max seen so far
          if (v > myagg.max) {
            myagg.max = v;
          }

          // Add value to NumDistinctValue Estimator
          myagg.numDV.addToEstimator(v);

        } catch (NumberFormatException e) {
          if (!warned) {
            warned = true;
            LOG.warn(getClass().getSimpleName() + " "
                + StringUtils.stringifyException(e));
            LOG.warn(getClass().getSimpleName()
                + " ignoring similar exceptions.");
          }
        }
      }
      }
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      LongStatsAgg myagg = (LongStatsAgg) agg;

      // Serialize numDistinctValue Estimator
      Text t = myagg.numDV.serialize();

      // Serialize rest of the field in the AggBuffer
      ((Text) partialResult[0]).set(myagg.columnType);
      ((LongWritable) partialResult[1]).set(myagg.min);
      ((LongWritable) partialResult[2]).set(myagg.max);
      ((LongWritable) partialResult[3]).set(myagg.countNulls);
      ((Text) partialResult[4]).set(t);
      ((IntWritable) partialResult[5]).set(myagg.numDV.getnumBitVectors());

      return partialResult;
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (partial != null) {
        LongStatsAgg myagg = (LongStatsAgg) agg;

        if (myagg.firstItem) {
          Object partialValue = soi.getStructFieldData(partial, numBitVectorsField);
          int numVectors = numBitVectorsFieldOI.get(partialValue);
          initNDVEstimator(myagg, numVectors);
          myagg.firstItem = false;
          myagg.numBitVectors = numVectors;

        }

        // Update min if min is lesser than the smallest value seen so far
        Object partialValue = soi.getStructFieldData(partial, minField);
        if (myagg.min > minFieldOI.get(partialValue)) {
          myagg.min = minFieldOI.get(partialValue);
        }

        // Update max if max is greater than the largest value seen so far
        partialValue = soi.getStructFieldData(partial, maxField);
        if (myagg.max < maxFieldOI.get(partialValue)) {
          myagg.max = maxFieldOI.get(partialValue);
        }

        // Update the null counter
        partialValue = soi.getStructFieldData(partial, countNullsField);
        myagg.countNulls += countNullsFieldOI.get(partialValue);

        // Merge numDistinctValue Estimators
        partialValue = soi.getStructFieldData(partial, ndvField);
        String v = ndvFieldOI.getPrimitiveJavaObject(partialValue);
        NumDistinctValueEstimator o = new NumDistinctValueEstimator(v, myagg.numBitVectors);
        myagg.numDV.mergeEstimators(o);
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      LongStatsAgg myagg = (LongStatsAgg) agg;

      long numDV = 0;
      if (myagg.numBitVectors != 0) {
        numDV = myagg.numDV.estimateNumDistinctValues();
      }

      // Serialize the result struct
      ((Text) result[0]).set(myagg.columnType);
      ((LongWritable) result[1]).set(myagg.min);
      ((LongWritable) result[2]).set(myagg.max);
      ((LongWritable) result[3]).set(myagg.countNulls);
      ((LongWritable) result[4]).set(numDV);

      return result;
    }
  }

  /**
   * GenericUDAFDoubleStatsEvaluator.
   *
   */
  public static class GenericUDAFDoubleStatsEvaluator extends GenericUDAFEvaluator {

    /* Object Inspector corresponding to the input parameter.
     */
    private PrimitiveObjectInspector inputOI;
    private PrimitiveObjectInspector numVectorsOI;

    /* Partial aggregation result returned by TerminatePartial. Partial result is a struct
     * containing a long field named "count".
     */
    private Object[] partialResult;

    /* Object Inspectors corresponding to the struct returned by TerminatePartial and the long
     * field within the struct - "count"
     */
    private StructObjectInspector soi;

    private StructField columnTypeField;
    private WritableStringObjectInspector columnTypeFieldOI;

    private StructField minField;
    private WritableDoubleObjectInspector minFieldOI;

    private StructField maxField;
    private WritableDoubleObjectInspector maxFieldOI;

    private StructField countNullsField;
    private WritableLongObjectInspector countNullsFieldOI;

    private StructField ndvField;
    private WritableStringObjectInspector ndvFieldOI;

    private StructField numBitVectorsField;
    private WritableIntObjectInspector numBitVectorsFieldOI;

    /* Output of final result of the aggregation
     */
    private Object[] result;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);

      // initialize input
      if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
        inputOI = (PrimitiveObjectInspector) parameters[0];
        numVectorsOI = (PrimitiveObjectInspector) parameters[1];
      } else {
        soi = (StructObjectInspector) parameters[0];

        columnTypeField = soi.getStructFieldRef("ColumnType");
        columnTypeFieldOI = (WritableStringObjectInspector)
                               columnTypeField.getFieldObjectInspector();

        minField = soi.getStructFieldRef("Min");
        minFieldOI = (WritableDoubleObjectInspector) minField.getFieldObjectInspector();

        maxField = soi.getStructFieldRef("Max");
        maxFieldOI = (WritableDoubleObjectInspector) maxField.getFieldObjectInspector();

        countNullsField = soi.getStructFieldRef("CountNulls");
        countNullsFieldOI = (WritableLongObjectInspector) countNullsField.getFieldObjectInspector();

        ndvField = soi.getStructFieldRef("BitVector");
        ndvFieldOI = (WritableStringObjectInspector) ndvField.getFieldObjectInspector();

        numBitVectorsField = soi.getStructFieldRef("NumBitVectors");
        numBitVectorsFieldOI = (WritableIntObjectInspector)
                                  numBitVectorsField.getFieldObjectInspector();
      }

      // initialize output
      if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
        List<ObjectInspector> foi = new ArrayList<ObjectInspector>();
        foi.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);

        List<String> fname = new ArrayList<String>();
        fname.add("ColumnType");
        fname.add("Min");
        fname.add("Max");
        fname.add("CountNulls");
        fname.add("BitVector");
        fname.add("NumBitVectors");

        partialResult = new Object[6];
        partialResult[0] = new Text();
        partialResult[1] = new DoubleWritable(0);
        partialResult[2] = new DoubleWritable(0);
        partialResult[3] = new LongWritable(0);
        partialResult[4] = new Text();
        partialResult[5] = new IntWritable(0);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fname,
          foi);
      } else {
        List<ObjectInspector> foi = new ArrayList<ObjectInspector>();
        foi.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);

        List<String> fname = new ArrayList<String>();
        fname.add("ColumnType");
        fname.add("Min");
        fname.add("Max");
        fname.add("CountNulls");
        fname.add("NumDistinctValues");

        result = new Object[5];
        result[0] = new Text();
        result[1] = new DoubleWritable(0);
        result[2] = new DoubleWritable(0);
        result[3] = new LongWritable(0);
        result[4] = new LongWritable(0);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fname,
            foi);
      }
    }

    public static class DoubleStatsAgg implements AggregationBuffer {
      public String columnType;
      public double min;                            /* Minimum value seen so far */
      public double max;                            /* Maximum value seen so far */
      public long countNulls;      /* Count of number of null values seen so far */
      public DoubleNumDistinctValueEstimator numDV;  /* Distinct value estimator */
      public boolean firstItem;                     /* First item in the aggBuf? */
      public int numBitVectors;
    };

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      DoubleStatsAgg result = new DoubleStatsAgg();
      reset(result);
      return result;
    }

    public void initNDVEstimator(DoubleStatsAgg aggBuffer, int numBitVectors) {
      aggBuffer.numDV = new DoubleNumDistinctValueEstimator(numBitVectors);
      aggBuffer.numDV.reset();
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      DoubleStatsAgg myagg = (DoubleStatsAgg) agg;
      myagg.columnType = new String("Double");
      myagg.min = 0.0;
      myagg.max = 0.0;
      myagg.countNulls = 0;
      myagg.firstItem = true;
    }

    boolean warned = false;

    private void printDebugOutput(String functionName, AggregationBuffer agg) {
      DoubleStatsAgg myagg = (DoubleStatsAgg) agg;

      LOG.debug(functionName);

      LOG.debug("Max Value:");
      LOG.debug(myagg.max);

      LOG.debug("Min Value:");
      LOG.debug(myagg.min);

      LOG.debug("Count of Null Values:");
      LOG.debug(myagg.countNulls);

      myagg.numDV.printNumDistinctValueEstimator();
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      Object p = parameters[0];
      DoubleStatsAgg myagg = (DoubleStatsAgg) agg;
      boolean emptyTable = false;

      if (parameters[1] == null) {
        emptyTable = true;
      }

      if (myagg.firstItem) {
        int numVectors = 0;
        if (!emptyTable) {
          numVectors = PrimitiveObjectInspectorUtils.getInt(parameters[1], numVectorsOI);
        }
        initNDVEstimator(myagg, numVectors);
        myagg.firstItem = false;
        myagg.numBitVectors = numVectors;
      }

      if (!emptyTable) {

        //Update null counter if a null value is seen
        if (p == null) {
          myagg.countNulls++;
        }
        else {
          try {

            double v = PrimitiveObjectInspectorUtils.getDouble(p, inputOI);

            //Update min counter if new value is less than min seen so far
            if (v < myagg.min) {
              myagg.min = v;
            }

            //Update max counter if new value is greater than max seen so far
            if (v > myagg.max) {
              myagg.max = v;
            }

            // Add value to NumDistinctValue Estimator
            myagg.numDV.addToEstimator(v);

          } catch (NumberFormatException e) {
            if (!warned) {
              warned = true;
              LOG.warn(getClass().getSimpleName() + " "
                  + StringUtils.stringifyException(e));
              LOG.warn(getClass().getSimpleName()
                  + " ignoring similar exceptions.");
            }
          }
        }
      }
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      DoubleStatsAgg myagg = (DoubleStatsAgg) agg;

      // Serialize numDistinctValue Estimator
      Text t = myagg.numDV.serialize();

      // Serialize the rest of the values in the AggBuffer
      ((Text) partialResult[0]).set(myagg.columnType);
      ((DoubleWritable) partialResult[1]).set(myagg.min);
      ((DoubleWritable) partialResult[2]).set(myagg.max);
      ((LongWritable) partialResult[3]).set(myagg.countNulls);
      ((Text) partialResult[4]).set(t);
      ((IntWritable) partialResult[5]).set(myagg.numBitVectors);

      return partialResult;
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (partial != null) {
        DoubleStatsAgg myagg = (DoubleStatsAgg) agg;

        if (myagg.firstItem) {
          Object partialValue = soi.getStructFieldData(partial, numBitVectorsField);
          int numVectors = numBitVectorsFieldOI.get(partialValue);
          initNDVEstimator(myagg, numVectors);
          myagg.firstItem = false;
          myagg.numBitVectors = numVectors;
        }

        // Update min if min is lesser than the smallest value seen so far
        Object partialValue = soi.getStructFieldData(partial, minField);
        if (myagg.min > minFieldOI.get(partialValue)) {
          myagg.min = minFieldOI.get(partialValue);
        }

        // Update max if max is greater than the largest value seen so far
        partialValue = soi.getStructFieldData(partial, maxField);
        if (myagg.max < maxFieldOI.get(partialValue)) {
          myagg.max = maxFieldOI.get(partialValue);
        }

        // Update the null counter
        partialValue = soi.getStructFieldData(partial, countNullsField);
        myagg.countNulls += countNullsFieldOI.get(partialValue);

        // Merge numDistinctValue Estimators
        partialValue = soi.getStructFieldData(partial, ndvField);
        String v = ndvFieldOI.getPrimitiveJavaObject(partialValue);

        NumDistinctValueEstimator o = new NumDistinctValueEstimator(v, myagg.numBitVectors);
        myagg.numDV.mergeEstimators(o);
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      DoubleStatsAgg myagg = (DoubleStatsAgg) agg;
      long numDV = 0;

      if (myagg.numBitVectors != 0) {
        numDV = myagg.numDV.estimateNumDistinctValues();
      }

      // Serialize the result struct
      ((Text) result[0]).set(myagg.columnType);
      ((DoubleWritable) result[1]).set(myagg.min);
      ((DoubleWritable) result[2]).set(myagg.max);
      ((LongWritable) result[3]).set(myagg.countNulls);
      ((LongWritable) result[4]).set(numDV);

      return result;
    }
  }

  /**
   * GenericUDAFStringStatsEvaluator.
   *
   */
  public static class GenericUDAFStringStatsEvaluator extends GenericUDAFEvaluator {

    /* Object Inspector corresponding to the input parameter.
     */
    private PrimitiveObjectInspector inputOI;
    private PrimitiveObjectInspector numVectorsOI;

    /* Partial aggregation result returned by TerminatePartial. Partial result is a struct
     * containing a long field named "count".
     */
    private Object[] partialResult;

    /* Object Inspectors corresponding to the struct returned by TerminatePartial and the
     * fields within the struct - "maxLength", "sumLength", "count", "countNulls", "ndv"
     */
    private StructObjectInspector soi;

    private StructField columnTypeField;
    private WritableStringObjectInspector columnTypeFieldOI;

    private StructField maxLengthField;
    private WritableLongObjectInspector maxLengthFieldOI;

    private StructField sumLengthField;
    private WritableLongObjectInspector sumLengthFieldOI;

    private StructField countField;
    private WritableLongObjectInspector countFieldOI;

    private StructField countNullsField;
    private WritableLongObjectInspector countNullsFieldOI;

    private StructField ndvField;
    private WritableStringObjectInspector ndvFieldOI;

    private StructField numBitVectorsField;
    private WritableIntObjectInspector numBitVectorsFieldOI;

    /* Output of final result of the aggregation
     */
    private Object[] result;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);

      // initialize input
      if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
        inputOI = (PrimitiveObjectInspector) parameters[0];
        numVectorsOI = (PrimitiveObjectInspector) parameters[1];
      } else {
        soi = (StructObjectInspector) parameters[0];

        columnTypeField = soi.getStructFieldRef("ColumnType");
        columnTypeFieldOI = (WritableStringObjectInspector)
                               columnTypeField.getFieldObjectInspector();

        maxLengthField = soi.getStructFieldRef("MaxLength");
        maxLengthFieldOI = (WritableLongObjectInspector) maxLengthField.getFieldObjectInspector();

        sumLengthField = soi.getStructFieldRef("SumLength");
        sumLengthFieldOI = (WritableLongObjectInspector) sumLengthField.getFieldObjectInspector();

        countField = soi.getStructFieldRef("Count");
        countFieldOI = (WritableLongObjectInspector) countField.getFieldObjectInspector();

        countNullsField = soi.getStructFieldRef("CountNulls");
        countNullsFieldOI = (WritableLongObjectInspector) countNullsField.getFieldObjectInspector();

        ndvField = soi.getStructFieldRef("BitVector");
        ndvFieldOI = (WritableStringObjectInspector) ndvField.getFieldObjectInspector();

        numBitVectorsField = soi.getStructFieldRef("NumBitVectors");
        numBitVectorsFieldOI = (WritableIntObjectInspector)
                                  numBitVectorsField.getFieldObjectInspector();
      }

      // initialize output
      if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
        List<ObjectInspector> foi = new ArrayList<ObjectInspector>();
        foi.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);

        List<String> fname = new ArrayList<String>();
        fname.add("ColumnType");
        fname.add("MaxLength");
        fname.add("SumLength");
        fname.add("Count");
        fname.add("CountNulls");
        fname.add("BitVector");
        fname.add("NumBitVectors");

        partialResult = new Object[7];
        partialResult[0] = new Text();
        partialResult[1] = new LongWritable(0);
        partialResult[2] = new LongWritable(0);
        partialResult[3] = new LongWritable(0);
        partialResult[4] = new LongWritable(0);
        partialResult[5] = new Text();
        partialResult[6] = new IntWritable(0);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fname,
          foi);
      } else {
        List<ObjectInspector> foi = new ArrayList<ObjectInspector>();
        foi.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);

        List<String> fname = new ArrayList<String>();
        fname.add("ColumnType");
        fname.add("MaxLength");
        fname.add("AvgLength");
        fname.add("CountNulls");
        fname.add("NumDistinctValues");

        result = new Object[5];
        result[0] = new Text();
        result[1] = new LongWritable(0);
        result[2] = new DoubleWritable(0);
        result[3] = new LongWritable(0);
        result[4] = new LongWritable(0);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fname,
            foi);
      }
    }

    public static class StringStatsAgg implements AggregationBuffer {
      public String columnType;
      public long maxLength;                           /* Maximum length seen so far */
      public long sumLength;             /* Sum of lengths of all values seen so far */
      public long count;                          /* Count of all values seen so far */
      public long countNulls;          /* Count of number of null values seen so far */
      public StringNumDistinctValueEstimator numDV;      /* Distinct value estimator */
      public int numBitVectors;
      public boolean firstItem;
    };

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      StringStatsAgg result = new StringStatsAgg();
      reset(result);
      return result;
    }

    public void initNDVEstimator(StringStatsAgg aggBuffer, int numBitVectors) {
      aggBuffer.numDV = new StringNumDistinctValueEstimator(numBitVectors);
      aggBuffer.numDV.reset();
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      StringStatsAgg myagg = (StringStatsAgg) agg;
      myagg.columnType = new String("String");
      myagg.maxLength = 0;
      myagg.sumLength = 0;
      myagg.count = 0;
      myagg.countNulls = 0;
      myagg.firstItem = true;
    }

    boolean warned = false;

    private void printDebugOutput(String functionName, AggregationBuffer agg) {
      StringStatsAgg myagg = (StringStatsAgg) agg;

      LOG.debug(functionName);

      LOG.debug("Max Length:");
      LOG.debug(myagg.maxLength);

      LOG.debug("Sum of Length:");
      LOG.debug(myagg.sumLength);

      LOG.debug("Count of non-Null Values:");
      LOG.debug(myagg.count);

      LOG.debug("Count of Null Values:");
      LOG.debug(myagg.countNulls);

      myagg.numDV.printNumDistinctValueEstimator();
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      Object p = parameters[0];
      StringStatsAgg myagg = (StringStatsAgg) agg;
      boolean emptyTable = false;

      if (parameters[1] == null) {
        emptyTable = true;
      }

      if (myagg.firstItem) {
        int numVectors = 0;
        if (!emptyTable) {
          numVectors = PrimitiveObjectInspectorUtils.getInt(parameters[1], numVectorsOI);
        }
        initNDVEstimator(myagg, numVectors);
        myagg.firstItem = false;
        myagg.numBitVectors = numVectors;
      }

      if (!emptyTable) {

        // Update null counter if a null value is seen
        if (p == null) {
          myagg.countNulls++;
        }
        else {
          try {

            String v = PrimitiveObjectInspectorUtils.getString(p, inputOI);

            // Update max length if new length is greater than the ones seen so far
            int len = v.length();
            if (len > myagg.maxLength) {
              myagg.maxLength = len;
            }

            // Update sum length with the new length
            myagg.sumLength += len;

            // Increment count of values seen so far
            myagg.count++;

            // Add string value to NumDistinctValue Estimator
            myagg.numDV.addToEstimator(v);

          } catch (NumberFormatException e) {
            if (!warned) {
              warned = true;
              LOG.warn(getClass().getSimpleName() + " "
                  + StringUtils.stringifyException(e));
              LOG.warn(getClass().getSimpleName()
                  + " ignoring similar exceptions.");
            }
          }
        }
      }
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      StringStatsAgg myagg = (StringStatsAgg) agg;

      // Serialize numDistinctValue Estimator
      Text t = myagg.numDV.serialize();

      // Serialize the rest of the values in the AggBuffer
      ((Text) partialResult[0]).set(myagg.columnType);
      ((LongWritable) partialResult[1]).set(myagg.maxLength);
      ((LongWritable) partialResult[2]).set(myagg.sumLength);
      ((LongWritable) partialResult[3]).set(myagg.count);
      ((LongWritable) partialResult[4]).set(myagg.countNulls);
      ((Text) partialResult[5]).set(t);
      ((IntWritable) partialResult[6]).set(myagg.numBitVectors);

      return partialResult;
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (partial != null) {
        StringStatsAgg myagg = (StringStatsAgg) agg;

        if (myagg.firstItem) {
          Object partialValue = soi.getStructFieldData(partial, numBitVectorsField);
          int numVectors = numBitVectorsFieldOI.get(partialValue);
          initNDVEstimator(myagg, numVectors);
          myagg.firstItem = false;
          myagg.numBitVectors = numVectors;
        }

        // Update maxLength if length is greater than the largest value seen so far
        Object partialValue = soi.getStructFieldData(partial, maxLengthField);
        if (myagg.maxLength < maxLengthFieldOI.get(partialValue)) {
          myagg.maxLength = maxLengthFieldOI.get(partialValue);
        }

        // Update sum of the length of the values seen so far
        partialValue = soi.getStructFieldData(partial, sumLengthField);
        myagg.sumLength += sumLengthFieldOI.get(partialValue);

        // Update the count of the number of values seen so far
        partialValue = soi.getStructFieldData(partial, countField);
        myagg.count += countFieldOI.get(partialValue);

        // Update the null counter
        partialValue = soi.getStructFieldData(partial, countNullsField);
        myagg.countNulls += countNullsFieldOI.get(partialValue);

        // Merge numDistinctValue Estimators
        partialValue = soi.getStructFieldData(partial, ndvField);
        String v = ndvFieldOI.getPrimitiveJavaObject(partialValue);
        NumDistinctValueEstimator o = new NumDistinctValueEstimator(v, myagg.numBitVectors);
        myagg.numDV.mergeEstimators(o);
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      StringStatsAgg myagg = (StringStatsAgg) agg;

      long numDV = 0;
      double avgLength = 0.0;
      long total = myagg.count + myagg.countNulls;

      if (myagg.numBitVectors != 0) {
        numDV = myagg.numDV.estimateNumDistinctValues();
      }

      if (total != 0) {
         avgLength = (double)(myagg.sumLength / (1.0 * total));
      }

      // Serialize the result struct
      ((Text) result[0]).set(myagg.columnType);
      ((LongWritable) result[1]).set(myagg.maxLength);
      ((DoubleWritable) result[2]).set(avgLength);
      ((LongWritable) result[3]).set(myagg.countNulls);
      ((LongWritable) result[4]).set(numDV);

      return result;
    }
  }

  /**
   * GenericUDAFBinaryStatsEvaluator.
   *
   */
  public static class GenericUDAFBinaryStatsEvaluator extends GenericUDAFEvaluator {

    /* Object Inspector corresponding to the input parameter.
     */
    private PrimitiveObjectInspector inputOI;

    /* Partial aggregation result returned by TerminatePartial. Partial result is a struct
     * containing a long field named "count".
     */
    private Object[] partialResult;

    /* Object Inspectors corresponding to the struct returned by TerminatePartial and the
     * fields within the struct - "maxLength", "sumLength", "count", "countNulls"
     */
    private StructObjectInspector soi;

    private StructField columnTypeField;
    private WritableStringObjectInspector columnTypeFieldOI;

    private StructField maxLengthField;
    private WritableLongObjectInspector maxLengthFieldOI;

    private StructField sumLengthField;
    private WritableLongObjectInspector sumLengthFieldOI;

    private StructField countField;
    private WritableLongObjectInspector countFieldOI;

    private StructField countNullsField;
    private WritableLongObjectInspector countNullsFieldOI;

    /* Output of final result of the aggregation
     */
    private Object[] result;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);

      // initialize input
      if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
        inputOI = (PrimitiveObjectInspector) parameters[0];
      } else {
        soi = (StructObjectInspector) parameters[0];

        columnTypeField = soi.getStructFieldRef("ColumnType");
        columnTypeFieldOI = (WritableStringObjectInspector)
                               columnTypeField.getFieldObjectInspector();

        maxLengthField = soi.getStructFieldRef("MaxLength");
        maxLengthFieldOI = (WritableLongObjectInspector) maxLengthField.getFieldObjectInspector();

        sumLengthField = soi.getStructFieldRef("SumLength");
        sumLengthFieldOI = (WritableLongObjectInspector) sumLengthField.getFieldObjectInspector();

        countField = soi.getStructFieldRef("Count");
        countFieldOI = (WritableLongObjectInspector) countField.getFieldObjectInspector();

        countNullsField = soi.getStructFieldRef("CountNulls");
        countNullsFieldOI = (WritableLongObjectInspector) countNullsField.getFieldObjectInspector();

      }

      // initialize output
      if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
        List<ObjectInspector> foi = new ArrayList<ObjectInspector>();
        foi.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);

        List<String> fname = new ArrayList<String>();
        fname.add("ColumnType");
        fname.add("MaxLength");
        fname.add("SumLength");
        fname.add("Count");
        fname.add("CountNulls");

        partialResult = new Object[5];
        partialResult[0] = new Text();
        partialResult[1] = new LongWritable(0);
        partialResult[2] = new LongWritable(0);
        partialResult[3] = new LongWritable(0);
        partialResult[4] = new LongWritable(0);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fname,
          foi);
      } else {
        List<ObjectInspector> foi = new ArrayList<ObjectInspector>();
        foi.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);

        List<String> fname = new ArrayList<String>();
        fname.add("ColumnType");
        fname.add("MaxLength");
        fname.add("AvgLength");
        fname.add("CountNulls");

        result = new Object[4];
        result[0] = new Text();
        result[1] = new LongWritable(0);
        result[2] = new DoubleWritable(0);
        result[3] = new LongWritable(0);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fname,
            foi);
      }
    }

    public static class BinaryStatsAgg implements AggregationBuffer {
      public String columnType;
      public long maxLength;                           /* Maximum length seen so far */
      public long sumLength;             /* Sum of lengths of all values seen so far */
      public long count;                          /* Count of all values seen so far */
      public long countNulls;          /* Count of number of null values seen so far */
    };

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      BinaryStatsAgg result = new BinaryStatsAgg();
      reset(result);
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      BinaryStatsAgg myagg = (BinaryStatsAgg) agg;
      myagg.columnType = new String("Binary");
      myagg.maxLength = 0;
      myagg.sumLength = 0;
      myagg.count = 0;
      myagg.countNulls = 0;
    }

    boolean warned = false;

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      Object p = parameters[0];
      BinaryStatsAgg myagg = (BinaryStatsAgg) agg;
      boolean emptyTable = false;

      if (parameters[1] == null) {
        emptyTable = true;
      }

      if (!emptyTable) {
        // Update null counter if a null value is seen
        if (p == null) {
          myagg.countNulls++;
        }
        else {
          try {
            BytesWritable v = PrimitiveObjectInspectorUtils.getBinary(p, inputOI);

            // Update max length if new length is greater than the ones seen so far
            int len = v.getLength();
            if (len > myagg.maxLength) {
              myagg.maxLength = len;
            }

            // Update sum length with the new length
            myagg.sumLength += len;

            // Increment count of values seen so far
            myagg.count++;

          } catch (NumberFormatException e) {
            if (!warned) {
              warned = true;
              LOG.warn(getClass().getSimpleName() + " "
                  + StringUtils.stringifyException(e));
              LOG.warn(getClass().getSimpleName()
                  + " ignoring similar exceptions.");
            }
          }
        }
      }
    }

    private void printDebugOutput(String functionName, AggregationBuffer agg) {
      BinaryStatsAgg myagg = (BinaryStatsAgg) agg;

      LOG.debug(functionName);

      LOG.debug("Max Length:");
      LOG.debug(myagg.maxLength);

      LOG.debug("Sum of Length:");
      LOG.debug(myagg.sumLength);

      LOG.debug("Count of non-Null Values:");
      LOG.debug(myagg.count);

      LOG.debug("Count of Null Values:");
      LOG.debug(myagg.countNulls);
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      BinaryStatsAgg myagg = (BinaryStatsAgg) agg;

      // Serialize AggBuffer
      ((Text) partialResult[0]).set(myagg.columnType);
      ((LongWritable) partialResult[1]).set(myagg.maxLength);
      ((LongWritable) partialResult[2]).set(myagg.sumLength);
      ((LongWritable) partialResult[3]).set(myagg.count);
      ((LongWritable) partialResult[4]).set(myagg.countNulls);

      return partialResult;
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (partial != null) {
        BinaryStatsAgg myagg = (BinaryStatsAgg) agg;

        // Update maxLength if length is greater than the largest value seen so far
        Object partialValue = soi.getStructFieldData(partial, maxLengthField);
        if (myagg.maxLength < maxLengthFieldOI.get(partialValue)) {
          myagg.maxLength = maxLengthFieldOI.get(partialValue);
        }

        // Update sum of the length of the values seen so far
        partialValue = soi.getStructFieldData(partial, sumLengthField);
        myagg.sumLength += sumLengthFieldOI.get(partialValue);

        // Update the count of the number of values seen so far
        partialValue = soi.getStructFieldData(partial, countField);
        myagg.count += countFieldOI.get(partialValue);

        // Update the null counter
        partialValue = soi.getStructFieldData(partial, countNullsField);
        myagg.countNulls += countNullsFieldOI.get(partialValue);
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      BinaryStatsAgg myagg = (BinaryStatsAgg) agg;
      double avgLength = 0.0;
      long count = myagg.count + myagg.countNulls;

      if (count != 0) {
        avgLength = (double)(myagg.sumLength / (1.0 * (myagg.count + myagg.countNulls)));
      }

      // Serialize the result struct
      ((Text) result[0]).set(myagg.columnType);
      ((LongWritable) result[1]).set(myagg.maxLength);
      ((DoubleWritable) result[2]).set(avgLength);
      ((LongWritable) result[3]).set(myagg.countNulls);

      return result;
    }
  }
}
