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
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.util.StringUtils;

/**
 * Computes an approximate percentile (quantile) from an approximate histogram, for very
 * large numbers of rows where the regular percentile() UDAF might run out of memory.
 * 
 * The input is a single double value or an array of double values representing the quantiles
 * requested. The output, corresponding to the input, is either an single double value or an
 * array of doubles that are the quantile values.
 */
@Description(name = "percentile_approx",
    value = "_FUNC_(expr, pc, [nb]) - For very large data, computes an approximate percentile " +
            "value from a histogram, using the optional argument [nb] as the number of histogram" +
            " bins to use. A higher value of nb results in a more accurate approximation, at " +
            "the cost of higher memory usage.",
    extended = "'expr' can be any numeric column, including doubles and floats, and 'pc' is " +
               "either a single double/float with a requested percentile, or an array of double/" +
               "float with multiple percentiles. If 'nb' is not specified, the default " +
               "approximation is done with 10,000 histogram bins, which means that if there are " + 
               "10,000 or fewer unique values in 'expr', you can expect an exact result. The " +
               "percentile() function always computes an exact percentile and can run out of " +
               "memory if there are too many unique values in a column, which necessitates " +
               "this function.\n" +
               "Example (three percentiles requested using a finer histogram approximation):\n" +
               "> SELECT percentile_approx(val, array(0.5, 0.95, 0.98), 100000) FROM somedata;\n" +
               "[0.05,1.64,2.26]\n")
public class GenericUDAFPercentileApprox implements GenericUDAFResolver {
  static final Log LOG = LogFactory.getLog(GenericUDAFPercentileApprox.class.getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
    if (parameters.length != 2 && parameters.length != 3) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Please specify either two or three arguments.");
    }
    
    // Validate the first parameter, which is the expression to compute over. This should be a
    // numeric primitive type.
    if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0,
          "Only primitive type arguments are accepted but "
          + parameters[0].getTypeName() + " was passed as parameter 1.");
    }
    switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
    case FLOAT:
    case DOUBLE:
      break;
    default:
      throw new UDFArgumentTypeException(0,
          "Only numeric type arguments are accepted but "
          + parameters[0].getTypeName() + " was passed as parameter 1.");
    }

    // Validate the second parameter, which is either a solitary double or an array of doubles.
    boolean wantManyQuantiles = false;
    switch(parameters[1].getCategory()) {
    case PRIMITIVE:
      // Only a single double was passed as parameter 2, a single quantile is being requested
      switch(((PrimitiveTypeInfo) parameters[1]).getPrimitiveCategory()) {
      case FLOAT:
      case DOUBLE:
        break;
      default:
        throw new UDFArgumentTypeException(1,
          "Only a float/double or float/double array argument is accepted as parameter 2, but "
          + parameters[1].getTypeName() + " was passed instead.");
      }
      break;

    case LIST:
      // An array was passed as parameter 2, make sure it's an array of primitives
      if(((ListTypeInfo) parameters[1]).getListElementTypeInfo().getCategory() !=
         ObjectInspector.Category.PRIMITIVE) {
          throw new UDFArgumentTypeException(1,
            "A float/double array argument may be passed as parameter 2, but "
            + parameters[1].getTypeName() + " was passed instead.");
      }
      // Now make sure it's an array of doubles or floats. We don't allow integer types here
      // because percentile (really, quantile) values should generally be strictly between 0 and 1.
      switch(((PrimitiveTypeInfo)((ListTypeInfo) parameters[1]).getListElementTypeInfo()).
             getPrimitiveCategory()) {
      case FLOAT:
      case DOUBLE:
        break;
      default:
        throw new UDFArgumentTypeException(1,
          "A float/double array argument may be passed as parameter 2, but "
          + parameters[1].getTypeName() + " was passed instead.");
      }
      wantManyQuantiles = true;
      break;

    default:
      throw new UDFArgumentTypeException(1,
        "Only a float/double or float/double array argument is accepted as parameter 2, but "
        + parameters[1].getTypeName() + " was passed instead.");
    }

    // If a third parameter has been specified, it should be an integer that specifies the number
    // of histogram bins to use in the percentile approximation.
    if(parameters.length == 3) {
      if(parameters[2].getCategory() != ObjectInspector.Category.PRIMITIVE) {
        throw new UDFArgumentTypeException(2, "Only a primitive argument is accepted as "
           + "parameter 3, but " + parameters[2].getTypeName() + " was passed instead.");
      }
      switch(((PrimitiveTypeInfo) parameters[2]).getPrimitiveCategory()) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        break;
      default:
        throw new UDFArgumentTypeException(2, "Only an integer argument is accepted as "
           + "parameter 3, but " + parameters[2].getTypeName() + " was passed instead.");
      }
    }

    // Return an evaluator depending on the return type
    if(wantManyQuantiles) {
      return new GenericUDAFMultiplePercentileApproxEvaluator();
    } else {
      return new GenericUDAFSinglePercentileApproxEvaluator();
    }
  }
  
  public static class GenericUDAFSinglePercentileApproxEvaluator extends
    GenericUDAFPercentileApproxEvaluator {

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);

      // init input object inspectors
      if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
        inputOI = (PrimitiveObjectInspector) parameters[0];
        quantilesOI = parameters[1];
        if(parameters.length > 2) {
          nbinsOI = (PrimitiveObjectInspector) parameters[2];
        }
      } else {
        loi = (StandardListObjectInspector) parameters[0];
      }

      // Init output object inspectors.
      //
      // The return type for a partial aggregation is still a list of doubles, as in
      // GenericUDAFHistogramNumeric, but we add on the percentile values requested to the
      // end, and handle serializing/deserializing before we pass things on to the parent
      // method.
      // The return type for FINAL and COMPLETE is a full aggregation result, which is a
      // single double value
      if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
        return ObjectInspectorFactory.getStandardListObjectInspector(
            PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
      } else {
        return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      PercentileAggBuf myagg = (PercentileAggBuf) agg;

      if (myagg.histogram.getUsedBins() < 1) { // SQL standard - return null for zero elements
        return null;
      } else {
        assert(myagg.quantiles != null);
        return new DoubleWritable(myagg.histogram.quantile(myagg.quantiles[0]));
      }
    }
  }

  
  public static class GenericUDAFMultiplePercentileApproxEvaluator extends
    GenericUDAFPercentileApproxEvaluator {

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);

      // init input object inspectors
      if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
        inputOI = (PrimitiveObjectInspector) parameters[0];
        quantilesOI = parameters[1];
        if(parameters.length > 2) {
          nbinsOI = (PrimitiveObjectInspector) parameters[2];
        }
      } else {
        loi = (StandardListObjectInspector) parameters[0];
      }

      // Init output object inspectors.
      //
      // The return type for a partial aggregation is still a list of doubles, as in
      // GenericUDAFHistogramNumeric, but we add on the percentile values requested to the
      // end, and handle serializing/deserializing before we pass things on to the parent
      // method.
      // The return type for FINAL and COMPLETE is a full aggregation result, which is also
      // a list of doubles
      return ObjectInspectorFactory.getStandardListObjectInspector(
          PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      PercentileAggBuf myagg = (PercentileAggBuf) agg;

      if (myagg.histogram.getUsedBins() < 1) { // SQL standard - return null for zero elements
        return null;
      } else {
        ArrayList<DoubleWritable> result = new ArrayList<DoubleWritable>();
        assert(myagg.quantiles != null);
        for(int i = 0; i < myagg.quantiles.length; i++) {
          result.add(new DoubleWritable(myagg.histogram.quantile(myagg.quantiles[i])));
        }
        return result;
      }
    }
  }

  /**
   * Construct a histogram using the algorithm described by Ben-Haim and Tom-Tov, and then
   * use it to compute an approximate percentile value.
   */
  public abstract static class GenericUDAFPercentileApproxEvaluator extends GenericUDAFEvaluator {
    // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
    protected PrimitiveObjectInspector inputOI;
    protected ObjectInspector quantilesOI;
    protected PrimitiveObjectInspector nbinsOI;

    // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list of doubles)
    protected StandardListObjectInspector loi;

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if(partial == null) { 
        return;
      }
      PercentileAggBuf myagg = (PercentileAggBuf) agg;
      List<DoubleWritable> partialHistogram = (List<DoubleWritable>) loi.getList(partial);

      // remove requested quantiles from the head of the list
      int nquantiles = (int) partialHistogram.get(0).get();
      if(nquantiles > 0) {
        myagg.quantiles = new double[nquantiles];
        for(int i = 1; i <= nquantiles; i++) {
          myagg.quantiles[i-1] = partialHistogram.get(i).get();
        }
        partialHistogram.subList(0, nquantiles+1).clear();
      }

      // merge histograms
      myagg.histogram.merge(partialHistogram);           
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      PercentileAggBuf myagg = (PercentileAggBuf) agg;
      ArrayList<DoubleWritable> result = new ArrayList<DoubleWritable>();

      if(myagg.quantiles != null) {
        result.add(new DoubleWritable(myagg.quantiles.length));
        for(int i = 0; i < myagg.quantiles.length; i++) {
          result.add(new DoubleWritable(myagg.quantiles[i]));
        }
      } else {
        result.add(new DoubleWritable(0));
      }
      result.addAll(myagg.histogram.serialize());

      return result;
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      assert (parameters.length == 2 || parameters.length == 3);
      if(parameters[0] == null || parameters[1] == null) {
        return;
      }
      PercentileAggBuf myagg = (PercentileAggBuf) agg;

      // Parse out the requested quantiles just once, if we haven't already done so before.
      if(myagg.quantiles == null) {
        if(quantilesOI.getCategory() == ObjectInspector.Category.LIST) {
          // Multiple quantiles are requested
          int nquantiles = ((StandardListObjectInspector) quantilesOI).getListLength(parameters[1]);
          assert(nquantiles >= 1);
          myagg.quantiles = new double[nquantiles];
          StandardListObjectInspector sloi = (StandardListObjectInspector) quantilesOI;
          for(int i = 0; i < nquantiles; i++) {
            myagg.quantiles[i] = PrimitiveObjectInspectorUtils.getDouble(
                                  sloi.getListElement(parameters[1], i), 
                                  (PrimitiveObjectInspector) sloi.getListElementObjectInspector());
          }
        } else {
          // Just one quantile is requested
          myagg.quantiles = new double[1];
          myagg.quantiles[0] = PrimitiveObjectInspectorUtils.getDouble(parameters[1], 
                               (PrimitiveObjectInspector) quantilesOI);
        }


        // Validate requested quantiles, make sure they're in [0,1]
        for(int i = 0; i < myagg.quantiles.length; i++) {
          if(myagg.quantiles[i] <= 0 || myagg.quantiles[i] >= 1) {
            throw new HiveException(getClass().getSimpleName() + " requires percentile values to "
                                    + "lie strictly between 0 and 1, but you supplied "
                                    + myagg.quantiles[i]);

          }
        } 
      }

      // Parse out the number of histogram bins just once, if we haven't done so before.
      if(!myagg.histogram.isReady()) {
        if(parameters.length == 3 && nbinsOI != null) {
          // User has specified the number of histogram bins to use
          myagg.histogram.allocate(PrimitiveObjectInspectorUtils.getInt(parameters[2], nbinsOI)); 
        } else {
          // Choose a nice default value.
          myagg.histogram.allocate(10000);
        }
      }

      // Get and process the current datum
      double v = PrimitiveObjectInspectorUtils.getDouble(parameters[0], inputOI);
      myagg.histogram.add(v);
    }

    // Aggregation buffer methods. We wrap GenericUDAFHistogramNumeric's aggregation buffer
    // inside our own, so that we can also store requested quantile values between calls
    static class PercentileAggBuf implements AggregationBuffer {
      NumericHistogram histogram;   // histogram used for quantile approximation
      double[] quantiles;           // the quantiles requested
    };

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      PercentileAggBuf result = new PercentileAggBuf();
      result.histogram = new NumericHistogram();
      reset(result);
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      PercentileAggBuf result = (PercentileAggBuf) agg;
      result.histogram.reset();
      result.quantiles = null;
    }
  }
}
