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

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

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
public class GenericUDAFPercentileApprox extends AbstractGenericUDAFResolver {
  static final Logger LOG = LoggerFactory.getLogger(GenericUDAFPercentileApprox.class.getName());

  private static void verifyFractionType(ObjectInspector oi) throws UDFArgumentTypeException {
    PrimitiveCategory pc = ((PrimitiveObjectInspector)oi).getPrimitiveCategory();
    switch(pc) {
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
        break;
      default:
        throw new UDFArgumentTypeException(1, "Only a floating point or decimal, or "
          + "floating point or decimal array argument is accepted as parameter 2, but "
          + pc + " was passed instead.");
    }
  }

  @Override
  public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
    ObjectInspector[] parameters = info.getParameterObjectInspectors();
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
    switch (((PrimitiveObjectInspector) parameters[0]).getPrimitiveCategory()) {
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
    case FLOAT:
    case DOUBLE:
    case TIMESTAMP:
    case DECIMAL:
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
      verifyFractionType(parameters[1]);
      break;

    case LIST:
      // An array was passed as parameter 2, make sure it's an array of primitives
      if(((ListObjectInspector) parameters[1]).getListElementObjectInspector().getCategory() !=
         ObjectInspector.Category.PRIMITIVE) {
          throw new UDFArgumentTypeException(1,
            "A floating point or decimal array argument may be passed as parameter 2, but "
            + parameters[1].getTypeName() + " was passed instead.");
      }
      // Now make sure it's an array of doubles or floats. We don't allow integer types here
      // because percentile (really, quantile) values should generally be strictly between 0 and 1.
      verifyFractionType(((ListObjectInspector) parameters[1]).getListElementObjectInspector());
      wantManyQuantiles = true;
      break;

    default:
      throw new UDFArgumentTypeException(1,
        "Only a floating point or decimal, or floating point or decimal array argument is accepted"
          + " as parameter 2, but " + parameters[1].getTypeName() + " was passed instead.");
    }
    // Also make sure it is a constant.
    if (!ObjectInspectorUtils.isConstantObjectInspector(parameters[1])) {
      throw new UDFArgumentTypeException(1,
        "The second argument must be a constant, but " + parameters[1].getTypeName() +
        " was passed instead.");
    }

    // If a third parameter has been specified, it should be an integer that specifies the number
    // of histogram bins to use in the percentile approximation.
    if(parameters.length == 3) {
      if(parameters[2].getCategory() != ObjectInspector.Category.PRIMITIVE) {
        throw new UDFArgumentTypeException(2, "Only a primitive argument is accepted as "
           + "parameter 3, but " + parameters[2].getTypeName() + " was passed instead.");
      }
      switch(((PrimitiveObjectInspector) parameters[2]).getPrimitiveCategory()) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case TIMESTAMP:
        break;
      default:
        throw new UDFArgumentTypeException(2, "Only an integer argument is accepted as "
           + "parameter 3, but " + parameters[2].getTypeName() + " was passed instead.");
      }
      // Also make sure it is a constant.
      if (!ObjectInspectorUtils.isConstantObjectInspector(parameters[2])) {
        throw new UDFArgumentTypeException(2,
          "The third argument must be a constant, but " + parameters[2].getTypeName() +
          " was passed instead.");
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
        quantiles = getQuantileArray((ConstantObjectInspector)parameters[1]);
        if(parameters.length > 2) {
          nbins = PrimitiveObjectInspectorUtils.getInt(
              ((ConstantObjectInspector) parameters[2]).getWritableConstantValue(),
              (PrimitiveObjectInspector)parameters[2]);
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
        quantiles = getQuantileArray((ConstantObjectInspector)parameters[1]);
        if(parameters.length > 2) {
          nbins = PrimitiveObjectInspectorUtils.getInt(
              ((ConstantObjectInspector) parameters[2]).getWritableConstantValue(),
              (PrimitiveObjectInspector)parameters[2]);
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
    protected double quantiles[];
    protected Integer nbins = 10000;

    // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list of doubles)
    protected transient StandardListObjectInspector loi;

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if(partial == null) {
        return;
      }
      PercentileAggBuf myagg = (PercentileAggBuf) agg;
      List partialHistogram = (List) loi.getList(partial);
      DoubleObjectInspector doi = (DoubleObjectInspector)loi.getListElementObjectInspector();

      // remove requested quantiles from the head of the list
      int nquantiles = (int) doi.get(partialHistogram.get(0));
      if(nquantiles > 0) {
        myagg.quantiles = new double[nquantiles];
        for(int i = 1; i <= nquantiles; i++) {
          myagg.quantiles[i-1] = doi.get(partialHistogram.get(i));
        }
        partialHistogram.subList(0, nquantiles+1).clear();
      } else {
        partialHistogram.subList(0, 1).clear();
      }

      // merge histograms
      myagg.histogram.merge(partialHistogram, doi);
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

      // Get and process the current datum
      double v = PrimitiveObjectInspectorUtils.getDouble(parameters[0], inputOI);
      myagg.histogram.add(v);
    }

    // Aggregation buffer methods. We wrap GenericUDAFHistogramNumeric's aggregation buffer
    // inside our own, so that we can also store requested quantile values between calls
    @AggregationType(estimable = true)
    static class PercentileAggBuf extends AbstractAggregationBuffer {
      NumericHistogram histogram;   // histogram used for quantile approximation
      double[] quantiles;           // the quantiles requested
      @Override
      public int estimate() {
        JavaDataModel model = JavaDataModel.get();
        return histogram.lengthFor(model) +
            model.array() + JavaDataModel.PRIMITIVES2 * quantiles.length;
      }
    };

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      PercentileAggBuf result = new PercentileAggBuf();
      result.histogram = new NumericHistogram();
      reset(result);
      return result;
    }

    protected double[] getQuantileArray(ConstantObjectInspector quantileOI)
        throws HiveException {
      double[] result = null;
      Object quantileObj = quantileOI.getWritableConstantValue();
      if (quantileOI instanceof ListObjectInspector) {
        ObjectInspector elemOI =
            ((ListObjectInspector)quantileOI).getListElementObjectInspector();
        result = new double[((List<?>)quantileObj).size()];
        assert(result.length >= 1);
        for (int ii = 0; ii < result.length; ++ii) {
          result[ii] = PrimitiveObjectInspectorUtils.getDouble(
              ((List<?>)quantileObj).get(ii),
              (PrimitiveObjectInspector)elemOI);
        }
      } else {
        result = new double[1];
        result[0] = PrimitiveObjectInspectorUtils.getDouble(
              quantileObj,
              (PrimitiveObjectInspector)quantileOI);
      }
      for(int ii = 0; ii < result.length; ++ii) {
        if (result[ii] <= 0 || result[ii] >= 1) {
          throw new HiveException(
              getClass().getSimpleName() + " requires percentile values to " +
              "lie strictly between 0 and 1, but you supplied " + result[ii]);
        }
      }

      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      PercentileAggBuf result = (PercentileAggBuf) agg;
      result.histogram.reset();
      result.quantiles = null;

      result.histogram.allocate(nbins);
      result.quantiles = quantiles;
    }
  }
}
