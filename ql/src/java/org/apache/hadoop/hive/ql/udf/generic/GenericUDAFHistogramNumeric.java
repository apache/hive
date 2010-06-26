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
import java.util.Arrays;
import java.util.Random;

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
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.util.StringUtils;

/**
 * Computes an approximate histogram of a numerical column using a user-specified number of bins.
 * 
 * The output is an array of (x,y) pairs as Hive struct objects that represents the histogram's
 * bin centers and heights.
 */
@Description(name = "histogram_numeric",
    value = "_FUNC_(expr, nb) - Computes a histogram on numeric 'expr' using nb bins.",
    extended = "Example:\n"
             + "> SELECT histogram_numeric(val, 3) FROM src;\n"
             + "[{\"x\":100,\"y\":14.0},{\"x\":200,\"y\":22.0},{\"x\":290.5,\"y\":11.0}]\n"
             + "The return value is an array of (x,y) pairs representing the centers of the "
             + "histogram's bins. As the value of 'nb' is increased, the histogram approximation"
             + "gets finer-grained, but may yield artifacts around outliers. In practice, 20-40 "
             + "histogram bins appear to work well, with more bins being required for skewed or "
             + "smaller datasets. Note that this function creates a histogram with non-uniform "
             + "bin widths. It offers no guarantees in terms of the mean-squared-error of the "
             + "histogram, but in practice is comparable to the histograms produced by the R/S-Plus "
             + "statistical computing packages." )
public class GenericUDAFHistogramNumeric implements GenericUDAFResolver {
  // class static variables
  static final Log LOG = LogFactory.getLog(GenericUDAFHistogramNumeric.class.getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
    if (parameters.length != 2) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Please specify exactly two arguments.");
    }
    
    // validate the first parameter, which is the expression to compute over
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
    case STRING:
    case BOOLEAN:
    default:
      throw new UDFArgumentTypeException(0,
          "Only numeric type arguments are accepted but "
          + parameters[0].getTypeName() + " was passed as parameter 1.");
    }

    // validate the second parameter, which is the number of histogram bins
    if (parameters[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(1,
          "Only primitive type arguments are accepted but "
          + parameters[1].getTypeName() + " was passed as parameter 2.");
    }
    if( ((PrimitiveTypeInfo) parameters[1]).getPrimitiveCategory()
        != PrimitiveObjectInspector.PrimitiveCategory.INT) {
      throw new UDFArgumentTypeException(1,
          "Only an integer argument is accepted as parameter 2, but "
          + parameters[1].getTypeName() + " was passed instead.");
    }

    return new GenericUDAFHistogramNumericEvaluator();
  }

  /**
   * Construct a histogram using an algorithm described by Ben-Haim and Tom-Tov.
   *
   * The algorithm is a heuristic adapted from the following paper:
   * Yael Ben-Haim and Elad Tom-Tov, "A streaming parallel decision tree algorithm",
   * J. Machine Learning Research 11 (2010), pp. 849--872. Although there are no approximation
   * guarantees, it appears to work well with adequate data and a large (e.g., 20-80) number
   * of histogram bins.
   */
  public static class GenericUDAFHistogramNumericEvaluator extends GenericUDAFEvaluator {

    // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
    private PrimitiveObjectInspector inputOI;
    private PrimitiveObjectInspector nbinsOI;

    // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list of doubles)
    private StandardListObjectInspector loi;


    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);

      // init input object inspectors
      if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
        assert(parameters.length == 2);
        inputOI = (PrimitiveObjectInspector) parameters[0];
        nbinsOI = (PrimitiveObjectInspector) parameters[1];
      } else {
        loi = (StandardListObjectInspector) parameters[0];
      }

      // init output object inspectors
      if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
        // The output of a partial aggregation is a list of doubles representing the
        // histogram being constructed. The first element in the list is the user-specified
        // number of bins in the histogram, and the histogram itself is represented as (x,y)
        // pairs following the first element, so the list length should *always* be odd.
        return ObjectInspectorFactory.getStandardListObjectInspector(
                 PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
      } else {
        // The output of FINAL and COMPLETE is a full aggregation, which is a
        // list of DoubleWritable structs that represent the final histogram as
        // (x,y) pairs of bin centers and heights.
        ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
        foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        ArrayList<String> fname = new ArrayList<String>();
        fname.add("x");
        fname.add("y");

        return ObjectInspectorFactory.getStandardListObjectInspector(
                 ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi) );
      }
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      // Return a single ArrayList where the first element is the number of histogram bins, 
      // and subsequent elements represent histogram (x,y) pairs.
      StdAgg myagg = (StdAgg) agg;
      return myagg.histogram.serialize();
    }


    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      StdAgg myagg = (StdAgg) agg;

      if (myagg.histogram.getUsedBins() < 1) { // SQL standard - return null for zero elements
        return null;
      } else {
        ArrayList<DoubleWritable[]> result = new ArrayList<DoubleWritable[]>();
        for(int i = 0; i < myagg.histogram.getUsedBins(); i++) {
          DoubleWritable[] bin = new DoubleWritable[2];
          bin[0] = new DoubleWritable(myagg.histogram.getBin(i).x);
          bin[1] = new DoubleWritable(myagg.histogram.getBin(i).y);
          result.add(bin);
        }
        return result;
      }
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if(partial == null) {
        return;
      }
      ArrayList partialHistogram = (ArrayList) loi.getList(partial);
      StdAgg myagg = (StdAgg) agg;
      myagg.histogram.merge(partialHistogram);
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      assert (parameters.length == 2);
      if(parameters[0] == null || parameters[1] == null) {
        return;
      }
      StdAgg myagg = (StdAgg) agg;

      // Parse out the number of histogram bins only once, if we haven't already done
      // so before. We need at least 2 bins; otherwise, there is no point in creating
      // a histogram.
      if(!myagg.histogram.isReady()) {
        int nbins = PrimitiveObjectInspectorUtils.getInt(parameters[1], nbinsOI);
        if(nbins < 2) {
          throw new HiveException(getClass().getSimpleName() + " needs nbins to be at least 2,"
                                  + " but you supplied " + nbins + ".");
        }

        // allocate memory for the histogram bins
        myagg.histogram.allocate(nbins);
      }

      // Process the current data point
      double v = PrimitiveObjectInspectorUtils.getDouble(parameters[0], inputOI);
      myagg.histogram.add(v);
    }


    // Aggregation buffer definition and manipulation methods 
    static class StdAgg implements AggregationBuffer {
      NumericHistogram histogram; // the histogram object
    };

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      StdAgg result = new StdAgg();
      reset(result);
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      StdAgg myagg = (StdAgg) agg;
      myagg.histogram = new NumericHistogram();
      myagg.histogram.reset();
    }
  }
}
