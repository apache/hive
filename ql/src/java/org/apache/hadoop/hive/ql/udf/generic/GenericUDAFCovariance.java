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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.StringUtils;

/**
 * Compute the covariance covar_pop(x, y), using the following one-pass method
 * (ref. "Formulas for Robust, One-Pass Parallel Computation of Covariances and
 *  Arbitrary-Order Statistical Moments", Philippe Pebay, Sandia Labs):
 *
 *  Incremental:
 *   n : <count>
 *   mx_n = mx_(n-1) + [x_n - mx_(n-1)]/n : <xavg>
 *   my_n = my_(n-1) + [y_n - my_(n-1)]/n : <yavg>
 *   c_n = c_(n-1) + (x_n - mx_(n-1))*(y_n - my_n) : <covariance * n>
 *
 *  Merge:
 *   c_X = c_A + c_B + (mx_A - mx_B)*(my_A - my_B)*n_A*n_B/n_X
 *
 */
@Description(name = "covariance,covar_pop",
    value = "_FUNC_(x,y) - Returns the population covariance of a set of number pairs",
    extended = "The function takes as arguments any pair of numeric types and returns a double.\n"
        + "Any pair with a NULL is ignored. If the function is applied to an empty set, NULL\n"
        + "will be returned. Otherwise, it computes the following:\n"
        + "   (SUM(x*y)-SUM(x)*SUM(y)/COUNT(x,y))/COUNT(x,y)\n"
        + "where neither x nor y is null.")
public class GenericUDAFCovariance extends AbstractGenericUDAFResolver {

  static final Log LOG = LogFactory.getLog(GenericUDAFCovariance.class.getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
    if (parameters.length != 2) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Exactly two arguments are expected.");
    }

    if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0,
          "Only primitive type arguments are accepted but "
          + parameters[0].getTypeName() + " is passed.");
    }

    if (parameters[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
        throw new UDFArgumentTypeException(1,
            "Only primitive type arguments are accepted but "
            + parameters[1].getTypeName() + " is passed.");
    }

    switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
    case FLOAT:
    case DOUBLE:
    case TIMESTAMP:
    case DECIMAL:
      switch (((PrimitiveTypeInfo) parameters[1]).getPrimitiveCategory()) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case TIMESTAMP:
      case DECIMAL:
        return new GenericUDAFCovarianceEvaluator();
      case STRING:
      case BOOLEAN:
      default:
        throw new UDFArgumentTypeException(1,
            "Only numeric or string type arguments are accepted but "
            + parameters[1].getTypeName() + " is passed.");
      }
    case STRING:
    case BOOLEAN:
    default:
      throw new UDFArgumentTypeException(0,
          "Only numeric or string type arguments are accepted but "
          + parameters[0].getTypeName() + " is passed.");
    }
  }

  /**
   * Evaluate the variance using the algorithm described in
   * http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance,
   * presumably by  Pébay, Philippe (2008), in "Formulas for Robust,
   * One-Pass Parallel Computation of Covariances and Arbitrary-Order
   * Statistical Moments", Technical Report SAND2008-6212,
   * Sandia National Laboratories,
   * http://infoserve.sandia.gov/sand_doc/2008/086212.pdf
   *
   *  Incremental:
   *   n : <count>
   *   mx_n = mx_(n-1) + [x_n - mx_(n-1)]/n : <xavg>
   *   my_n = my_(n-1) + [y_n - my_(n-1)]/n : <yavg>
   *   c_n = c_(n-1) + (x_n - mx_(n-1))*(y_n - my_n) : <covariance * n>
   *
   *  Merge:
   *   c_X = c_A + c_B + (mx_A - mx_B)*(my_A - my_B)*n_A*n_B/n_X
   *
   *  This one-pass algorithm is stable.
   *
   */
  public static class GenericUDAFCovarianceEvaluator extends GenericUDAFEvaluator {

    // For PARTIAL1 and COMPLETE
    private PrimitiveObjectInspector xInputOI;
    private PrimitiveObjectInspector yInputOI;

    // For PARTIAL2 and FINAL
    private StructObjectInspector soi;
    private StructField countField;
    private StructField xavgField;
    private StructField yavgField;
    private StructField covarField;
    private LongObjectInspector countFieldOI;
    private DoubleObjectInspector xavgFieldOI;
    private DoubleObjectInspector yavgFieldOI;
    private DoubleObjectInspector covarFieldOI;

    // For PARTIAL1 and PARTIAL2
    private Object[] partialResult;

    // For FINAL and COMPLETE
    private DoubleWritable result;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);

      // init input
      if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
        assert (parameters.length == 2);
        xInputOI = (PrimitiveObjectInspector) parameters[0];
        yInputOI = (PrimitiveObjectInspector) parameters[1];
      } else {
        assert (parameters.length == 1);
        soi = (StructObjectInspector) parameters[0];

        countField = soi.getStructFieldRef("count");
        xavgField = soi.getStructFieldRef("xavg");
        yavgField = soi.getStructFieldRef("yavg");
        covarField = soi.getStructFieldRef("covar");

        countFieldOI =
            (LongObjectInspector) countField.getFieldObjectInspector();
        xavgFieldOI =
            (DoubleObjectInspector) xavgField.getFieldObjectInspector();
        yavgFieldOI =
            (DoubleObjectInspector) yavgField.getFieldObjectInspector();
        covarFieldOI =
            (DoubleObjectInspector) covarField.getFieldObjectInspector();
      }

      // init output
      if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
        // The output of a partial aggregation is a struct containing
        // a long count, two double averages, and a double covariance.

        ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();

        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);

        ArrayList<String> fname = new ArrayList<String>();
        fname.add("count");
        fname.add("xavg");
        fname.add("yavg");
        fname.add("covar");

        partialResult = new Object[4];
        partialResult[0] = new LongWritable(0);
        partialResult[1] = new DoubleWritable(0);
        partialResult[2] = new DoubleWritable(0);
        partialResult[3] = new DoubleWritable(0);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);

      } else {
        setResult(new DoubleWritable(0));
        return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
      }
    }

    static class StdAgg implements AggregationBuffer {
      long count; // number n of elements
      double xavg; // average of x elements
      double yavg; // average of y elements
      double covar; // n times the covariance
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
      myagg.count = 0;
      myagg.xavg = 0;
      myagg.yavg = 0;
      myagg.covar = 0;
    }

    private boolean warned = false;

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      assert (parameters.length == 2);
      Object px = parameters[0];
      Object py = parameters[1];
      if (px != null && py != null) {
        StdAgg myagg = (StdAgg) agg;
        double vx = PrimitiveObjectInspectorUtils.getDouble(px, xInputOI);
        double vy = PrimitiveObjectInspectorUtils.getDouble(py, yInputOI);
        myagg.count++;
        myagg.yavg = myagg.yavg + (vy - myagg.yavg) / myagg.count;
        if (myagg.count > 1) {
            myagg.covar += (vx - myagg.xavg) * (vy - myagg.yavg);
        }
        myagg.xavg = myagg.xavg + (vx - myagg.xavg) / myagg.count;
      }
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      StdAgg myagg = (StdAgg) agg;
      ((LongWritable) partialResult[0]).set(myagg.count);
      ((DoubleWritable) partialResult[1]).set(myagg.xavg);
      ((DoubleWritable) partialResult[2]).set(myagg.yavg);
      ((DoubleWritable) partialResult[3]).set(myagg.covar);
      return partialResult;
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (partial != null) {
        StdAgg myagg = (StdAgg) agg;

        Object partialCount = soi.getStructFieldData(partial, countField);
        Object partialXAvg = soi.getStructFieldData(partial, xavgField);
        Object partialYAvg = soi.getStructFieldData(partial, yavgField);
        Object partialCovar = soi.getStructFieldData(partial, covarField);

        long nA = myagg.count;
        long nB = countFieldOI.get(partialCount);

        if (nA == 0) {
            // Just copy the information since there is nothing so far
            myagg.count = countFieldOI.get(partialCount);
            myagg.xavg = xavgFieldOI.get(partialXAvg);
            myagg.yavg = yavgFieldOI.get(partialYAvg);
            myagg.covar = covarFieldOI.get(partialCovar);
        }

        if (nA != 0 && nB != 0) {
          // Merge the two partials
          double xavgA = myagg.xavg;
          double yavgA = myagg.yavg;
          double xavgB = xavgFieldOI.get(partialXAvg);
          double yavgB = yavgFieldOI.get(partialYAvg);
          double covarB = covarFieldOI.get(partialCovar);

          myagg.count += nB;
          myagg.xavg = (xavgA * nA + xavgB * nB) / myagg.count;
          myagg.yavg = (yavgA * nA + yavgB * nB) / myagg.count;
          myagg.covar +=
              covarB + (xavgA - xavgB) * (yavgA - yavgB) * ((double) (nA * nB) / myagg.count);
        }
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      StdAgg myagg = (StdAgg) agg;

      if (myagg.count == 0) { // SQL standard - return null for zero elements
          return null;
      } else {
          getResult().set(myagg.covar / (myagg.count));
          return getResult();
      }
    }

    public void setResult(DoubleWritable result) {
      this.result = result;
    }

    public DoubleWritable getResult() {
      return result;
    }
  }

}
