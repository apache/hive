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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.LongWritable;

/**
 * GenericUDAFPercentileCont.
 */
@Description(name = "percentile_cont", value = "_FUNC_(input, pc) "
    + "- Returns the percentile of expr at pc (range: [0,1]).")
public class GenericUDAFPercentileCont extends AbstractGenericUDAFResolver {

  private static final Comparator<LongWritable> LONG_COMPARATOR;
  private static final Comparator<DoubleWritable> DOUBLE_COMPARATOR;

  static {
    LONG_COMPARATOR = ShimLoader.getHadoopShims().getLongComparator();
    DOUBLE_COMPARATOR = new Comparator<DoubleWritable>() {
      @Override
      public int compare(DoubleWritable o1, DoubleWritable o2) {
        return o1.compareTo(o2);
      }
    };
  }

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
    if (parameters.length != 2) {
      throw new UDFArgumentTypeException(parameters.length - 1, "Exactly 2 argument is expected.");
    }

    if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0, "Only primitive type arguments are accepted but "
          + parameters[0].getTypeName() + " is passed.");
    }
    switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
    case VOID:
      return new PercentileContLongEvaluator();
    case FLOAT:
    case DOUBLE:
    case DECIMAL:
      return new PercentileContDoubleEvaluator();
    case STRING:
    case TIMESTAMP:
    case VARCHAR:
    case CHAR:
    case BOOLEAN:
    case DATE:
    default:
      throw new UDFArgumentTypeException(0,
          "Only numeric arguments are accepted but " + parameters[0].getTypeName() + " is passed.");
    }
  }

  /**
   * A comparator to sort the entries in order - Long.
   */
  @SuppressWarnings("serial")
  public static class LongComparator
      implements Comparator<Map.Entry<LongWritable, LongWritable>>, Serializable {
    @Override
    public int compare(Map.Entry<LongWritable, LongWritable> o1,
        Map.Entry<LongWritable, LongWritable> o2) {
      return LONG_COMPARATOR.compare(o1.getKey(), o2.getKey());
    }
  }

  /**
   * A comparator to sort the entries in order - Double.
   */
  @SuppressWarnings("serial")
  public static class DoubleComparator
      implements Comparator<Map.Entry<DoubleWritable, LongWritable>>, Serializable {
    @Override
    public int compare(Map.Entry<DoubleWritable, LongWritable> o1,
        Map.Entry<DoubleWritable, LongWritable> o2) {
      return DOUBLE_COMPARATOR.compare(o1.getKey(), o2.getKey());
    }
  }

  protected interface PercentileCalculator<U> {
    double getPercentile(List<Map.Entry<U, LongWritable>> entriesList, double position);
  }

  /**
   * An abstract class to hold the generic udf functions for calculating percentile.
   */
  public abstract static class PercentileContEvaluator<T, U> extends GenericUDAFEvaluator {
    PercentileCalculator<U> calc = getCalculator();

    /**
     * A state class to store intermediate aggregation results.
     */
    public class PercentileAgg extends AbstractAggregationBuffer {
      Map<U, LongWritable> counts;
      List<DoubleWritable> percentiles;
    }

    // For PARTIAL1 and COMPLETE
    protected PrimitiveObjectInspector inputOI;
    MapObjectInspector countsOI;
    ListObjectInspector percentilesOI;

    // For PARTIAL1 and PARTIAL2
    protected transient Object[] partialResult;

    // FINAL and COMPLETE output
    protected DoubleWritable result;

    // PARTIAL2 and FINAL inputs
    protected transient StructObjectInspector soi;
    protected transient StructField countsField;
    protected transient StructField percentilesField;

    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);

      initInspectors(parameters);

      if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {// ...for partial result
        partialResult = new Object[2];

        ArrayList<ObjectInspector> foi = getPartialInspectors();

        ArrayList<String> fname = new ArrayList<String>();
        fname.add("counts");
        fname.add("percentiles");

        return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
      } else { // ...for final result
        result = new DoubleWritable(0);
        return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
      }
    }

    protected abstract PercentileCalculator<U> getCalculator();

    protected abstract ArrayList<ObjectInspector> getPartialInspectors();

    protected abstract T getInput(Object object, PrimitiveObjectInspector inputOI);

    protected abstract U wrapInput(T input);

    protected abstract U copyInput(U input);

    protected abstract void sortEntries(List<Entry<U, LongWritable>> entriesList);

    protected void initInspectors(ObjectInspector[] parameters) {
      if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {// ...for real input data
        inputOI = (PrimitiveObjectInspector) parameters[0];
      } else { // ...for partial result as input
        soi = (StructObjectInspector) parameters[0];

        countsField = soi.getStructFieldRef("counts");
        percentilesField = soi.getStructFieldRef("percentiles");

        countsOI = (MapObjectInspector) countsField.getFieldObjectInspector();
        percentilesOI = (ListObjectInspector) percentilesField.getFieldObjectInspector();
      }
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      PercentileAgg agg = new PercentileAgg();
      return agg;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      PercentileAgg percAgg = (PercentileAgg) agg;
      if (percAgg.counts != null) {
        percAgg.counts.clear();
      }
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      PercentileAgg percAgg = (PercentileAgg) agg;
      Double percentile = ((HiveDecimalWritable) parameters[1]).getHiveDecimal().doubleValue();

      if (percAgg.percentiles == null) {
        validatePercentile(percentile);
        percAgg.percentiles = new ArrayList<DoubleWritable>(1);
        percAgg.percentiles.add(new DoubleWritable(percentile));
      }

      if (parameters[0] == null) {
        return;
      }

      T input = getInput(parameters[0], inputOI);

      if (input != null) {
        increment(percAgg, wrapInput(input), 1);
      }
    }

    protected void increment(PercentileAgg s, U input, long i) {
      if (s.counts == null) {
        s.counts = new HashMap<U, LongWritable>();
      }
      LongWritable count = s.counts.get(input);
      if (count == null) {
        s.counts.put(copyInput(input), new LongWritable(i));
      } else {
        count.set(count.get() + i);
      }
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (partial == null) {
        return;
      }

      Object objCounts = soi.getStructFieldData(partial, countsField);
      Object objPercentiles = soi.getStructFieldData(partial, percentilesField);

      Map<U, LongWritable> counts = (Map<U, LongWritable>) countsOI.getMap(objCounts);
      List<DoubleWritable> percentiles =
          (List<DoubleWritable>) percentilesOI.getList(objPercentiles);

      if (counts == null || percentiles == null) {
        return;
      }

      PercentileAgg percAgg = (PercentileAgg) agg;

      if (percAgg.percentiles == null) {
        percAgg.percentiles = new ArrayList<DoubleWritable>(percentiles);
      }

      for (Map.Entry<U, LongWritable> e : counts.entrySet()) {
        increment(percAgg, e.getKey(), e.getValue().get());
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      PercentileAgg percAgg = (PercentileAgg) agg;

      // No input data.
      if (percAgg.counts == null || percAgg.counts.size() == 0) {
        return null;
      }

      // Get all items into an array and sort them.
      Set<Map.Entry<U, LongWritable>> entries = percAgg.counts.entrySet();
      List<Map.Entry<U, LongWritable>> entriesList =
          new ArrayList<Map.Entry<U, LongWritable>>(entries);
      sortEntries(entriesList);

      // Accumulate the counts.
      long total = getTotal(entriesList);

      // Initialize the result.
      if (result == null) {
        result = new DoubleWritable();
      }

      calculatePercentile(percAgg, entriesList, total);

      return result;
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      PercentileAgg percAgg = (PercentileAgg) agg;
      partialResult[0] = percAgg.counts;
      partialResult[1] = percAgg.percentiles;

      return partialResult;
    }

    protected long getTotal(List<Map.Entry<U, LongWritable>> entriesList) {
      long total = 0;
      for (int i = 0; i < entriesList.size(); i++) {
        LongWritable count = entriesList.get(i).getValue();
        total += count.get();
        count.set(total);
      }
      return total;
    }

    protected void validatePercentile(Double percentile) {
      if (percentile < 0.0 || percentile > 1.0) {
        throw new RuntimeException("Percentile value must be within the range of 0 to 1.");
      }
    }

    protected void calculatePercentile(PercentileAgg percAgg,
        List<Map.Entry<U, LongWritable>> entriesList, long total) {
      // maxPosition is the 1.0 percentile
      long maxPosition = total - 1;
      double position = maxPosition * percAgg.percentiles.get(0).get();
      result.set(calc.getPercentile(entriesList, position));
    }

  }

  /**
   * The evaluator for percentile computation based on long.
   */
  public static class PercentileContLongEvaluator
      extends PercentileContEvaluator<Long, LongWritable> {

    protected ArrayList<ObjectInspector> getPartialInspectors() {
      ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();

      foi.add(ObjectInspectorFactory.getStandardMapObjectInspector(
          PrimitiveObjectInspectorFactory.writableLongObjectInspector,
          PrimitiveObjectInspectorFactory.writableLongObjectInspector));
      foi.add(ObjectInspectorFactory.getStandardListObjectInspector(
          PrimitiveObjectInspectorFactory.writableDoubleObjectInspector));
      return foi;
    }

    protected Long getInput(Object parameter, PrimitiveObjectInspector inputOI) {
      return PrimitiveObjectInspectorUtils.getLong(parameter, inputOI);
    }

    protected LongWritable wrapInput(Long input) {
      return new LongWritable(input);
    }

    protected LongWritable copyInput(LongWritable input) {
      return new LongWritable(input.get());
    }

    protected void sortEntries(List<Entry<LongWritable, LongWritable>> entriesList) {
      Collections.sort(entriesList, new LongComparator());
    }

    @Override
    protected PercentileCalculator<LongWritable> getCalculator() {
      return new PercentileContLongCalculator();
    }
  }

  /**
   * The evaluator for percentile computation based on double.
   */
  public static class PercentileContDoubleEvaluator
      extends PercentileContEvaluator<Double, DoubleWritable> {
    @Override
    protected ArrayList<ObjectInspector> getPartialInspectors() {
      ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();

      foi.add(ObjectInspectorFactory.getStandardMapObjectInspector(
          PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
          PrimitiveObjectInspectorFactory.writableLongObjectInspector));
      foi.add(ObjectInspectorFactory.getStandardListObjectInspector(
          PrimitiveObjectInspectorFactory.writableDoubleObjectInspector));
      return foi;
    }

    @Override
    protected Double getInput(Object parameter, PrimitiveObjectInspector inputOI) {
      return PrimitiveObjectInspectorUtils.getDouble(parameter, inputOI);
    }

    @Override
    protected DoubleWritable wrapInput(Double input) {
      return new DoubleWritable(input);
    }

    protected DoubleWritable copyInput(DoubleWritable input) {
      return new DoubleWritable(input.get());
    }

    protected void sortEntries(List<Entry<DoubleWritable, LongWritable>> entriesList) {
      Collections.sort(entriesList, new DoubleComparator());
    }

    @Override
    protected PercentileCalculator<DoubleWritable> getCalculator() {
      return new PercentileContDoubleCalculator();
    }
  }

  /**
   * continuous percentile calculators
   */
  public static class PercentileContLongCalculator implements PercentileCalculator<LongWritable> {
    /**
     * Get the percentile value.
     */
    public double getPercentile(List<Entry<LongWritable, LongWritable>> entriesList,
        double position) {
      // We may need to do linear interpolation to get the exact percentile
      long lower = (long) Math.floor(position);
      long higher = (long) Math.ceil(position);

      // Linear search since this won't take much time from the total execution anyway
      // lower has the range of [0 .. total-1]
      // The first entry with accumulated count (lower+1) corresponds to the lower position.
      int i = 0;
      while (entriesList.get(i).getValue().get() < lower + 1) {
        i++;
      }

      long lowerKey = entriesList.get(i).getKey().get();
      if (higher == lower) {
        // no interpolation needed because position does not have a fraction
        return lowerKey;
      }

      if (entriesList.get(i).getValue().get() < higher + 1) {
        i++;
      }
      long higherKey = entriesList.get(i).getKey().get();

      if (higherKey == lowerKey) {
        // no interpolation needed because lower position and higher position has the same key
        return lowerKey;
      }

      // Linear interpolation to get the exact percentile
      return (higher - position) * lowerKey + (position - lower) * higherKey;
    }
  }

  public static class PercentileContDoubleCalculator
      implements PercentileCalculator<DoubleWritable> {

    public double getPercentile(List<Map.Entry<DoubleWritable, LongWritable>> entriesList,
        double position) {
      long lower = (long) Math.floor(position);
      long higher = (long) Math.ceil(position);

      int i = 0;
      while (entriesList.get(i).getValue().get() < lower + 1) {
        i++;
      }

      double lowerKey = entriesList.get(i).getKey().get();
      if (higher == lower) {
        return lowerKey;
      }

      if (entriesList.get(i).getValue().get() < higher + 1) {
        i++;
      }
      double higherKey = entriesList.get(i).getKey().get();

      if (higherKey == lowerKey) {
        return lowerKey;
      }

      return (higher - position) * lowerKey + (position - lower) * higherKey;
    }
  }
}
