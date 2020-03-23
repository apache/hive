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

package org.apache.hadoop.hive.ql.exec;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;

/**
 * Registers functions from the DataSketches library as builtin functions.
 *
 * In an effort to show a more consistent
 */
public class DataSketchesFunctions {

  private static final String DATASKETCHES_PREFIX = "ds";
  private static final String DATA_TO_SKETCH = "sketch";
  private static final String SKETCH_TO_ESTIMATE_WITH_ERROR_BOUNDS = "estimate_bounds";
  private static final String SKETCH_TO_ESTIMATE = "estimate";
  private static final String SKETCH_TO_STRING = "stringify";
  private static final String UNION_SKETCH = "union";
  private static final String UNION_SKETCH1 = "union_f";
  private static final String GET_N = "n";
  private static final String GET_CDF = "cdf";
  private static final String GET_PMF = "pmf";
  private static final String GET_QUANTILES = "quantiles";
  private static final String GET_QUANTILE = "quantile";
  private static final String GET_RANK = "rank";
  private static final String INTERSECT_SKETCH = "intersect";
  private static final String INTERSECT_SKETCH1 = "intersect_f";
  private static final String EXCLUDE_SKETCH = "exclude";
  private static final String GET_K = "k";
  private static final String GET_FREQUENT_ITEMS = "frequent_items";
  private static final String T_TEST = "ttest";
  private static final String SKETCH_TO_MEANS = "means";
  private static final String SKETCH_TO_NUMBER_OF_RETAINED_ENTRIES = "n_retained";
  private static final String SKETCH_TO_QUANTILES_SKETCH = "quantiles_sketch";
  private static final String SKETCH_TO_VALUES = "values";
  private static final String SKETCH_TO_VARIANCES = "variances";
  private static final String SKETCH_TO_PERCENTILE = "percentile";

  private final Registry system;
  private List<SketchDescriptor> sketchClasses;

  public DataSketchesFunctions(Registry system) {
    this.system = system;
    this.sketchClasses = new ArrayList<SketchDescriptor>();

    String prefix = DATASKETCHES_PREFIX;
    //    dsf.registerHll();
    registerHll();
    DataSketchesFunctions dsf = this;
    dsf.registerCpc(prefix);
    dsf.registerKll(prefix);
    dsf.registerTheta(prefix);
    dsf.registerTuple(prefix);
    dsf.registerQuantiles(prefix);
    dsf.registerFrequencies(prefix);
  }

  public static void register(Registry system) {
    DataSketchesFunctions dsf = new DataSketchesFunctions(system);
  }

  static class SketchFunctionDescriptor {
    String name;
    Class<?> udfClass;
  }

  static class SketchDescriptor {
    List<SketchFunctionDescriptor> li;

    public SketchDescriptor(String string) {
      String x = DATASKETCHES_PREFIX;
      // TODO Auto-generated constructor stub
    }

    public void registerUDAF(Class<?> class1, String p, String dataToSketch) {
      register(dataToSketch, class1);
      // TODO Auto-generated method stub

    }

    private void register(String dataToSketch, Class<?> class1) {
      // TODO Auto-generated method stub

    }

    public void registerUDF(Class<?> class1, String p,
        String sketchToEstimateWithErrorBounds) {
      register(sketchToEstimateWithErrorBounds, class1);
    }


    public void registerUDTF(Class<?> class1, String p, String getFrequentItems) {
      // TODO Auto-generated method stub

    }
  }

  private void registerHll() {
    String p = "asd";
    SketchDescriptor sd = new SketchDescriptor("hll");
    sd.registerUDAF(org.apache.datasketches.hive.hll.DataToSketchUDAF.class, p, DATA_TO_SKETCH);
    sd.registerUDF(org.apache.datasketches.hive.hll.SketchToEstimateAndErrorBoundsUDF.class, p,
        SKETCH_TO_ESTIMATE_WITH_ERROR_BOUNDS);
    sd.registerUDF(org.apache.datasketches.hive.hll.SketchToEstimateUDF.class, p, SKETCH_TO_ESTIMATE);
    sd.registerUDF(org.apache.datasketches.hive.hll.SketchToStringUDF.class, p, SKETCH_TO_STRING);
    sd.registerUDF(org.apache.datasketches.hive.hll.UnionSketchUDF.class, p, UNION_SKETCH1);
    sd.registerUDAF(org.apache.datasketches.hive.hll.UnionSketchUDAF.class, p, UNION_SKETCH);
    sketchClasses.add(sd);
  }

  private void registerCpc(String prefix) {
    String p = prefix + "_cpc_";
    SketchDescriptor sd = new SketchDescriptor("cpc");
    sd.registerUDAF(org.apache.datasketches.hive.cpc.DataToSketchUDAF.class, p, DATA_TO_SKETCH);
    // FIXME: normalize GetEstimateAndErrorBoundsUDF vs SketchToEstimateAndErrorBoundsUDF
    sd.registerUDF(org.apache.datasketches.hive.cpc.GetEstimateAndErrorBoundsUDF.class, p,
        SKETCH_TO_ESTIMATE_WITH_ERROR_BOUNDS);
    // FIXME: normalize GetEstimateUDF vs SketchToEstimateUDF
    sd.registerUDF(org.apache.datasketches.hive.cpc.GetEstimateUDF.class, p, SKETCH_TO_ESTIMATE);
    sd.registerUDF(org.apache.datasketches.hive.cpc.SketchToStringUDF.class, p, SKETCH_TO_STRING);
    sd.registerUDF(org.apache.datasketches.hive.cpc.UnionSketchUDF.class, p, UNION_SKETCH1);
    sd.registerUDAF(org.apache.datasketches.hive.cpc.UnionSketchUDAF.class, p, UNION_SKETCH);
    sketchClasses.add(sd);
  }

  private void registerKll(String prefix) {
    String p = prefix + "_kll_";
    SketchDescriptor sd = new SketchDescriptor("kll");
    sd.registerUDAF(org.apache.datasketches.hive.kll.DataToSketchUDAF.class, p, DATA_TO_SKETCH);
    sd.registerUDF(org.apache.datasketches.hive.kll.SketchToStringUDF.class, p, SKETCH_TO_STRING);
    //    registerUDF(org.apache.datasketches.hive.kll.UnionSketchUDF.class, p , UNION_SKETCH);
    sd.registerUDAF(org.apache.datasketches.hive.kll.UnionSketchUDAF.class, p, UNION_SKETCH);

    sd.registerUDF(org.apache.datasketches.hive.kll.GetNUDF.class, p, GET_N);
    sd.registerUDF(org.apache.datasketches.hive.kll.GetCdfUDF.class, p, GET_CDF);
    sd.registerUDF(org.apache.datasketches.hive.kll.GetPmfUDF.class, p, GET_PMF);
    sd.registerUDF(org.apache.datasketches.hive.kll.GetQuantilesUDF.class, p, GET_QUANTILES);
    sd.registerUDF(org.apache.datasketches.hive.kll.GetQuantileUDF.class, p, GET_QUANTILE);
    sd.registerUDF(org.apache.datasketches.hive.kll.GetRankUDF.class, p, GET_RANK);
    sketchClasses.add(sd);
  }

  private void registerTheta(String prefix) {
    String p = prefix + "_theta_";
    SketchDescriptor sd = new SketchDescriptor("kll");
    sd.registerUDAF(org.apache.datasketches.hive.theta.DataToSketchUDAF.class, p, DATA_TO_SKETCH);
    // FIXME: missing?
    //registerUDF(org.apache.datasketches.hive.theta.SketchToStringUDF.class, p + SKETCH_TO_STRING);
    sd.registerUDF(org.apache.datasketches.hive.theta.UnionSketchUDF.class, p, UNION_SKETCH1);
    sd.registerUDAF(org.apache.datasketches.hive.theta.UnionSketchUDAF.class, p, UNION_SKETCH);
    sd.registerUDF(org.apache.datasketches.hive.theta.IntersectSketchUDF.class, p, INTERSECT_SKETCH1);
    sd.registerUDAF(org.apache.datasketches.hive.theta.IntersectSketchUDAF.class, p, INTERSECT_SKETCH);
    sd.registerUDF(org.apache.datasketches.hive.theta.EstimateSketchUDF.class, p, SKETCH_TO_ESTIMATE);
    sd.registerUDF(org.apache.datasketches.hive.theta.ExcludeSketchUDF.class, p, EXCLUDE_SKETCH);
    sketchClasses.add(sd);

  }

  private void registerTuple(String prefix) {
    registerTupleArrayOfDoubles(prefix + "_tuple_arrayofdouble");
    registerTupleDoubleSummary(prefix + "_tuple_doublesummary");
  }

  private void registerTupleArrayOfDoubles(String string) {
    String p = string + "_";
    SketchDescriptor sd = new SketchDescriptor("tuple_arrayofdouble");
    sd.registerUDAF(org.apache.datasketches.hive.tuple.DataToArrayOfDoublesSketchUDAF.class, p, DATA_TO_SKETCH);
    // FIXME: missing?
    //registerUDF(org.apache.datasketches.hive.theta.SketchToStringUDF.class, p , SKETCH_TO_STRING);
    sd.registerUDAF(org.apache.datasketches.hive.tuple.UnionArrayOfDoublesSketchUDAF.class, p, UNION_SKETCH);
    sd.registerUDF(org.apache.datasketches.hive.tuple.ArrayOfDoublesSketchesTTestUDF.class, p, T_TEST);
    sd.registerUDF(org.apache.datasketches.hive.tuple.ArrayOfDoublesSketchToEstimatesUDF.class, p, SKETCH_TO_ESTIMATE);
    sd.registerUDF(org.apache.datasketches.hive.tuple.ArrayOfDoublesSketchToEstimateAndErrorBoundsUDF.class, p,
        SKETCH_TO_ESTIMATE_WITH_ERROR_BOUNDS);
    sd.registerUDF(org.apache.datasketches.hive.tuple.ArrayOfDoublesSketchToMeansUDF.class, p, SKETCH_TO_MEANS);
    sd.registerUDF(org.apache.datasketches.hive.tuple.ArrayOfDoublesSketchToNumberOfRetainedEntriesUDF.class, p,
        SKETCH_TO_NUMBER_OF_RETAINED_ENTRIES);
    sd.registerUDF(org.apache.datasketches.hive.tuple.ArrayOfDoublesSketchToQuantilesSketchUDF.class, p,
        SKETCH_TO_QUANTILES_SKETCH);
    sd.registerUDTF(org.apache.datasketches.hive.tuple.ArrayOfDoublesSketchToValuesUDTF.class, p, SKETCH_TO_VALUES);
    sd.registerUDF(org.apache.datasketches.hive.tuple.ArrayOfDoublesSketchToVariancesUDF.class, p, SKETCH_TO_VARIANCES);
    sketchClasses.add(sd);
  }

  private void registerTupleDoubleSummary(String string) {
    String p = string + "_";
    SketchDescriptor sd = new SketchDescriptor("tuple_doublesummary");
    sd.registerUDAF(org.apache.datasketches.hive.tuple.DataToDoubleSummarySketchUDAF.class, p, DATA_TO_SKETCH);
    //registerUDF(org.apache.datasketches.hive.theta.SketchToStringUDF.class, p + SKETCH_TO_STRING);
    sd.registerUDAF(org.apache.datasketches.hive.tuple.UnionDoubleSummarySketchUDAF.class, p, UNION_SKETCH);
    sd.registerUDF(org.apache.datasketches.hive.tuple.DoubleSummarySketchToEstimatesUDF.class, p, SKETCH_TO_ESTIMATE);
    sd.registerUDF(org.apache.datasketches.hive.tuple.DoubleSummarySketchToPercentileUDF.class, p,
        SKETCH_TO_PERCENTILE);
    sketchClasses.add(sd);
  }

  private void registerQuantiles(String prefix) {
    registerQuantilesString(prefix + "_quantile");
    registerQuantilesDoubles(prefix + "_quantile");
  }

  private void registerFrequencies(String prefix) {
    String p = prefix + "_freq_";
    SketchDescriptor sd = new SketchDescriptor("freq");

    sd.registerUDAF(org.apache.datasketches.hive.frequencies.DataToStringsSketchUDAF.class, p, DATA_TO_SKETCH);
    // FIXME: missing?
    //registerUDF(org.apache.datasketches.hive.frequencies.DoublesSketchToStringUDF.class, p + SKETCH_TO_STRING);
    //registerUDF(org.apache.datasketches.hive.quantiles.UnionItemsSketchUDAF.class, p + UNION_SKETCH);
    sd.registerUDAF(org.apache.datasketches.hive.frequencies.UnionStringsSketchUDAF.class, p, UNION_SKETCH);
    sd.registerUDTF(org.apache.datasketches.hive.frequencies.GetFrequentItemsFromStringsSketchUDTF.class, p,
        GET_FREQUENT_ITEMS);
    sketchClasses.add(sd);
  }

  private void registerQuantilesString(String prefix) {
    String p = prefix + "_strings_";
    SketchDescriptor sd = new SketchDescriptor("quantile_strings");
    sd.registerUDAF(org.apache.datasketches.hive.quantiles.DataToStringsSketchUDAF.class, p, DATA_TO_SKETCH);
    sd.registerUDF(org.apache.datasketches.hive.quantiles.StringsSketchToStringUDF.class, p, SKETCH_TO_STRING);
    //registerUDF(org.apache.datasketches.hive.quantiles.UnionItemsSketchUDAF.class, p , UNION_SKETCH);
    sd.registerUDAF(org.apache.datasketches.hive.quantiles.UnionStringsSketchUDAF.class, p, UNION_SKETCH);
    sd.registerUDF(org.apache.datasketches.hive.quantiles.GetNFromStringsSketchUDF.class, p, GET_N);
    sd.registerUDF(org.apache.datasketches.hive.quantiles.GetKFromStringsSketchUDF.class, p, GET_K);
    sd.registerUDF(org.apache.datasketches.hive.quantiles.GetCdfFromStringsSketchUDF.class, p, GET_CDF);
    sd.registerUDF(org.apache.datasketches.hive.quantiles.GetPmfFromStringsSketchUDF.class, p, GET_PMF);
    sd.registerUDF(org.apache.datasketches.hive.quantiles.GetQuantileFromStringsSketchUDF.class, p, GET_QUANTILE);
    sd.registerUDF(org.apache.datasketches.hive.quantiles.GetQuantilesFromStringsSketchUDF.class, p, GET_QUANTILES);
    sketchClasses.add(sd);
  }

  private void registerQuantilesDoubles(String prefix) {
    String p = prefix + "_doubles_";
    SketchDescriptor sd = new SketchDescriptor("quantile_doubles");
    sd.registerUDAF(org.apache.datasketches.hive.quantiles.DataToDoublesSketchUDAF.class, p, DATA_TO_SKETCH);
    sd.registerUDF(org.apache.datasketches.hive.quantiles.DoublesSketchToStringUDF.class, p, SKETCH_TO_STRING);
    //registerUDF(org.apache.datasketches.hive.quantiles.UnionItemsSketchUDAF.class, p , UNION_SKETCH);
    sd.registerUDAF(org.apache.datasketches.hive.quantiles.UnionDoublesSketchUDAF.class, p, UNION_SKETCH);
    sd.registerUDF(org.apache.datasketches.hive.quantiles.GetNFromDoublesSketchUDF.class, p, GET_N);
    sd.registerUDF(org.apache.datasketches.hive.quantiles.GetKFromDoublesSketchUDF.class, p, GET_K);
    sd.registerUDF(org.apache.datasketches.hive.quantiles.GetCdfFromDoublesSketchUDF.class, p, GET_CDF);
    sd.registerUDF(org.apache.datasketches.hive.quantiles.GetPmfFromDoublesSketchUDF.class, p, GET_PMF);
    sd.registerUDF(org.apache.datasketches.hive.quantiles.GetQuantileFromDoublesSketchUDF.class, p, GET_QUANTILE);
    sd.registerUDF(org.apache.datasketches.hive.quantiles.GetQuantilesFromDoublesSketchUDF.class, p, GET_QUANTILES);
    sketchClasses.add(sd);
  }

  private void registerUDF(Class<? extends UDF> udfClass, String name) {
    system.registerUDF(name, udfClass, false);
  }

  private void registerUDAF(Class<? extends GenericUDAFResolver2> udafClass, String name) {
    try {
      system.registerGenericUDAF(name, udafClass.newInstance());
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException("Unable to register: " + name, e);
    }
  }

  private void registerUDTF(Class<? extends GenericUDTF> udtfClass, String name) {
    system.registerGenericUDTF(name, udtfClass);
  }

  public static boolean isUnionFunction(String udfName) {
    return (udfName.startsWith(DATASKETCHES_PREFIX + "_") && udfName.endsWith("_" + UNION_SKETCH));
  }

  public static boolean isSketchFunction(String udfName) {
    return (udfName.startsWith(DATASKETCHES_PREFIX + "_") && udfName.endsWith("_" + DATA_TO_SKETCH));
  }

  public static String getUnionFor(String hiveUdfName) {
    if (isSketchFunction(hiveUdfName)) {
      return hiveUdfName.replaceFirst("_" + DATA_TO_SKETCH + "$", "_" + UNION_SKETCH);
    } else {
      throw new RuntimeException("error; unexpected udf name: " + hiveUdfName);
    }
  }

  public static void registerCalciteFunctions(Consumer<Pair<String, SqlOperator>> staticBlockBuilder) {
    // TODO Auto-generated method stub

  }

}
