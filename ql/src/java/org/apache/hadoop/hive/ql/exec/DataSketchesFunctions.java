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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveMergeablAggregate;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;

/**
 * Registers functions from the DataSketches library as builtin functions.
 *
 * In an effort to show a more consistent
 */
public class DataSketchesFunctions {

<<<<<<< HEAD
=======
  private static final String DATASKETCHES_PREFIX = "ds";

>>>>>>> kgyrtkirk/HIVE-23030-rollup-union
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

<<<<<<< HEAD
  private final Registry system;

  public DataSketchesFunctions(Registry system) {
    this.system = system;
  }

  public static void register(Registry system) {
    DataSketchesFunctions dsf = new DataSketchesFunctions(system);
    String prefix = "ds";
    dsf.registerHll(prefix);
    dsf.registerCpc(prefix);
    dsf.registerKll(prefix);
    dsf.registerTheta(prefix);
    dsf.registerTuple(prefix);
    dsf.registerQuantiles(prefix);
    dsf.registerFrequencies(prefix);
  }

  private void registerHll(String prefix) {
    String p = prefix + "_hll_";
    registerUDAF(org.apache.datasketches.hive.hll.DataToSketchUDAF.class, p + DATA_TO_SKETCH);
    registerUDF(org.apache.datasketches.hive.hll.SketchToEstimateAndErrorBoundsUDF.class,
        p + SKETCH_TO_ESTIMATE_WITH_ERROR_BOUNDS);
    registerUDF(org.apache.datasketches.hive.hll.SketchToEstimateUDF.class, p + SKETCH_TO_ESTIMATE);
    registerUDF(org.apache.datasketches.hive.hll.SketchToStringUDF.class, p + SKETCH_TO_STRING);
    registerUDF(org.apache.datasketches.hive.hll.UnionSketchUDF.class, p + UNION_SKETCH1);
    registerUDAF(org.apache.datasketches.hive.hll.UnionSketchUDAF.class, p + UNION_SKETCH);
  }

  private void registerCpc(String prefix) {
    String p = prefix + "_cpc_";
    registerUDAF(org.apache.datasketches.hive.cpc.DataToSketchUDAF.class, p + DATA_TO_SKETCH);
    // FIXME: normalize GetEstimateAndErrorBoundsUDF vs SketchToEstimateAndErrorBoundsUDF
    registerUDF(org.apache.datasketches.hive.cpc.GetEstimateAndErrorBoundsUDF.class,
        p + SKETCH_TO_ESTIMATE_WITH_ERROR_BOUNDS);
    // FIXME: normalize GetEstimateUDF vs SketchToEstimateUDF
    registerUDF(org.apache.datasketches.hive.cpc.GetEstimateUDF.class, p + SKETCH_TO_ESTIMATE);
    registerUDF(org.apache.datasketches.hive.cpc.SketchToStringUDF.class, p + SKETCH_TO_STRING);
    registerUDF(org.apache.datasketches.hive.cpc.UnionSketchUDF.class, p + UNION_SKETCH1);
    registerUDAF(org.apache.datasketches.hive.cpc.UnionSketchUDAF.class, p + UNION_SKETCH);
  }

  private void registerKll(String prefix) {
    String p = prefix + "_kll_";
    registerUDAF(org.apache.datasketches.hive.kll.DataToSketchUDAF.class, p + DATA_TO_SKETCH);
    registerUDF(org.apache.datasketches.hive.kll.SketchToStringUDF.class, p + SKETCH_TO_STRING);
    //    registerUDF(org.apache.datasketches.hive.kll.UnionSketchUDF.class, p + UNION_SKETCH);
    registerUDAF(org.apache.datasketches.hive.kll.UnionSketchUDAF.class, p + UNION_SKETCH);

    registerUDF(org.apache.datasketches.hive.kll.GetNUDF.class, p + GET_N);
    registerUDF(org.apache.datasketches.hive.kll.GetCdfUDF.class, p + GET_CDF);
    registerUDF(org.apache.datasketches.hive.kll.GetPmfUDF.class, p + GET_PMF);
    registerUDF(org.apache.datasketches.hive.kll.GetQuantilesUDF.class, p + GET_QUANTILES);
    registerUDF(org.apache.datasketches.hive.kll.GetQuantileUDF.class, p + GET_QUANTILE);
    registerUDF(org.apache.datasketches.hive.kll.GetRankUDF.class, p + GET_RANK);
  }

  private void registerTheta(String prefix) {
    String p = prefix + "_theta_";
    registerUDAF(org.apache.datasketches.hive.theta.DataToSketchUDAF.class, p + DATA_TO_SKETCH);
    // FIXME: missing?
    //registerUDF(org.apache.datasketches.hive.theta.SketchToStringUDF.class, p + SKETCH_TO_STRING);
    registerUDF(org.apache.datasketches.hive.theta.UnionSketchUDF.class, p + UNION_SKETCH1);
    registerUDAF(org.apache.datasketches.hive.theta.UnionSketchUDAF.class, p + UNION_SKETCH);
    registerUDF(org.apache.datasketches.hive.theta.IntersectSketchUDF.class, p + INTERSECT_SKETCH1);
    registerUDAF(org.apache.datasketches.hive.theta.IntersectSketchUDAF.class, p + INTERSECT_SKETCH);
    registerUDF(org.apache.datasketches.hive.theta.EstimateSketchUDF.class, p + SKETCH_TO_ESTIMATE);
    registerUDF(org.apache.datasketches.hive.theta.ExcludeSketchUDF.class, p + EXCLUDE_SKETCH);
=======
  private List<SketchDescriptor> sketchClasses;
>>>>>>> kgyrtkirk/HIVE-23030-rollup-union

  public DataSketchesFunctions() {
    this.sketchClasses = new ArrayList<SketchDescriptor>();
    registerHll();
    registerCpc();
    registerKll();
    registerTheta();
    registerTuple();
    registerQuantiles();
    registerFrequencies();
  }

  public static void registerHiveFunctions(Registry system) {
    new DataSketchesFunctions().registerHiveFunctionsInternal(system);
  }

  /**
   * Registers functions which should communicate special features of the functions.
   *
   * Mergability is exposed to Calcite; which enables to use it during rollup.
   */
  public static void registerCalciteFunctions(Consumer<Pair<String, SqlOperator>> r) {
    new DataSketchesFunctions().registerCalciteInternal(r);
  }

  private void registerCalciteInternal(Consumer<Pair<String, SqlOperator>> r) {

    for (SketchDescriptor sd : sketchClasses) {

      RelProtoDataType sketchType = RelDataTypeImpl.proto(SqlTypeName.BINARY, true);

      SketchFunctionDescriptor sketchSFD = sd.fnMap.get(DATA_TO_SKETCH);
      SketchFunctionDescriptor unionSFD = sd.fnMap.get(UNION_SKETCH);

      if (sketchSFD == null || unionSFD == null) {
        continue;
      }

      HiveMergeablAggregate unionFn = new HiveMergeablAggregate(unionSFD.name,
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(sketchType),
          InferTypes.ANY_NULLABLE,
          OperandTypes.family(),
          null);

      HiveMergeablAggregate sketchFn = new HiveMergeablAggregate(sketchSFD.name,
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(sketchType),
          InferTypes.ANY_NULLABLE,
          OperandTypes.family(),
          unionFn);

      r.accept(new Pair<String, SqlOperator>(unionSFD.name, unionFn));
      r.accept(new Pair<String, SqlOperator>(sketchSFD.name, sketchFn));
    }

  }


  private void registerHiveFunctionsInternal(Registry system2) {
    Registry system = system2;
    for (SketchDescriptor sketchDescriptor : sketchClasses) {
      Collection<SketchFunctionDescriptor> functions = sketchDescriptor.fnMap.values();
      for (SketchFunctionDescriptor fn : functions) {
        if (UDF.class.isAssignableFrom(fn.udfClass)) {
          system.registerUDF(fn.name, (Class<? extends UDF>) fn.udfClass, false);
          continue;
        }
        if (GenericUDAFResolver2.class.isAssignableFrom(fn.udfClass)) {
          String name = fn.name;
          try {
            system.registerGenericUDAF(name, ((Class<? extends GenericUDAFResolver2>) fn.udfClass).newInstance());
          } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException("Unable to register: " + name, e);
          }
          continue;
        }
        if (GenericUDTF.class.isAssignableFrom(fn.udfClass)) {
          system.registerGenericUDTF(fn.name, (Class<? extends GenericUDTF>) fn.udfClass);
          continue;
        }
        throw new RuntimeException("Don't know how to register: " + fn.name);
      }
    }

  }

  static class SketchFunctionDescriptor {
    String name;
    Class<?> udfClass;

    public SketchFunctionDescriptor(String name, Class<?> udfClass) {
      this.name = name;
      this.udfClass = udfClass;
    }
  }

  static class SketchDescriptor {
    Map<String, SketchFunctionDescriptor> fnMap;
    private String functionPrefix;

    public SketchDescriptor(String string) {
      fnMap = new HashMap<String, SketchFunctionDescriptor>();
      functionPrefix = DATASKETCHES_PREFIX + "_" + string + "_";
    }

    private void register(String name, Class<?> clazz) {
      fnMap.put(name, new SketchFunctionDescriptor(functionPrefix + name, clazz));
    }
  }

  private void registerHll() {
    String p = "asd";
    SketchDescriptor sd = new SketchDescriptor("hll");
    sd.register(DATA_TO_SKETCH, org.apache.datasketches.hive.hll.DataToSketchUDAF.class);
    sd.register(SKETCH_TO_ESTIMATE_WITH_ERROR_BOUNDS,
        org.apache.datasketches.hive.hll.SketchToEstimateAndErrorBoundsUDF.class);
    sd.register(SKETCH_TO_ESTIMATE, org.apache.datasketches.hive.hll.SketchToEstimateUDF.class);
    sd.register(SKETCH_TO_STRING, org.apache.datasketches.hive.hll.SketchToStringUDF.class);
    sd.register(UNION_SKETCH1, org.apache.datasketches.hive.hll.UnionSketchUDF.class);
    sd.register(UNION_SKETCH, org.apache.datasketches.hive.hll.UnionSketchUDAF.class);
    sketchClasses.add(sd);
  }

<<<<<<< HEAD
=======
  private void registerCpc() {
    SketchDescriptor sd = new SketchDescriptor("cpc");
    sd.register(DATA_TO_SKETCH, org.apache.datasketches.hive.cpc.DataToSketchUDAF.class);
    // FIXME: normalize GetEstimateAndErrorBoundsUDF vs SketchToEstimateAndErrorBoundsUDF
    sd.register(SKETCH_TO_ESTIMATE_WITH_ERROR_BOUNDS,
        org.apache.datasketches.hive.cpc.GetEstimateAndErrorBoundsUDF.class);
    // FIXME: normalize GetEstimateUDF vs SketchToEstimateUDF
    sd.register(SKETCH_TO_ESTIMATE, org.apache.datasketches.hive.cpc.GetEstimateUDF.class);
    sd.register(SKETCH_TO_STRING, org.apache.datasketches.hive.cpc.SketchToStringUDF.class);
    sd.register(UNION_SKETCH1, org.apache.datasketches.hive.cpc.UnionSketchUDF.class);
    sd.register(UNION_SKETCH, org.apache.datasketches.hive.cpc.UnionSketchUDAF.class);
    sketchClasses.add(sd);
  }

  private void registerKll() {
    SketchDescriptor sd = new SketchDescriptor("kll");
    sd.register(DATA_TO_SKETCH, org.apache.datasketches.hive.kll.DataToSketchUDAF.class);
    sd.register(SKETCH_TO_STRING, org.apache.datasketches.hive.kll.SketchToStringUDF.class);
    //    registerUDF(org.apache.datasketches.hive.kll.UnionSketchUDF.class, p , UNION_SKETCH);
    sd.register(UNION_SKETCH, org.apache.datasketches.hive.kll.UnionSketchUDAF.class);

    sd.register(GET_N, org.apache.datasketches.hive.kll.GetNUDF.class);
    sd.register(GET_CDF, org.apache.datasketches.hive.kll.GetCdfUDF.class);
    sd.register(GET_PMF, org.apache.datasketches.hive.kll.GetPmfUDF.class);
    sd.register(GET_QUANTILES, org.apache.datasketches.hive.kll.GetQuantilesUDF.class);
    sd.register(GET_QUANTILE, org.apache.datasketches.hive.kll.GetQuantileUDF.class);
    sd.register(GET_RANK, org.apache.datasketches.hive.kll.GetRankUDF.class);
    sketchClasses.add(sd);
  }

  private void registerTheta() {
    SketchDescriptor sd = new SketchDescriptor("theta");
    sd.register(DATA_TO_SKETCH, org.apache.datasketches.hive.theta.DataToSketchUDAF.class);
    // FIXME: missing?
    //registerUDF(org.apache.datasketches.hive.theta.SketchToStringUDF.class, p + SKETCH_TO_STRING);
    sd.register(UNION_SKETCH1, org.apache.datasketches.hive.theta.UnionSketchUDF.class);
    sd.register(UNION_SKETCH, org.apache.datasketches.hive.theta.UnionSketchUDAF.class);
    sd.register(INTERSECT_SKETCH1, org.apache.datasketches.hive.theta.IntersectSketchUDF.class);
    sd.register(INTERSECT_SKETCH, org.apache.datasketches.hive.theta.IntersectSketchUDAF.class);
    sd.register(SKETCH_TO_ESTIMATE, org.apache.datasketches.hive.theta.EstimateSketchUDF.class);
    sd.register(EXCLUDE_SKETCH, org.apache.datasketches.hive.theta.ExcludeSketchUDF.class);
    sketchClasses.add(sd);

  }

  private void registerTuple() {
    registerTupleArrayOfDoubles();
    registerTupleDoubleSummary();
  }

  private void registerTupleArrayOfDoubles() {
    SketchDescriptor sd = new SketchDescriptor("tuple_arrayofdouble");
    sd.register(DATA_TO_SKETCH, org.apache.datasketches.hive.tuple.DataToArrayOfDoublesSketchUDAF.class);
    // FIXME: missing?
    //registerUDF(org.apache.datasketches.hive.theta.SketchToStringUDF.class, p , SKETCH_TO_STRING);
    sd.register(UNION_SKETCH, org.apache.datasketches.hive.tuple.UnionArrayOfDoublesSketchUDAF.class);
    sd.register(T_TEST, org.apache.datasketches.hive.tuple.ArrayOfDoublesSketchesTTestUDF.class);
    sd.register(SKETCH_TO_ESTIMATE, org.apache.datasketches.hive.tuple.ArrayOfDoublesSketchToEstimatesUDF.class);
    sd.register(SKETCH_TO_ESTIMATE_WITH_ERROR_BOUNDS,
        org.apache.datasketches.hive.tuple.ArrayOfDoublesSketchToEstimateAndErrorBoundsUDF.class);
    sd.register(SKETCH_TO_MEANS, org.apache.datasketches.hive.tuple.ArrayOfDoublesSketchToMeansUDF.class);
    sd.register(SKETCH_TO_NUMBER_OF_RETAINED_ENTRIES,
        org.apache.datasketches.hive.tuple.ArrayOfDoublesSketchToNumberOfRetainedEntriesUDF.class);
    sd.register(SKETCH_TO_QUANTILES_SKETCH,
        org.apache.datasketches.hive.tuple.ArrayOfDoublesSketchToQuantilesSketchUDF.class);
    sd.register(SKETCH_TO_VALUES, org.apache.datasketches.hive.tuple.ArrayOfDoublesSketchToValuesUDTF.class);
    sd.register(SKETCH_TO_VARIANCES, org.apache.datasketches.hive.tuple.ArrayOfDoublesSketchToVariancesUDF.class);
    sketchClasses.add(sd);
  }

  private void registerTupleDoubleSummary() {
    SketchDescriptor sd = new SketchDescriptor("tuple_doublesummary");
    sd.register(DATA_TO_SKETCH, org.apache.datasketches.hive.tuple.DataToDoubleSummarySketchUDAF.class);
    //registerUDF(org.apache.datasketches.hive.theta.SketchToStringUDF.class, p + SKETCH_TO_STRING);
    sd.register(UNION_SKETCH, org.apache.datasketches.hive.tuple.UnionDoubleSummarySketchUDAF.class);
    sd.register(SKETCH_TO_ESTIMATE, org.apache.datasketches.hive.tuple.DoubleSummarySketchToEstimatesUDF.class);
    sd.register(SKETCH_TO_PERCENTILE, org.apache.datasketches.hive.tuple.DoubleSummarySketchToPercentileUDF.class);
    sketchClasses.add(sd);
  }

  private void registerQuantiles() {
    registerQuantilesString();
    registerQuantilesDoubles();
  }

  private void registerFrequencies() {
    SketchDescriptor sd = new SketchDescriptor("freq");

    sd.register(DATA_TO_SKETCH, org.apache.datasketches.hive.frequencies.DataToStringsSketchUDAF.class);
    // FIXME: missing?
    //registerUDF(org.apache.datasketches.hive.frequencies.DoublesSketchToStringUDF.class, p + SKETCH_TO_STRING);
    //registerUDF(org.apache.datasketches.hive.quantiles.UnionItemsSketchUDAF.class, p + UNION_SKETCH);
    sd.register(UNION_SKETCH, org.apache.datasketches.hive.frequencies.UnionStringsSketchUDAF.class);
    sd.register(GET_FREQUENT_ITEMS,
        org.apache.datasketches.hive.frequencies.GetFrequentItemsFromStringsSketchUDTF.class);
    sketchClasses.add(sd);
  }

  private void registerQuantilesString() {
    SketchDescriptor sd = new SketchDescriptor("quantile_strings");
    sd.register(DATA_TO_SKETCH, org.apache.datasketches.hive.quantiles.DataToStringsSketchUDAF.class);
    sd.register(SKETCH_TO_STRING, org.apache.datasketches.hive.quantiles.StringsSketchToStringUDF.class);
    //registerUDF(org.apache.datasketches.hive.quantiles.UnionItemsSketchUDAF.class, p , UNION_SKETCH);
    sd.register(UNION_SKETCH, org.apache.datasketches.hive.quantiles.UnionStringsSketchUDAF.class);
    sd.register(GET_N, org.apache.datasketches.hive.quantiles.GetNFromStringsSketchUDF.class);
    sd.register(GET_K, org.apache.datasketches.hive.quantiles.GetKFromStringsSketchUDF.class);
    sd.register(GET_CDF, org.apache.datasketches.hive.quantiles.GetCdfFromStringsSketchUDF.class);
    sd.register(GET_PMF, org.apache.datasketches.hive.quantiles.GetPmfFromStringsSketchUDF.class);
    sd.register(GET_QUANTILE, org.apache.datasketches.hive.quantiles.GetQuantileFromStringsSketchUDF.class);
    sd.register(GET_QUANTILES, org.apache.datasketches.hive.quantiles.GetQuantilesFromStringsSketchUDF.class);
    sketchClasses.add(sd);
  }

  private void registerQuantilesDoubles() {
    SketchDescriptor sd = new SketchDescriptor("quantile_doubles");
    sd.register(DATA_TO_SKETCH, org.apache.datasketches.hive.quantiles.DataToDoublesSketchUDAF.class);
    sd.register(SKETCH_TO_STRING, org.apache.datasketches.hive.quantiles.DoublesSketchToStringUDF.class);
    //registerUDF(org.apache.datasketches.hive.quantiles.UnionItemsSketchUDAF.class, p , UNION_SKETCH);
    sd.register(UNION_SKETCH, org.apache.datasketches.hive.quantiles.UnionDoublesSketchUDAF.class);
    sd.register(GET_N, org.apache.datasketches.hive.quantiles.GetNFromDoublesSketchUDF.class);
    sd.register(GET_K, org.apache.datasketches.hive.quantiles.GetKFromDoublesSketchUDF.class);
    sd.register(GET_CDF, org.apache.datasketches.hive.quantiles.GetCdfFromDoublesSketchUDF.class);
    sd.register(GET_PMF, org.apache.datasketches.hive.quantiles.GetPmfFromDoublesSketchUDF.class);
    sd.register(GET_QUANTILE, org.apache.datasketches.hive.quantiles.GetQuantileFromDoublesSketchUDF.class);
    sd.register(GET_QUANTILES, org.apache.datasketches.hive.quantiles.GetQuantilesFromDoublesSketchUDF.class);
    sketchClasses.add(sd);
  }
>>>>>>> kgyrtkirk/HIVE-23030-rollup-union
}
