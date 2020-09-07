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

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTypeSystemImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveMergeableAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSqlFunction;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hive.plugin.api.HiveUDFPlugin;

/**
 * Registers functions from the DataSketches library as builtin functions.
 *
 * In an effort to show a more consistent
 */
public final class DataSketchesFunctions implements HiveUDFPlugin {

  public static final DataSketchesFunctions INSTANCE = new DataSketchesFunctions();

  private static final String DATASKETCHES_PREFIX = "ds";

  public static final String DATA_TO_SKETCH = "sketch";
  public static final String SKETCH_TO_ESTIMATE = "estimate";
  private static final String SKETCH_TO_ESTIMATE_WITH_ERROR_BOUNDS = "estimate_bounds";
  private static final String SKETCH_TO_STRING = "stringify";
  private static final String UNION_SKETCH = "union";
  private static final String UNION_SKETCH1 = "union_f";
  public static final String GET_N = "n";
  public static final String GET_CDF = "cdf";
  private static final String GET_PMF = "pmf";
  private static final String GET_QUANTILES = "quantiles";
  public static final String GET_QUANTILE = "quantile";
  public static final String GET_RANK = "rank";
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

  private final Map<String, SketchDescriptor> sketchClasses;
  private final ArrayList<UDFDescriptor> descriptors;

  private DataSketchesFunctions() {
    this.sketchClasses = new HashMap<>();
    this.descriptors = new ArrayList<>();
    registerHll();
    registerCpc();
    registerKll();
    registerTheta();
    registerTuple();
    registerQuantiles();
    registerFrequencies();

    buildCalciteFns();
    buildDescritors();
  }

  @Override
  public Iterable<UDFDescriptor> getDescriptors() {
    return descriptors;
  }

  public SketchFunctionDescriptor getSketchFunction(String className, String function) {
    if (!sketchClasses.containsKey(className)) {
      throw new IllegalArgumentException(String.format("Sketch-class '%s' doesn't exists", className));
    }
    SketchDescriptor sc = sketchClasses.get(className);
    if (!sc.fnMap.containsKey(function)) {
      throw new IllegalArgumentException(
          String.format("The Sketch-class '%s' doesn't have a '%s' method", className, function));
    }
    return sketchClasses.get(className).fnMap.get(function);
  }

  private void buildDescritors() {
    for (SketchDescriptor sketchDescriptor : sketchClasses.values()) {
      descriptors.addAll(sketchDescriptor.fnMap.values());
    }
  }

  private void buildCalciteFns() {
    for (SketchDescriptor sd : sketchClasses.values()) {

      registerAsHiveFunction(sd.fnMap.get(SKETCH_TO_ESTIMATE));
      registerAsHiveFunction(sd.fnMap.get(GET_QUANTILE));
      registerAsHiveFunction(sd.fnMap.get(GET_CDF));
      registerAsHiveFunction(sd.fnMap.get(GET_N));
      registerAsHiveFunction(sd.fnMap.get(GET_RANK));

      // Mergability is exposed to Calcite; which enables to use it during rollup.
      RelProtoDataType sketchType = RelDataTypeImpl.proto(SqlTypeName.BINARY, true);

      SketchFunctionDescriptor sketchSFD = sd.fnMap.get(DATA_TO_SKETCH);
      SketchFunctionDescriptor unionSFD = sd.fnMap.get(UNION_SKETCH);

      if (sketchSFD == null || unionSFD == null) {
        continue;
      }

      HiveMergeableAggregate unionFn = new HiveMergeableAggregate(unionSFD.name,
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(sketchType),
          InferTypes.ANY_NULLABLE,
          OperandTypes.family(),
          null);

      HiveMergeableAggregate sketchFn = new HiveMergeableAggregate(sketchSFD.name,
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(sketchType),
          InferTypes.ANY_NULLABLE,
          OperandTypes.family(),
          unionFn);


      unionSFD.setCalciteFunction(unionFn);
      sketchSFD.setCalciteFunction(sketchFn);


    }
  }

  private void registerAsHiveFunction(SketchFunctionDescriptor sfd) {
    if (sfd != null && sfd.getReturnRelDataType().isPresent()) {
      SqlFunction cdfFn =
          new HiveSqlFunction(sfd.name,
              SqlKind.OTHER_FUNCTION,
              ReturnTypes.explicit(sfd.getReturnRelDataType().get()),
              InferTypes.ANY_NULLABLE,
              OperandTypes.family(),
              SqlFunctionCategory.USER_DEFINED_FUNCTION,
              true,
              false);

      sfd.setCalciteFunction(cdfFn);
    }
  }

  private void registerHiveFunctionsInternal(Registry system) {
    for (SketchDescriptor sketchDescriptor : sketchClasses.values()) {
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

  static class SketchFunctionDescriptor implements HiveUDFPlugin.UDFDescriptor {
    String name;
    Class<?> udfClass;
    private SqlFunction calciteFunction;
    private Type returnType;

    public SketchFunctionDescriptor(String name, Class<?> udfClass) {
      this.name = name;
      this.udfClass = udfClass;
    }

    @Override
    public Class<?> getUDFClass() {
      return udfClass;
    }

    @Override
    public String getFunctionName() {
      return name;
    }

    public Optional<RelDataType> getReturnRelDataType() {
      if (returnType == null) {
        return Optional.empty();
      } else {
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl(new HiveTypeSystemImpl());
        if (returnType instanceof ParameterizedType) {
          ParameterizedType parameterizedType = (ParameterizedType) returnType;
          if (parameterizedType.getRawType() == List.class) {
            final RelDataType componentRelType = typeFactory.createType(parameterizedType.getActualTypeArguments()[0]);
            return Optional
                .of(typeFactory.createArrayType(typeFactory.createTypeWithNullability(componentRelType, true), -1));
          }
          return Optional.empty();
        }
        return Optional.of(typeFactory.createType(returnType));
      }
    }

    public void setReturnType(Type type) {
      this.returnType = type;
    }

    @Override
    public Optional<SqlFunction> getCalciteFunction() {
      return Optional.ofNullable(calciteFunction);
    }

    public void setCalciteFunction(SqlFunction calciteFunction) {
      this.calciteFunction = calciteFunction;
    }

    @Override
    public String toString() {
      return getClass().getCanonicalName() + "[" + name + "]";
    }
  }

  private static class SketchDescriptor {
    Map<String, SketchFunctionDescriptor> fnMap;
    private String functionPrefix;

    public SketchDescriptor(String string) {
      fnMap = new HashMap<String, SketchFunctionDescriptor>();
      functionPrefix = DATASKETCHES_PREFIX + "_" + string + "_";
    }

    private void register(String name, Class<?> clazz) {
      SketchFunctionDescriptor value = new SketchFunctionDescriptor(functionPrefix + name, clazz);
      if (UDF.class.isAssignableFrom(clazz)) {
        Optional<Method> evaluateMethod = getEvaluateMethod(clazz);
        if (evaluateMethod.isPresent()) {
          value.setReturnType(evaluateMethod.get().getGenericReturnType());
        }
      }
      fnMap.put(name, value);
    }

    private Optional<Method> getEvaluateMethod(Class<?> clazz) {
      List<Method> evaluateMethods = new ArrayList<Method>();
      for (Method method : clazz.getMethods()) {
        if ("evaluate".equals(method.getName())) {
          evaluateMethods.add(method);
        }
      }
      if (evaluateMethods.size() > 0) {
        return Optional.of(evaluateMethods.get(0));
      } else {
        return Optional.empty();
      }
    }
  }

  private void registerHll() {
    SketchDescriptor sd = new SketchDescriptor("hll");
    sd.register(DATA_TO_SKETCH, org.apache.datasketches.hive.hll.DataToSketchUDAF.class);
    sd.register(SKETCH_TO_ESTIMATE_WITH_ERROR_BOUNDS,
        org.apache.datasketches.hive.hll.SketchToEstimateAndErrorBoundsUDF.class);
    sd.register(SKETCH_TO_ESTIMATE, org.apache.datasketches.hive.hll.SketchToEstimateUDF.class);
    sd.register(SKETCH_TO_STRING, org.apache.datasketches.hive.hll.SketchToStringUDF.class);
    sd.register(UNION_SKETCH1, org.apache.datasketches.hive.hll.UnionSketchUDF.class);
    sd.register(UNION_SKETCH, org.apache.datasketches.hive.hll.UnionSketchUDAF.class);
    sketchClasses.put("hll", sd);
  }

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
    sketchClasses.put("cpc", sd);
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
    sketchClasses.put("kll", sd);
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
    sketchClasses.put("theta", sd);

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
    sketchClasses.put("tuple_arrayofdouble", sd);
  }

  private void registerTupleDoubleSummary() {
    SketchDescriptor sd = new SketchDescriptor("tuple_doublesummary");
    sd.register(DATA_TO_SKETCH, org.apache.datasketches.hive.tuple.DataToDoubleSummarySketchUDAF.class);
    //registerUDF(org.apache.datasketches.hive.theta.SketchToStringUDF.class, p + SKETCH_TO_STRING);
    sd.register(UNION_SKETCH, org.apache.datasketches.hive.tuple.UnionDoubleSummarySketchUDAF.class);
    sd.register(SKETCH_TO_ESTIMATE, org.apache.datasketches.hive.tuple.DoubleSummarySketchToEstimatesUDF.class);
    sd.register(SKETCH_TO_PERCENTILE, org.apache.datasketches.hive.tuple.DoubleSummarySketchToPercentileUDF.class);
    sketchClasses.put("tuple_doublesummary", sd);
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
    sketchClasses.put("freq", sd);
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
    sketchClasses.put("quantile_strings", sd);
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
    sketchClasses.put("quantile_doubles", sd);
  }

}
