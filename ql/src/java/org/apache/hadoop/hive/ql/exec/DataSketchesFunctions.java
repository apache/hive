package org.apache.hadoop.hive.ql.exec;


import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;

public class DataSketchesFunctions {

  private static final String DATA_TO_SKETCH = "datatosketch";
  private static final String SKETCH_TO_ESTIMATE_WITH_ERROR_BOUNDS = "sketchToEstimateWithErrorBounds";
  private static final String SKETCH_TO_ESTIMATE = "sketchToEstimate";
  private static final String SKETCH_TO_STRING = "sketchToString";
  private static final String UNION_SKETCH = "unionSketch";
  private static final String GET_N = "getN";
  private static final String GET_CDF = "getCdf";
  private static final String GET_PMF = "getPmf";
  private static final String GET_QUANTILES = "GetQuantiles";
  private static final String GET_QUANTILE = "GetQuantile";
  private static final String GET_RANK = "getRank";
  private static final String INTERSECT_SKETCH = "intersectSketch";
  private static final String EXCLUDE_SKETCH = "excludeSketch";
  private static final String GET_K = "getK";
  private static final String GET_FREQUENT_ITEMS = "getFrequentItems";

  private final Registry system;

  public DataSketchesFunctions(Registry system) {
    this.system = system;
  }

  public static void register(Registry system) {
    DataSketchesFunctions dsf = new DataSketchesFunctions(system);
    dsf.registerHll("approx_hll");
    dsf.registerCpc("approx_cpc");
    dsf.registerKll("approx_kll");
    dsf.registerTheta("approx_theta");
  }

  private void registerHll(String prefix) {
    String p = prefix + "_";
    registerUDAF(org.apache.datasketches.hive.hll.DataToSketchUDAF.class, p + DATA_TO_SKETCH);
    registerUDF(org.apache.datasketches.hive.hll.SketchToEstimateAndErrorBoundsUDF.class,
        p + SKETCH_TO_ESTIMATE_WITH_ERROR_BOUNDS);
    registerUDF(org.apache.datasketches.hive.hll.SketchToEstimateUDF.class, p + SKETCH_TO_ESTIMATE);
    registerUDF(org.apache.datasketches.hive.hll.SketchToStringUDF.class, p + SKETCH_TO_STRING);
    registerUDF(org.apache.datasketches.hive.hll.UnionSketchUDF.class, prefix + UNION_SKETCH);
    registerUDAF(org.apache.datasketches.hive.hll.UnionSketchUDAF.class, prefix + UNION_SKETCH);
  }

  private void registerCpc(String prefix) {
    String p = prefix + "_";
    registerUDAF(org.apache.datasketches.hive.cpc.DataToSketchUDAF.class, p + DATA_TO_SKETCH);
    // FIXME: normalize GetEstimateAndErrorBoundsUDF vs SketchToEstimateAndErrorBoundsUDF
    registerUDF(org.apache.datasketches.hive.cpc.GetEstimateAndErrorBoundsUDF.class,
        p + SKETCH_TO_ESTIMATE_WITH_ERROR_BOUNDS);
    // FIXME: normalize GetEstimateUDF vs SketchToEstimateUDF
    registerUDF(org.apache.datasketches.hive.cpc.GetEstimateUDF.class, p + SKETCH_TO_ESTIMATE);
    registerUDF(org.apache.datasketches.hive.cpc.SketchToStringUDF.class, p + SKETCH_TO_STRING);
    registerUDF(org.apache.datasketches.hive.cpc.UnionSketchUDF.class, prefix + UNION_SKETCH);
    registerUDAF(org.apache.datasketches.hive.cpc.UnionSketchUDAF.class, prefix + UNION_SKETCH);
  }

  private void registerKll(String prefix) {
    String p = prefix + "_";
    registerUDAF(org.apache.datasketches.hive.kll.DataToSketchUDAF.class, p + DATA_TO_SKETCH);
    registerUDF(org.apache.datasketches.hive.kll.SketchToStringUDF.class, p + SKETCH_TO_STRING);
    //    registerUDF(org.apache.datasketches.hive.kll.UnionSketchUDF.class, prefix + UNION_SKETCH);
    registerUDAF(org.apache.datasketches.hive.kll.UnionSketchUDAF.class, prefix + UNION_SKETCH);

    registerUDF(org.apache.datasketches.hive.kll.GetNUDF.class, prefix + GET_N);
    registerUDF(org.apache.datasketches.hive.kll.GetCdfUDF.class, prefix + GET_CDF);
    registerUDF(org.apache.datasketches.hive.kll.GetPmfUDF.class, prefix + GET_PMF);
    registerUDF(org.apache.datasketches.hive.kll.GetQuantilesUDF.class, prefix + GET_QUANTILES);
    registerUDF(org.apache.datasketches.hive.kll.GetQuantileUDF.class, prefix + GET_QUANTILE);
    registerUDF(org.apache.datasketches.hive.kll.GetRankUDF.class, prefix + GET_RANK);
  }

  private void registerTheta(String prefix) {
    String p = prefix + "_";
    registerUDAF(org.apache.datasketches.hive.theta.DataToSketchUDAF.class, p + DATA_TO_SKETCH);
    // FIXME: missing?
    //registerUDF(org.apache.datasketches.hive.theta.SketchToStringUDF.class, p + SKETCH_TO_STRING);
    registerUDF(org.apache.datasketches.hive.theta.UnionSketchUDF.class, prefix + UNION_SKETCH);
    registerUDAF(org.apache.datasketches.hive.theta.UnionSketchUDAF.class, prefix + UNION_SKETCH);
    registerUDF(org.apache.datasketches.hive.theta.IntersectSketchUDF.class, prefix + INTERSECT_SKETCH);
    registerUDAF(org.apache.datasketches.hive.theta.IntersectSketchUDAF.class, prefix + INTERSECT_SKETCH);
    registerUDF(org.apache.datasketches.hive.theta.EstimateSketchUDF.class, p + SKETCH_TO_ESTIMATE);
    registerUDF(org.apache.datasketches.hive.theta.ExcludeSketchUDF.class, p + EXCLUDE_SKETCH);

  }

  private void registerTuple(String prefix) {
    // FIXME: there are 2 sketches there ; AoD and a generic on
  }

  private void registerQuantiles(String prefix) {
    registerQuantilesString(prefix + "_strings");
    registerQuantilesDoubles(prefix + "_doubles");
  }

  private void registerFrequencies(String prefix) {
    p = prefix + "_";
    registerUDAF(org.apache.datasketches.hive.frequencies.DataToStringsSketchUDAF.class, p + DATA_TO_SKETCH);
    // FIXME: missing?
    //registerUDF(org.apache.datasketches.hive.frequencies.DoublesSketchToStringUDF.class, p + SKETCH_TO_STRING);
    //registerUDF(org.apache.datasketches.hive.quantiles.UnionItemsSketchUDAF.class, prefix + UNION_SKETCH);
    registerUDAF(org.apache.datasketches.hive.frequencies.UnionStringsSketchUDAF.class, prefix + UNION_SKETCH);
    registerUDTF(org.apache.datasketches.hive.frequencies.GetFrequentItemsFromStringsSketchUDTF.class,
        prefix + GET_FREQUENT_ITEMS);
  }

  private void registerQuantilesString(String prefix) {
    String p = prefix + "_";
    registerUDAF(org.apache.datasketches.hive.quantiles.DataToStringsSketchUDAF.class, p + DATA_TO_SKETCH);
    registerUDF(org.apache.datasketches.hive.quantiles.StringsSketchToStringUDF.class, p + SKETCH_TO_STRING);
    //registerUDF(org.apache.datasketches.hive.quantiles.UnionItemsSketchUDAF.class, prefix + UNION_SKETCH);
    registerUDAF(org.apache.datasketches.hive.quantiles.UnionStringsSketchUDAF.class, prefix + UNION_SKETCH);
    registerUDF(org.apache.datasketches.hive.quantiles.GetNFromStringsSketchUDF.class, prefix + GET_N);
    registerUDF(org.apache.datasketches.hive.quantiles.GetKFromStringsSketchUDF.class, prefix + GET_K);
    registerUDF(org.apache.datasketches.hive.quantiles.GetCdfFromStringsSketchUDF.class, prefix + GET_CDF);
    registerUDF(org.apache.datasketches.hive.quantiles.GetPmfFromStringsSketchUDF.class, prefix + GET_PMF);
    registerUDF(org.apache.datasketches.hive.quantiles.GetQuantileFromStringsSketchUDF.class, prefix + GET_QUANTILE);
    registerUDF(org.apache.datasketches.hive.quantiles.GetQuantilesFromStringsSketchUDF.class, prefix + GET_QUANTILES);
  }

  private void registerQuantilesDoubles(String prefix) {
    String p = prefix + "_";
    registerUDAF(org.apache.datasketches.hive.quantiles.DataToDoublesSketchUDAF.class, p + DATA_TO_SKETCH);
    registerUDF(org.apache.datasketches.hive.quantiles.DoublesSketchToStringUDF.class, p + SKETCH_TO_STRING);
    //registerUDF(org.apache.datasketches.hive.quantiles.UnionItemsSketchUDAF.class, prefix + UNION_SKETCH);
    registerUDAF(org.apache.datasketches.hive.quantiles.UnionDoublesSketchUDAF.class, prefix + UNION_SKETCH);
    registerUDF(org.apache.datasketches.hive.quantiles.GetNFromDoublesSketchUDF.class, prefix + GET_N);
    registerUDF(org.apache.datasketches.hive.quantiles.GetKFromDoublesSketchUDF.class, prefix + GET_K);
    registerUDF(org.apache.datasketches.hive.quantiles.GetCdfFromDoublesSketchUDF.class, prefix + GET_CDF);
    registerUDF(org.apache.datasketches.hive.quantiles.GetPmfFromDoublesSketchUDF.class, prefix + GET_PMF);
    registerUDF(org.apache.datasketches.hive.quantiles.GetQuantileFromDoublesSketchUDF.class, prefix + GET_QUANTILE);
    registerUDF(org.apache.datasketches.hive.quantiles.GetQuantilesFromDoublesSketchUDF.class, prefix + GET_QUANTILES);
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

  private String getUDFName(Class<?> clazz) {
    Description desc = getDescription(clazz);
    String name = desc.name().toLowerCase();
    if (name == null || name == "") {
      throw new RuntimeException("The UDF class (" + clazz.getName() + ") doesn't have a valid name");
    }
    return name;
  }

  private Description getDescription(Class<?> clazz) {
    Description desc = clazz.getAnnotation(Description.class);
    if (desc == null) {
      throw new RuntimeException("no Description annotation on class: " + clazz.getName());
    }
    return desc;
  }

}
