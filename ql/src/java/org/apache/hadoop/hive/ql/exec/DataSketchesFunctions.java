package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;

public class DataSketchesFunctions {

  private static final String DATA_TO_SKETCH = "datatosketch";
  private static final String SKETCH_TO_ESTIMATE_WITH_ERROR_BOUNDS = "sketchToEstimateWithErrorBounds";
  // FIXME: consider to rename it to simply "estimate" or "evaluate" - in case of the counting sketches the "sketchto..." doesnt add value
  private static final String SKETCH_TO_ESTIMATE = "sketchToEstimate";
  private static final String SKETCH_TO_STRING = "sketchToString";
  // FIXME: probably use simply "union"?
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
  private static final String T_TEST = "TTest";
  private static final String SKETCH_TO_MEANS = "sketchtomeans";
  private static final String SKETCH_TO_NUMBER_OF_RETAINED_ENTRIES = "sketchtonumberofretainedentries";
  private static final String SKETCH_TO_QUANTILES_SKETCH = "sketchToQuantilesSketch";
  private static final String SKETCH_TO_VALUES = "sketchToValues";
  private static final String SKETCH_TO_VARIANCES = "sketchToVariances";
  private static final String SKETCH_TO_PERCENTILE = "sketchToPercentile";
  private static final String UNION_SKETCH1 = "unionSketch1";
  private static final String INTERSECT_SKETCH1 = "inter1";

  private final Registry system;

  public DataSketchesFunctions(Registry system) {
    this.system = system;
  }

  public static void register(Registry system) {
    DataSketchesFunctions dsf = new DataSketchesFunctions(system);
    // FIXME: what this should be approx, ds ... other?
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

  }

  private void registerTuple(String prefix) {
    registerTupleArrayOfDoubles(prefix + "_tuple_arrayofdouble");
    registerTupleDoubleSummary(prefix + "_tuple_doublesummary");
  }

  private void registerTupleArrayOfDoubles(String string) {
    String p = string + "_";
    registerUDAF(org.apache.datasketches.hive.tuple.DataToArrayOfDoublesSketchUDAF.class, p + DATA_TO_SKETCH);
    // FIXME: missing?
    //registerUDF(org.apache.datasketches.hive.theta.SketchToStringUDF.class, p + SKETCH_TO_STRING);
    registerUDAF(org.apache.datasketches.hive.tuple.UnionArrayOfDoublesSketchUDAF.class, p + UNION_SKETCH);
    registerUDF(org.apache.datasketches.hive.tuple.ArrayOfDoublesSketchesTTestUDF.class, p + T_TEST);
    registerUDF(org.apache.datasketches.hive.tuple.ArrayOfDoublesSketchToEstimatesUDF.class, p + SKETCH_TO_ESTIMATE);
    registerUDF(org.apache.datasketches.hive.tuple.ArrayOfDoublesSketchToEstimateAndErrorBoundsUDF.class,
        p + SKETCH_TO_ESTIMATE_WITH_ERROR_BOUNDS);
    registerUDF(org.apache.datasketches.hive.tuple.ArrayOfDoublesSketchToMeansUDF.class, p + SKETCH_TO_MEANS);
    registerUDF(org.apache.datasketches.hive.tuple.ArrayOfDoublesSketchToNumberOfRetainedEntriesUDF.class,
        p + SKETCH_TO_NUMBER_OF_RETAINED_ENTRIES);
    registerUDF(org.apache.datasketches.hive.tuple.ArrayOfDoublesSketchToQuantilesSketchUDF.class,
        p + SKETCH_TO_QUANTILES_SKETCH);
    registerUDTF(org.apache.datasketches.hive.tuple.ArrayOfDoublesSketchToValuesUDTF.class, p + SKETCH_TO_VALUES);
    registerUDF(org.apache.datasketches.hive.tuple.ArrayOfDoublesSketchToVariancesUDF.class, p + SKETCH_TO_VARIANCES);
  }

  private void registerTupleDoubleSummary(String string) {
    String p = string + "_";
    registerUDAF(org.apache.datasketches.hive.tuple.DataToDoubleSummarySketchUDAF.class, p + DATA_TO_SKETCH);
    // FIXME: missing?
    //registerUDF(org.apache.datasketches.hive.theta.SketchToStringUDF.class, p + SKETCH_TO_STRING);
    registerUDAF(org.apache.datasketches.hive.tuple.UnionDoubleSummarySketchUDAF.class, p + UNION_SKETCH);
    registerUDF(org.apache.datasketches.hive.tuple.DoubleSummarySketchToEstimatesUDF.class, p + SKETCH_TO_ESTIMATE);
    registerUDF(org.apache.datasketches.hive.tuple.DoubleSummarySketchToPercentileUDF.class, p + SKETCH_TO_PERCENTILE);
  }

  private void registerQuantiles(String prefix) {
    registerQuantilesString(prefix + "_quantile");
    registerQuantilesDoubles(prefix + "_quantile");
  }

  private void registerFrequencies(String prefix) {
    String p = prefix + "_";
    registerUDAF(org.apache.datasketches.hive.frequencies.DataToStringsSketchUDAF.class, p + DATA_TO_SKETCH);
    // FIXME: missing?
    //registerUDF(org.apache.datasketches.hive.frequencies.DoublesSketchToStringUDF.class, p + SKETCH_TO_STRING);
    //registerUDF(org.apache.datasketches.hive.quantiles.UnionItemsSketchUDAF.class, p + UNION_SKETCH);
    registerUDAF(org.apache.datasketches.hive.frequencies.UnionStringsSketchUDAF.class, p + UNION_SKETCH);
    registerUDTF(org.apache.datasketches.hive.frequencies.GetFrequentItemsFromStringsSketchUDTF.class,
        prefix + GET_FREQUENT_ITEMS);
  }

  private void registerQuantilesString(String prefix) {
    String p = prefix + "_strings_";
    registerUDAF(org.apache.datasketches.hive.quantiles.DataToStringsSketchUDAF.class, p + DATA_TO_SKETCH);
    registerUDF(org.apache.datasketches.hive.quantiles.StringsSketchToStringUDF.class, p + SKETCH_TO_STRING);
    //registerUDF(org.apache.datasketches.hive.quantiles.UnionItemsSketchUDAF.class, p + UNION_SKETCH);
    registerUDAF(org.apache.datasketches.hive.quantiles.UnionStringsSketchUDAF.class, p + UNION_SKETCH);
    registerUDF(org.apache.datasketches.hive.quantiles.GetNFromStringsSketchUDF.class, p + GET_N);
    registerUDF(org.apache.datasketches.hive.quantiles.GetKFromStringsSketchUDF.class, p + GET_K);
    registerUDF(org.apache.datasketches.hive.quantiles.GetCdfFromStringsSketchUDF.class, p + GET_CDF);
    registerUDF(org.apache.datasketches.hive.quantiles.GetPmfFromStringsSketchUDF.class, p + GET_PMF);
    registerUDF(org.apache.datasketches.hive.quantiles.GetQuantileFromStringsSketchUDF.class, p + GET_QUANTILE);
    registerUDF(org.apache.datasketches.hive.quantiles.GetQuantilesFromStringsSketchUDF.class, p + GET_QUANTILES);
  }

  private void registerQuantilesDoubles(String prefix) {
    String p = prefix + "_doubles_";
    registerUDAF(org.apache.datasketches.hive.quantiles.DataToDoublesSketchUDAF.class, p + DATA_TO_SKETCH);
    registerUDF(org.apache.datasketches.hive.quantiles.DoublesSketchToStringUDF.class, p + SKETCH_TO_STRING);
    //registerUDF(org.apache.datasketches.hive.quantiles.UnionItemsSketchUDAF.class, p + UNION_SKETCH);
    registerUDAF(org.apache.datasketches.hive.quantiles.UnionDoublesSketchUDAF.class, p + UNION_SKETCH);
    registerUDF(org.apache.datasketches.hive.quantiles.GetNFromDoublesSketchUDF.class, p + GET_N);
    registerUDF(org.apache.datasketches.hive.quantiles.GetKFromDoublesSketchUDF.class, p + GET_K);
    registerUDF(org.apache.datasketches.hive.quantiles.GetCdfFromDoublesSketchUDF.class, p + GET_CDF);
    registerUDF(org.apache.datasketches.hive.quantiles.GetPmfFromDoublesSketchUDF.class, p + GET_PMF);
    registerUDF(org.apache.datasketches.hive.quantiles.GetQuantileFromDoublesSketchUDF.class, p + GET_QUANTILE);
    registerUDF(org.apache.datasketches.hive.quantiles.GetQuantilesFromDoublesSketchUDF.class, p + GET_QUANTILES);
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
