package org.apache.hadoop.hive.ql.exec;

import org.apache.datasketches.hive.cpc.DataToSketchUDAF;
import org.apache.datasketches.hive.cpc.SketchToStringUDF;
import org.apache.datasketches.hive.cpc.UnionSketchUDF;
import org.apache.datasketches.hive.hll.SketchToEstimateAndErrorBoundsUDF;
import org.apache.datasketches.hive.hll.SketchToEstimateUDF;
import org.apache.datasketches.hive.hll.UnionSketchUDAF;

public class SketchesX {

  public static void register(Registry system) {
    system.registerGenericUDAF("dataToSketch", new DataToSketchUDAF());
    system.registerUDF("SketchToEstimateAndErrorBounds", SketchToEstimateAndErrorBoundsUDF.class, false);
    system.registerUDF("SketchToEstimate", SketchToEstimateUDF.class, false);
    system.registerUDF("SketchToString", SketchToStringUDF.class, false);
    system.registerUDF("unionSketch_u", UnionSketchUDF.class, false);

    system.registerGenericUDAF("unionSketch", new UnionSketchUDAF());


  }

}
