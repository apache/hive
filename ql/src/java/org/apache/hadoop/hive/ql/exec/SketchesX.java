package org.apache.hadoop.hive.ql.exec;

import org.apache.datasketches.hive.hll.*;
import org.apache.datasketches.hive.hll.UnionSketchUDF;
import org.apache.datasketches.hive.hll.SketchToEstimateAndErrorBoundsUDF;
import org.apache.datasketches.hive.hll.SketchToEstimateUDF;
import org.apache.datasketches.hive.hll.UnionSketchUDAF;

public class SketchesX {

  public static void register(Registry system) {
    // FIXME: consider prefixing the functions?
    system.registerGenericUDAF("dataToSketch", new DataToSketchUDAF());
    system.registerUDF("SketchToEstimateAndErrorBounds", SketchToEstimateAndErrorBoundsUDF.class, false);
    system.registerUDF("SketchToEstimate", SketchToEstimateUDF.class, false);
    system.registerUDF("SketchToString", SketchToStringUDF.class, false);
    system.registerUDF("unionSketch_u", UnionSketchUDF.class, false);

    system.registerGenericUDAF("unionSketch", new UnionSketchUDAF());


  }

}
/*
add jar /home/dev/hive/packaging/target/apache-hive-4.0.0-SNAPSHOT-bin/apache-hive-4.0.0-SNAPSHOT-bin/lib/datasketches-hive-1.1.0-incubating-SNAPSHOT.jar;
add jar /home/dev/hive/packaging/target/apache-hive-4.0.0-SNAPSHOT-bin/apache-hive-4.0.0-SNAPSHOT-bin/lib/sketches-core-0.9.0.jar;
add jar /home/dev/hive/packaging/target/apache-hive-4.0.0-SNAPSHOT-bin/apache-hive-4.0.0-SNAPSHOT-bin/lib/datasketches-java-1.2.0-incubating.jar;
add jar /home/dev/hive/packaging/target/apache-hive-4.0.0-SNAPSHOT-bin/apache-hive-4.0.0-SNAPSHOT-bin/lib/datasketches-memory-1.2.0-incubating.jar
*/
