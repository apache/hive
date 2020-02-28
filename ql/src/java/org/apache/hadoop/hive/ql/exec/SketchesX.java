package org.apache.hadoop.hive.ql.exec;

import org.apache.datasketches.hive.hll.*;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;

public class SketchesX {

  private final Registry system;

  public SketchesX(Registry system) {
    this.system = system;
  }

  public static void register(Registry system) {
    new SketchesX(system).registerHll("hll");
  }

  private void registerHll(String prefix) {

    register(new DataToSketchUDAF(), prefix);
    system.registerUDF(prefix + "SketchToEstimateAndErrorBounds", SketchToEstimateAndErrorBoundsUDF.class, false);
    system.registerUDF(prefix + "SketchToEstimate", SketchToEstimateUDF.class, false);
    system.registerUDF(prefix + "SketchToString", SketchToStringUDF.class, false);
    system.registerUDF(prefix + "unionSketch_u", UnionSketchUDF.class, false);

    system.registerGenericUDAF(prefix + "unionSketch", new UnionSketchUDAF());

    system.registerGenericUDAF(prefix + "dataToSketch2", new DataToSketchUDAF());
    system.registerUDF(prefix + "SketchToEstimate2", SketchToEstimateUDF.class, false);


  }

  private void register(GenericUDAFResolver2 udaf, String prefix) {
    String name = getUDFName(udaf.getClass());
    system.registerGenericUDAF(prefix + name, new DataToSketchUDAF());

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
