package org.apache.hadoop.hive.cli.control;

import java.util.ArrayList;
import java.util.List;

public class SplitSupport {

  public static List<Object[]> process(List<Object[]> parameters, Class<?> currentClass, int nSplits) {
    // auto-disable primary test in case splits are present
    if (!isSplitClass(currentClass) && isSplit0ClassExistsFor(currentClass)) {
      return new ArrayList<>();
    }
    int i = getSplitIndex(currentClass);
    return getSplitParams(parameters, i, nSplits);
  }

  private static List<Object[]> getSplitParams(List<Object[]> parameters, int i, int nSplits) {
    if(i<0 || i>=nSplits) {
      throw new IllegalArgumentException("unexpected");
    }
    int n = parameters.size();
    int st = i * n / nSplits;
    int ed = (i + 1) * n / nSplits;

    return parameters.subList(st, ed);
  }

  private static boolean isSplitClass(Class<?> currentClass) {
    Package p = currentClass.getPackage();
    return p.getName().matches(".*split[1-9]+$");
  }

  private static int getSplitIndex(Class<?> currentClass) {
    Package p = currentClass.getPackage();
    String splitNum = p.getName().replaceAll("(.*split)([1-9]+)$", "\\1");
    return Integer.parseInt(splitNum);
  }

  private static boolean isSplit0ClassExistsFor(Class<?> clazz) {
    Package p = clazz.getPackage();
    String split1 = p.getName() + ".split0." + clazz.getSimpleName();
    try {
      Class<?> c = Class.forName(split1);
      return c != null;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

}
