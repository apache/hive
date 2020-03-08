package org.apache.hadoop.hive.cli.control;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;

public class SplitSupport {

  public static List<Object[]> process(List<Object[]> parameters, Class<?> currentClass, int nSplits) {
    if (!isSplitExecution(currentClass)) {
      return parameters;
    }
    // auto-disable primary test in case splits are present
    if (isSplit0ClassExistsFor(currentClass)) {
      return new ArrayList<>();
    }
    int i = getSplitIndex(currentClass);
    return getSplitParams(parameters, i, nSplits);
  }

  private static boolean isSplitExecution(Class<?> currentClass) {
    return isSplitClass(currentClass) || isSplit0ClassExistsFor(currentClass);
  }

  @VisibleForTesting
  static List<Object[]> getSplitParams(List<Object[]> parameters, int i, int nSplits) {
    if(i<0 || i>=nSplits) {
      throw new IllegalArgumentException("unexpected");
    }
    int n = parameters.size();
    int st = i * n / nSplits;
    int ed = (i + 1) * n / nSplits;

    return parameters.subList(st, ed);
  }

  @VisibleForTesting
  static boolean isSplitClass(Class<?> currentClass) {
    Package p = currentClass.getPackage();
    return p.getName().matches(".*split[0-9]+$");
  }

  @VisibleForTesting
  static int getSplitIndex(Class<?> currentClass) {
    Package p = currentClass.getPackage();
    Pattern pat = Pattern.compile("(.*split)([0-9]+)$");
    Matcher matcher = pat.matcher(p.getName());
    if (matcher.find()) {
      return Integer.parseInt(matcher.group(2));
    }
    throw new IllegalArgumentException("cant get splitindex for: " + p);
  }

  @VisibleForTesting
  static boolean isSplit0ClassExistsFor(Class<?> clazz) {
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
