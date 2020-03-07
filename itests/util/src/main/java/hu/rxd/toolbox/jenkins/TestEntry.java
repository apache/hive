package hu.rxd.toolbox.jenkins;

import java.util.Comparator;

/**
 * Represents 1 test's execution
 */
public class TestEntry {

  public static enum Status {
    PASSED,
    REGRESSION,
    FAILED,
    FIXED,
    SKIPPED;

    boolean isFailed() {
      return this == REGRESSION || this == FAILED;
    }

    boolean isPassed() {
      return this == PASSED || this == FIXED;
    }

  }
  public static final Comparator<? super TestEntry> LABEL_COMPARATOR = new Comparator<TestEntry>() {

    @Override
    public int compare(TestEntry o1, TestEntry o2) {
      return o1.label.compareTo(o2.label);
    }
  };
  private String label;
  private double duration;
  private String className;
  private String methodName;
  private Status status;

  public TestEntry(String className, String methodName, double duration, String status) {
    this.duration = duration;
    this.methodName = methodName;
    this.status = Status.valueOf(status);
    this.className = className.replace('.', '/') + ".class";
    // label = className.replaceAll(".*\\.", "") + "#" + methodName;
    label = className + "#" + methodName;
  }

  @Override
  public String toString() {
    return String.format("%s  %s %s", label, duration, status);
  }

  /**
   * maven compatible test pattern.
   *
   * can be passed as -Dtest=
   */
  public String getLabel() {
    return label;
  }

  public double getDuration() {
    return duration;
  }

  public boolean isFailed() {
    return status.isFailed();
  }

  public boolean isPassed() {
    return status.isPassed();
  }

}