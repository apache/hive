
package hu.rxd.toolbox.jenkins;

import java.util.List;

/**
 * model to parse jenkins/junit-plugins model
 */
public class TestResults {

  public static class Suite {
    public static class Case {
      public String className;
      public String name;
      public String status;
      public double duration;
    }

    public String name;
    public double duration;
    public List<Case> cases;

    @Override
    public String toString() {
      return name + " duration: " + duration;
    }
  };

  public String _class;
  public List<Suite> suites;

}
