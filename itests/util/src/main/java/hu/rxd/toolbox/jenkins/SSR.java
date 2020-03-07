package hu.rxd.toolbox.jenkins;

import java.util.Set;
import java.util.TreeSet;

/**
 * StringSet regexp
 */
public class SSR {

  public SSR(Set<String> sIn) {
    TreeSet<String> s = new TreeSet<>();
    s.addAll(sIn);
  }

}
