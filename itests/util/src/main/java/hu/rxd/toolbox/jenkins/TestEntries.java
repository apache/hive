package hu.rxd.toolbox.jenkins;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;

import hu.rxd.toolbox.qtest.diff.CachedURL;

public class TestEntries {

  private List<TestEntry> entries;

  private TestEntries(List<TestEntry> entries) {
    this.entries = entries;
  }


  /**
   * example buildURL: http://j1:8080/job/tmp_kx_2/lastCompletedBuild/
   *
   * @param buildURL
   * @return
   * @throws Exception
   */
  public static TestEntries fromJenkinsBuild(String buildURL) throws Exception {
    String treePart = "suites[cases[className,name,duration,status]]";
    treePart = treePart.replaceAll("\\[", "%5B").replaceAll("\\]", "%5D");
    URL u0 = new URL(buildURL + "/testReport/api/json?pretty=true&tree=" + treePart);
    URL u = new CachedURL(u0).getURL();
    try (InputStream jsonStream = u.openStream()) {
      return new TestEntries(testEntries(parseTestResults(jsonStream)));
    }
  }

  public static void main(String[] args) throws Exception {
    String url = "https://builds.apache.org/job/PreCommit-HIVE-Build/20999/";
    TestEntries res = fromJenkinsBuild(url);
    TestEntries res2 = res.filterFailed().limit(400);
    String pat = res2.getSimpleMavenTestPattern();
    System.out.println(pat);
    System.out.println("|pat|=" + pat.length());
    System.out.println(res.entries.size());
    System.out.println(res2.entries.size());
  }

  public TestEntries filterFailed() {
    List<TestEntry> ret = new ArrayList<>();
    for (TestEntry entry : entries) {

      if (entry.isFailed()) {
        ret.add(entry);
      }
    }
    return new TestEntries(ret);
  }

  public TestEntries limit(int max) {
    List<TestEntry> ret = new ArrayList<>();
    if (entries.size() > max) {
      throw new RuntimeException(String.format("is everything working fine? orig:%d max:%d", entries.size(), max));
//      System.err.printf("limiting test list from %d to contain %d elements", entries.size(), max);
    }
    ret.addAll(entries);
    return new TestEntries(ret);
  }

  public String getSimpleMavenTestPattern() {
    List<String> labels = new LinkedList<>();
    for (TestEntry testEntry : entries) {
      labels.add(testEntry.getLabel());
    }
    return Joiner.on(",").join(labels);
  }

  public String getSimpleMavenTestPattern2() {
    List<String> labels = new LinkedList<>();
    for (TestEntry testEntry : entries) {
      labels.add(testEntry.getLabel());
    }
    return Joiner.on(",").join(labels);
  }

  private static TestResults parseTestResults(InputStream jsonStream) throws IOException, JsonParseException, JsonMappingException {
    ObjectMapper mapper = new ObjectMapper();
    TestResults results = mapper.readValue(jsonStream, TestResults.class);
    return results;
  }

  private static List<TestEntry> testEntries(TestResults results) {
    List<TestEntry> entries = new ArrayList<TestEntry>();

    for (TestResults.Suite s : results.suites) {
      for (TestResults.Suite.Case c : s.cases) {
        entries.add(new TestEntry(c.className, c.name, c.duration, (c.status)));
      }
    }
    return entries;
  }

  @Override
  public String toString() {
    return String.format("TestEntries; %d", entries.size());
  }

}
