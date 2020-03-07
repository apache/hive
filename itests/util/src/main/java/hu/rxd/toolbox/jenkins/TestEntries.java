package hu.rxd.toolbox.jenkins;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;

import hu.rxd.toolbox.jenkins.TestResults.Suite;
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
    TestResults parseTestResults = fromJenkinsBuild0(buildURL);
    return new TestEntries(testEntries(parseTestResults));
  }

  // FIXME move?
  private static TestResults fromJenkinsBuild0(String buildURL)
      throws MalformedURLException, IOException, JsonParseException, JsonMappingException {
    String treePart = "suites[name,duration,cases[className,name,duration,status]]";
    treePart = treePart.replaceAll("\\[", "%5B").replaceAll("\\]", "%5D");
    URL u0 = new URL(buildURL + "/testReport/api/json?pretty=true&tree=" + treePart);
    URL u = new CachedURL(u0).getURL();
    TestResults parseTestResults;
    try (InputStream jsonStream = u.openStream()) {
      parseTestResults = parseTestResults(jsonStream);
    }
    return parseTestResults;
  }

  public static void main(String[] args) throws Exception {
    String url = "https://builds.apache.org/job/PreCommit-HIVE-Build/20999/";
    TestResults res = fromJenkinsBuild0(url);

    for (Suite s : res.suites) {
      if (!s.name.contains("CliDriver")) {
        continue;
      }
      int plannedMaxDuration = 800;
      if (s.duration > plannedMaxDuration) {
        System.out.println(s);
        proposeSplits(s, Math.floorDiv((int) s.duration, plannedMaxDuration) + 1);
      }

    }
  }

  private static void proposeSplits(Suite s, int n) {
    List<TestEntry> e = testEntries(s);
    e.sort(TestEntry.LABEL_COMPARATOR);
    double tt = totalTime(e);
    double targetT = tt / n;
    double x = 0.0;
    int splitK = e.size() / n;
    int cnt = 0;
    for (TestEntry testEntry : e) {
      cnt++;
      x+=testEntry.getDuration();
      if (cnt > splitK) {
        System.out.println(testEntry + " @" + x + "/" + (x - targetT) + " #" + cnt);
        if (x > 2 * targetT) {
          System.err.println("problematic: " + s + " " + x);
        }
        x=0;
        cnt = 0;
      }
    }
    System.out.println("EOF" + " @" + x + "/" + (x - targetT) + " #" + cnt);
    System.err.println("for " + s + " " + n);

  }

  private static double totalTime(List<TestEntry> e) {
    double dur = 0.0;
    for (TestEntry testEntry : e) {
      dur += testEntry.getDuration();
    }
    return dur;
  }

  public static void main0(String[] args) throws Exception {
    String url = "https://builds.apache.org/job/PreCommit-HIVE-Build/20999/";
    TestEntries res = fromJenkinsBuild(url);

    TestEntries res2 = res.filterSlow(3600);//res.filterFailed().limit(400);
    String pat = res2.getSimpleMavenTestPattern();
    System.out.println(pat);
    System.out.println("|pat|=" + pat.length());
    System.out.println(res.entries.size());
    System.out.println(res2.entries.size());
  }

  private TestEntries filterSlow(double minDur) {
    List<TestEntry> ret = new ArrayList<>();
    for (TestEntry entry : entries) {

      if (entry.getDuration() > minDur) {
        ret.add(entry);
      }
    }
    return new TestEntries(ret);
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
      List<TestEntry> entries1 = testEntries(s);
      entries.addAll(entries1);
    }
    return entries;
  }


  private static List<TestEntry> testEntries(TestResults.Suite s) {
    List<TestEntry> entries1 = new ArrayList<TestEntry>();
    for (TestResults.Suite.Case c : s.cases) {
      entries1.add(new TestEntry(c.className, c.name, c.duration, (c.status)));
    }
    return entries1;
  }

  @Override
  public String toString() {
    return String.format("TestEntries; %d", entries.size());
  }

}
