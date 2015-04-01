package org.apache.hadoop.hive.ql.session;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.apache.hadoop.hive.ql.session.SessionState.ResourceType;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.io.BufferedWriter;
import java.io.FileWriter;


public class TestAddResource {

  private static final String TEST_JAR_DIR = System.getProperty("test.tmp.dir", ".") + "/";
  private HiveConf conf;
  private ResourceType t;

  @Before
  public void setup() throws IOException {
    conf = new HiveConf();
    t = ResourceType.JAR;

    //Generate test jar files
    for (int i = 1; i <= 5; i++) {
      Writer output = null;
      String dataFile = TEST_JAR_DIR + "testjar" + i + ".jar";
      File file = new File(dataFile);
      output = new BufferedWriter(new FileWriter(file));
      output.write("sample");
      output.close();
    }
  }

  // Check that all the jars are added to the classpath
  @Test
  public void testSanity() throws URISyntaxException, IOException {
    SessionState ss = Mockito.spy(SessionState.start(conf).get());
    String query = "testQuery";

    // add all the dependencies to a list
    List<URI> list = new LinkedList<URI>();
    List<String> addList = new LinkedList<String>();
    list.add(new URI(TEST_JAR_DIR + "testjar1.jar"));
    list.add(new URI(TEST_JAR_DIR + "testjar2.jar"));
    list.add(new URI(TEST_JAR_DIR + "testjar3.jar"));
    list.add(new URI(TEST_JAR_DIR + "testjar4.jar"));
    list.add(new URI(TEST_JAR_DIR + "testjar5.jar"));

    //return all the dependency urls
    Mockito.when(ss.resolveAndDownload(t, query, false)).thenReturn(list);
    addList.add(query);
    ss.add_resources(t, addList);
    Set<String> dependencies = ss.list_resource(t, null);
    LinkedList<URI> actual = new LinkedList<URI>();
    for (String dependency : dependencies) {
      actual.add(new URI(dependency));
    }

    // sort both the lists
    Collections.sort(list);
    Collections.sort(actual);

    assertEquals(list, actual);
    ss.close();

  }

  // add same jar multiple times and check that dependencies are added only once.
  @Test
  public void testDuplicateAdds() throws URISyntaxException, IOException {

    SessionState ss = Mockito.spy(SessionState.start(conf).get());

    String query = "testQuery";

    List<URI> list = new LinkedList<URI>();
    List<String> addList = new LinkedList<String>();
    list.add(new URI(TEST_JAR_DIR + "testjar1.jar"));
    list.add(new URI(TEST_JAR_DIR + "testjar2.jar"));
    list.add(new URI(TEST_JAR_DIR + "testjar3.jar"));
    list.add(new URI(TEST_JAR_DIR + "testjar4.jar"));
    list.add(new URI(TEST_JAR_DIR + "testjar5.jar"));

    Collections.sort(list);

    Mockito.when(ss.resolveAndDownload(t, query, false)).thenReturn(list);
    for (int i = 0; i < 10; i++) {
      addList.add(query);
    }
    ss.add_resources(t, addList);
    Set<String> dependencies = ss.list_resource(t, null);
    LinkedList<URI> actual = new LinkedList<URI>();
    for (String dependency : dependencies) {
      actual.add(new URI(dependency));
    }

    Collections.sort(actual);
    assertEquals(list, actual);
    ss.close();

  }

  // test when two jars with shared dependencies are added, the classloader contains union of the dependencies
  @Test
  public void testUnion() throws URISyntaxException, IOException {

    HiveConf conf = new HiveConf();
    SessionState ss = Mockito.spy(SessionState.start(conf).get());
    ResourceType t = ResourceType.JAR;
    String query1 = "testQuery1";
    String query2 = "testQuery2";
    List<String> addList = new LinkedList<String>();
    // add dependencies for the jars
    List<URI> list1 = new LinkedList<URI>();
    List<URI> list2 = new LinkedList<URI>();
    list1.add(new URI(TEST_JAR_DIR + "testjar1.jar"));
    list1.add(new URI(TEST_JAR_DIR + "testjar2.jar"));
    list1.add(new URI(TEST_JAR_DIR + "testjar3.jar"));
    list1.add(new URI(TEST_JAR_DIR + "testjar4.jar"));
    list2.add(new URI(TEST_JAR_DIR + "testjar5.jar"));
    list2.add(new URI(TEST_JAR_DIR + "testjar3.jar"));
    list2.add(new URI(TEST_JAR_DIR + "testjar4.jar"));

    Mockito.when(ss.resolveAndDownload(t, query1, false)).thenReturn(list1);
    Mockito.when(ss.resolveAndDownload(t, query2, false)).thenReturn(list2);
    addList.add(query1);
    addList.add(query2);
    ss.add_resources(t, addList);

    Set<String> dependencies = ss.list_resource(t, null);
    LinkedList<URI> actual = new LinkedList<URI>();
    for (String dependency : dependencies) {
      actual.add(new URI(dependency));
    }
    List<URI> expected = union(list1, list2);

    Collections.sort(expected);
    Collections.sort(actual);

    assertEquals(expected, actual);
    ss.close();

  }

  // Test when two jars are added with shared dependencies and one jar is deleted, the shared dependencies should not be deleted
  @Test
  public void testDeleteJar() throws URISyntaxException, IOException {
    SessionState ss = Mockito.spy(SessionState.start(conf).get());

    String query1 = "testQuery1";
    String query2 = "testQuery2";

    List<URI> list1 = new LinkedList<URI>();
    List<URI> list2 = new LinkedList<URI>();
    List<String> addList = new LinkedList<String>();
    list1.add(new URI(TEST_JAR_DIR + "testjar1.jar"));
    list1.add(new URI(TEST_JAR_DIR + "testjar2.jar"));
    list1.add(new URI(TEST_JAR_DIR + "testjar3.jar"));
    list1.add(new URI(TEST_JAR_DIR + "testjar4.jar"));
    list2.add(new URI(TEST_JAR_DIR + "testjar5.jar"));
    list2.add(new URI(TEST_JAR_DIR + "testjar3.jar"));
    list2.add(new URI(TEST_JAR_DIR + "testjar4.jar"));

    Collections.sort(list1);
    Collections.sort(list2);

    Mockito.when(ss.resolveAndDownload(t, query1, false)).thenReturn(list1);
    Mockito.when(ss.resolveAndDownload(t, query2, false)).thenReturn(list2);
    addList.add(query1);
    addList.add(query2);
    ss.add_resources(t, addList);
    List<String> deleteList = new LinkedList<String>();
    deleteList.add(list1.get(0).toString());
    // delete jar and its dependencies added using query1
    ss.delete_resources(t, deleteList);

    Set<String> dependencies = ss.list_resource(t, null);
    LinkedList<URI> actual = new LinkedList<URI>();
    for (String dependency : dependencies) {
      actual.add(new URI(dependency));
    }
    List<URI> expected = list2;
    Collections.sort(expected);
    Collections.sort(actual);
    assertEquals(expected, actual);

    deleteList.clear();
    deleteList.add(list2.get(0).toString());
    // delete remaining jars
    ss.delete_resources(t, deleteList);
    dependencies = ss.list_resource(t, null);
    assertEquals(dependencies.isEmpty(), true);

    ss.close();

  }

  // same test as above but with 3 jars sharing dependencies
  @Test
  public void testDeleteJarMultiple() throws URISyntaxException, IOException {
    SessionState ss = Mockito.spy(SessionState.start(conf).get());

    String query1 = "testQuery1";
    String query2 = "testQuery2";
    String query3 = "testQuery3";

    List<URI> list1 = new LinkedList<URI>();
    List<URI> list2 = new LinkedList<URI>();
    List<URI> list3 = new LinkedList<URI>();
    List<String> addList = new LinkedList<String>();
    list1.add(new URI(TEST_JAR_DIR + "testjar1.jar"));
    list1.add(new URI(TEST_JAR_DIR + "testjar2.jar"));
    list1.add(new URI(TEST_JAR_DIR + "testjar3.jar"));
    list1.add(new URI(TEST_JAR_DIR + "testjar4.jar"));
    list2.add(new URI(TEST_JAR_DIR + "testjar5.jar"));
    list2.add(new URI(TEST_JAR_DIR + "testjar3.jar"));
    list2.add(new URI(TEST_JAR_DIR + "testjar4.jar"));
    list3.add(new URI(TEST_JAR_DIR + "testjar4.jar"));
    list3.add(new URI(TEST_JAR_DIR + "testjar2.jar"));
    list3.add(new URI(TEST_JAR_DIR + "testjar5.jar"));

    Collections.sort(list1);
    Collections.sort(list2);
    Collections.sort(list3);

    Mockito.when(ss.resolveAndDownload(t, query1, false)).thenReturn(list1);
    Mockito.when(ss.resolveAndDownload(t, query2, false)).thenReturn(list2);
    Mockito.when(ss.resolveAndDownload(t, query3, false)).thenReturn(list3);
    addList.add(query1);
    addList.add(query2);
    addList.add(query3);
    ss.add_resources(t, addList);

    List<String> deleteList = new LinkedList<String>();
    deleteList.add(list1.get(0).toString());
    // delete jar added using query1
    ss.delete_resources(t, deleteList);

    Set<String> dependencies = ss.list_resource(t, null);
    LinkedList<URI> actual = new LinkedList<URI>();
    for (String dependency : dependencies) {
      actual.add(new URI(dependency));
    }
    List<URI> expected = union(list2, list3);
    Collections.sort(expected);
    Collections.sort(actual);
    assertEquals(expected, actual);

    actual.clear();
    expected.clear();

    deleteList.clear();
    deleteList.add(list2.get(0).toString());
    // delete jars added using query2
    ss.delete_resources(t, deleteList);
    dependencies = ss.list_resource(t, null);
    actual = new LinkedList<URI>();
    for (String dependency : dependencies) {
      actual.add(new URI(dependency));
    }
    expected = new LinkedList<URI>(list3);
    Collections.sort(expected);
    Collections.sort(actual);
    assertEquals(expected, actual);

    actual.clear();
    expected.clear();

    // delete remaining jars
    deleteList.clear();
    deleteList.add(list3.get(0).toString());
    ss.delete_resources(t, deleteList);

    dependencies = ss.list_resource(t, null);
    assertEquals(dependencies.isEmpty(), true);

    ss.close();
  }

  @After
  public void tearDown() {
    // delete sample jars
    for (int i = 1; i <= 5; i++) {
      String dataFile = TEST_JAR_DIR + "testjar" + i + ".jar";

      File f = new File(dataFile);
      if (!f.delete()) {
        throw new RuntimeException("Could not delete the data file");
      }
    }
  }

  private <T> List<T> union(List<T> list1, List<T> list2) {
    Set<T> set = new HashSet<T>();

    set.addAll(list1);
    set.addAll(list2);

    return new LinkedList<T>(set);
  }

}
