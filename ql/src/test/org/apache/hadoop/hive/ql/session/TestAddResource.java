/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.session;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
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
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.ql.session.SessionState.ResourceType;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;


public class TestAddResource {

  private static final String TEST_JAR_DIR = System.getProperty("test.tmp.dir", ".") + File.separator;

  private HiveConf conf;
  private ResourceType t;

  @Before
  public void setup() throws IOException {
    conf = new HiveConfForTest(getClass());
    t = ResourceType.JAR;

    //Generate test jar files
    for (int i = 1; i <= 5; i++) {
      writeTestJarFile(TEST_JAR_DIR + "testjar" + i + ".jar", "sample");
    }
  }

  private static void writeTestJarFile(String dataFile, String content) throws IOException {
    File file = new File(dataFile);
    file.deleteOnExit();
    Writer output = new BufferedWriter(new FileWriter(file));
    output.write(content);
    output.close();
  }

  /**
   * Tests adding jar with special chars in the path:
   * - works if it's given in URL encoded format
   * - fails if it's not encoded (user should have done it).
   * @throws Exception
   */
  @Test
  public void testSpecialCharsInPath() throws Exception {
    String filePath = TEST_JAR_DIR + "testjar-[specialchars].jar";
    String filePathEncoded = TEST_JAR_DIR + "testjar-%5Bspecialchars%5D.jar";
    writeTestJarFile(filePath, "sample");

    SessionState ss = Mockito.spy(SessionState.start(conf).get());
    try {
      ss.add_resource(t, filePath);
    } catch (RuntimeException e) {
      assertEquals(URISyntaxException.class, e.getCause().getClass());
    }
    ss.add_resource(t, filePathEncoded);

    Set<String> result = ss.list_resource(t, null);
    assertEquals(1, result.size());
    assertEquals(filePath, result.stream().findFirst().get());

    ss.close();
  }

  // Check that all the jars are added to the classpath
  @Test
  public void testSanity() throws URISyntaxException, IOException {
    SessionState ss = Mockito.spy(SessionState.start(conf).get());
    String query = "testQuery";

    // add all the dependencies to a list
    List<URI> list = new LinkedList<URI>();
    List<String> addList = new LinkedList<String>();
    list.add(createURI(TEST_JAR_DIR + "testjar1.jar"));
    list.add(createURI(TEST_JAR_DIR + "testjar2.jar"));
    list.add(createURI(TEST_JAR_DIR + "testjar3.jar"));
    list.add(createURI(TEST_JAR_DIR + "testjar4.jar"));
    list.add(createURI(TEST_JAR_DIR + "testjar5.jar"));

    //return all the dependency urls
    Mockito.when(ss.resolveAndDownload(t, query, false)).thenReturn(list);
    addList.add(query);
    ss.add_resources(t, addList);
    Set<String> dependencies = ss.list_resource(t, null);
    LinkedList<URI> actual = new LinkedList<URI>();
    for (String dependency : dependencies) {
      actual.add(createURI(dependency));
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
    list.add(createURI(TEST_JAR_DIR + "testjar1.jar"));
    list.add(createURI(TEST_JAR_DIR + "testjar2.jar"));
    list.add(createURI(TEST_JAR_DIR + "testjar3.jar"));
    list.add(createURI(TEST_JAR_DIR + "testjar4.jar"));
    list.add(createURI(TEST_JAR_DIR + "testjar5.jar"));

    Collections.sort(list);

    Mockito.when(ss.resolveAndDownload(t, query, false)).thenReturn(list);
    for (int i = 0; i < 10; i++) {
      addList.add(query);
    }
    ss.add_resources(t, addList);
    Set<String> dependencies = ss.list_resource(t, null);
    LinkedList<URI> actual = new LinkedList<URI>();
    for (String dependency : dependencies) {
      actual.add(createURI(dependency));
    }

    Collections.sort(actual);
    assertEquals(list, actual);
    ss.close();

  }

  // test when two jars with shared dependencies are added, the classloader contains union of the dependencies
  @Test
  public void testUnion() throws URISyntaxException, IOException {
    SessionState ss = Mockito.spy(SessionState.start(conf).get());
    ResourceType t = ResourceType.JAR;
    String query1 = "testQuery1";
    String query2 = "testQuery2";
    List<String> addList = new LinkedList<String>();
    // add dependencies for the jars
    List<URI> list1 = new LinkedList<URI>();
    List<URI> list2 = new LinkedList<URI>();
    list1.add(createURI(TEST_JAR_DIR + "testjar1.jar"));
    list1.add(createURI(TEST_JAR_DIR + "testjar2.jar"));
    list1.add(createURI(TEST_JAR_DIR + "testjar3.jar"));
    list1.add(createURI(TEST_JAR_DIR + "testjar4.jar"));
    list2.add(createURI(TEST_JAR_DIR + "testjar5.jar"));
    list2.add(createURI(TEST_JAR_DIR + "testjar3.jar"));
    list2.add(createURI(TEST_JAR_DIR + "testjar4.jar"));

    Mockito.when(ss.resolveAndDownload(t, query1, false)).thenReturn(list1);
    Mockito.when(ss.resolveAndDownload(t, query2, false)).thenReturn(list2);
    addList.add(query1);
    addList.add(query2);
    ss.add_resources(t, addList);

    Set<String> dependencies = ss.list_resource(t, null);
    LinkedList<URI> actual = new LinkedList<URI>();
    for (String dependency : dependencies) {
      actual.add(createURI(dependency));
    }
    List<URI> expected = union(list1, list2);

    Collections.sort(expected);
    Collections.sort(actual);

    assertEquals(expected, actual);
    ss.close();

  }

  /**
   * @param path
   * @return URI corresponding to the path.
   */
  private static URI createURI(String path) throws URISyntaxException {
    return new URI(path);
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
    list1.add(createURI(TEST_JAR_DIR + "testjar1.jar"));
    list1.add(createURI(TEST_JAR_DIR + "testjar2.jar"));
    list1.add(createURI(TEST_JAR_DIR + "testjar3.jar"));
    list1.add(createURI(TEST_JAR_DIR + "testjar4.jar"));
    list2.add(createURI(TEST_JAR_DIR + "testjar5.jar"));
    list2.add(createURI(TEST_JAR_DIR + "testjar3.jar"));
    list2.add(createURI(TEST_JAR_DIR + "testjar4.jar"));

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
      actual.add(createURI(dependency));
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
    list1.add(createURI(TEST_JAR_DIR + "testjar1.jar"));
    list1.add(createURI(TEST_JAR_DIR + "testjar2.jar"));
    list1.add(createURI(TEST_JAR_DIR + "testjar3.jar"));
    list1.add(createURI(TEST_JAR_DIR + "testjar4.jar"));
    list2.add(createURI(TEST_JAR_DIR + "testjar5.jar"));
    list2.add(createURI(TEST_JAR_DIR + "testjar3.jar"));
    list2.add(createURI(TEST_JAR_DIR + "testjar4.jar"));
    list3.add(createURI(TEST_JAR_DIR + "testjar4.jar"));
    list3.add(createURI(TEST_JAR_DIR + "testjar2.jar"));
    list3.add(createURI(TEST_JAR_DIR + "testjar5.jar"));

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
      actual.add(createURI(dependency));
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
      actual.add(createURI(dependency));
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

  private <T> List<T> union(List<T> list1, List<T> list2) {
    Set<T> set = new HashSet<T>();

    set.addAll(list1);
    set.addAll(list2);

    return new LinkedList<T>(set);
  }

}
