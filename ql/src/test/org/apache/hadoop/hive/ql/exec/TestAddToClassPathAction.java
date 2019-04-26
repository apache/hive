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

package org.apache.hadoop.hive.ql.exec;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.security.AccessController;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

/**
 * Minimal tests for AddToClassPathAction class. Most of the tests don't use
 * {@link java.security.AccessController#doPrivileged(java.security.PrivilegedAction)},
 * presumably the tests will not be executed under security manager.
 */
public class TestAddToClassPathAction {

  private ClassLoader originalClassLoader;

  private static void assertURLsMatch(String message, List<String> expected, URL[] actual) {
    List<String> actualStrings = Arrays.stream(actual).map(URL::toExternalForm).collect(Collectors.toList());
    assertEquals(message, expected, actualStrings);
  }

  private static void assertURLsMatch(List<String> expected, URL[] actual) {
    assertURLsMatch("", expected, actual);
  }

  @Before
  public void saveClassLoader() {
    originalClassLoader = Thread.currentThread().getContextClassLoader();
  }

  @After
  public void restoreClassLoader() {
    Thread.currentThread().setContextClassLoader(originalClassLoader);
  }

  @Test
  public void testNullClassLoader() {
    try {
      new AddToClassPathAction(null, Collections.emptyList());
      fail("When pafrent class loader is null, IllegalArgumentException is expected!");
    } catch (IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void testNullPaths() {
    ClassLoader rootLoader = Thread.currentThread().getContextClassLoader();
    AddToClassPathAction action = new AddToClassPathAction(rootLoader, null);
    UDFClassLoader childLoader = action.run();
    assertURLsMatch(
        "When newPaths is null, loader shall be created normally with no extra paths.",
        Collections.emptyList(), childLoader.getURLs());
  }

  @Test
  public void testUseExisting() {
    ClassLoader rootLoader = Thread.currentThread().getContextClassLoader();
    AddToClassPathAction action1 = new AddToClassPathAction(rootLoader, Arrays.asList("/a/1", "/c/3"));
    UDFClassLoader parentLoader = action1.run();
    AddToClassPathAction action2 = new AddToClassPathAction(parentLoader, Arrays.asList("/b/2", "/d/4"));
    UDFClassLoader childLoader = action2.run();
    assertSame(
        "Normally, the existing class loader should be reused (not closed, no force new).",
        parentLoader, childLoader);
    assertURLsMatch(
        "The class path of the class loader should be updated.",
        Arrays.asList("file:/a/1", "file:/c/3", "file:/b/2", "file:/d/4"), childLoader.getURLs());
  }

  @Test
  public void testClosed() throws IOException {
    ClassLoader rootLoader = Thread.currentThread().getContextClassLoader();
    AddToClassPathAction action1 = new AddToClassPathAction(rootLoader, Arrays.asList("/a/1", "/c/3"));
    UDFClassLoader parentLoader = action1.run();
    parentLoader.close();
    AddToClassPathAction action2 = new AddToClassPathAction(parentLoader, Arrays.asList("/b/2", "/d/4"));
    UDFClassLoader childLoader = action2.run();
    assertNotSame(
        "When the parent class loader is closed, a new instance must be created.",
        parentLoader, childLoader);
    assertURLsMatch(Arrays.asList("file:/b/2", "file:/d/4"), childLoader.getURLs());
  }

  @Test
  public void testForceNew() {
    ClassLoader rootLoader = Thread.currentThread().getContextClassLoader();
    AddToClassPathAction action1 = new AddToClassPathAction(rootLoader, Arrays.asList("/a/1", "/c/3"));
    UDFClassLoader parentLoader = action1.run();
    AddToClassPathAction action2 = new AddToClassPathAction(parentLoader, Arrays.asList("/b/2", "/d/4"), true);
    UDFClassLoader childLoader = action2.run();
    assertNotSame(
        "When forceNewClassLoader is set, a new instance must be created.",
        parentLoader, childLoader);
    assertURLsMatch(Arrays.asList("file:/b/2", "file:/d/4"), childLoader.getURLs());
  }

  @Test
  public void testLegalPaths() {
    ClassLoader rootLoader = Thread.currentThread().getContextClassLoader();
    List<String> newPaths = Arrays.asList("file://a/aa", "c/cc", "/bb/b");
    String userDir = System.getProperty("user.dir");
    List<String> expectedURLs = Arrays.asList(
        "file://a/aa",
        "file:" + userDir + "/c/cc",
        "file:/bb/b");
    AddToClassPathAction action = new AddToClassPathAction(rootLoader, newPaths);
    UDFClassLoader loader = AccessController.doPrivileged(action);
    assertURLsMatch(expectedURLs, loader.getURLs());
  }

}
