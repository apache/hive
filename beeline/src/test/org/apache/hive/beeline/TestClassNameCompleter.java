/**
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

package org.apache.hive.beeline;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

public class TestClassNameCompleter {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void addingAndEmptyJarFile() throws IOException {

    String fileName = "empty.file.jar";
    File p = tmpFolder.newFile(fileName);

    URLClassLoader classLoader = (URLClassLoader) Thread.currentThread().getContextClassLoader();
    try {
      URLClassLoader newClassLoader = new URLClassLoader(new URL[] { p.toURL() }, classLoader);

      Thread.currentThread().setContextClassLoader(newClassLoader);
      ClassNameCompleter.getClassNames();
      fail("an exception was expected!");
    } catch (IOException e) {
      assertTrue("Exception message should contain the filename!",
          e.getMessage().indexOf(fileName) >= 0);
    } finally {
      Thread.currentThread().setContextClassLoader(classLoader);
    }

  }

  @Test
  public void addingEmptyFile() throws IOException {

    String fileName = "empty.file";
    File p = tmpFolder.newFile(fileName);

    URLClassLoader classLoader = (URLClassLoader) Thread.currentThread().getContextClassLoader();
    try {
      URLClassLoader newClassLoader = new URLClassLoader(new URL[] { p.toURL() }, classLoader);

      Thread.currentThread().setContextClassLoader(newClassLoader);
      ClassNameCompleter.getClassNames();
    } finally {
      Thread.currentThread().setContextClassLoader(classLoader);
    }

  }
}
