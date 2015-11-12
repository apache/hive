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

package org.apache.hive.common.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

import com.google.common.io.Files;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class HiveTestUtils {
  public static final Logger LOG = LoggerFactory.getLogger(HiveTestUtils.class);

  public final static String JAVA_FILE_EXT = ".java";
  public final static String CLAZZ_FILE_EXT = ".class";
  public final static String JAR_FILE_EXT = ".jar";
  public final static String TXT_FILE_EXT = ".txt";

  public static String getFileFromClasspath(String name) {
    URL url = ClassLoader.getSystemResource(name);
    if (url == null) {
      throw new IllegalArgumentException("Could not find " + name);
    }
    return url.getPath();
  }

  private static void executeCmd(String[] cmdArr, File dir) throws IOException, InterruptedException {
    final Process p1 = Runtime.getRuntime().exec(cmdArr, null, dir);
    new Thread(new Runnable() {
      @Override
      public void run() {
        BufferedReader input = new BufferedReader(new InputStreamReader(p1.getErrorStream()));
        String line;
        try {
          while ((line = input.readLine()) != null) {
            System.out.println(line);
          }
        } catch (IOException e) {
          LOG.error("Failed to execute the command due the exception " + e);
        }
      }
    }).start();
    p1.waitFor();
  }

  public static File genLocalJarForTest(String pathToClazzFile, String clazzName)
      throws IOException, InterruptedException {
    String u = pathToClazzFile;
    File dir = new File(u);
    File parentDir = dir.getParentFile();
    File f = new File(parentDir, clazzName + JAVA_FILE_EXT);
    Files.copy(dir, f);
    executeCmd(new String[] { "javac", clazzName + JAVA_FILE_EXT }, parentDir);
    executeCmd(new String[] { "jar", "cf", clazzName + JAR_FILE_EXT, clazzName + CLAZZ_FILE_EXT },
        parentDir);
    f.delete();
    new File(parentDir, clazzName + CLAZZ_FILE_EXT).delete();
    return new File(parentDir, clazzName + JAR_FILE_EXT);
  }
}
