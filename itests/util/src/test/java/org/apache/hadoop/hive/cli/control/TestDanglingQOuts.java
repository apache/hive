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
package org.apache.hadoop.hive.cli.control;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FilenameFilter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.junit.Ignore;
import org.junit.Test;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

/**
 * This test ensures that there are no dangling q.out files in the project
 *
 * It has a cli functionlity to remove them if there are any.
 */
public class TestDanglingQOuts {

  public static class QOutFilter implements FilenameFilter {

    Pattern pattern = Pattern.compile(".*q.out$");

    @Override
    public boolean accept(File dir, String fileName) {
      return pattern.matcher(fileName).matches();
    }
  }

  public static class Params {
    @Parameter(names = "--delete", description = "Removes any unreferenced q.out")
    private boolean delete = false;

  }

  private Set<File> outsFound = new HashSet<>();
  private Map<File, AbstractCliConfig> outsNeeded = new HashMap<>();

  public TestDanglingQOuts() throws Exception {

    for (Class<?> clz : CliConfigs.class.getDeclaredClasses()) {
      handleCliConfig((AbstractCliConfig) clz.newInstance());
    }
  }

  private void handleCliConfig(AbstractCliConfig config) throws Exception {
    Set<File> qfiles = config.getQueryFiles();
    for (File file : qfiles) {
      String baseName = file.getName();
      String rd = config.getResultsDir();
      File of = new File(rd, baseName + ".out");
      if (outsNeeded.containsKey(of)) {
        System.err.printf("duplicate: [%s;%s] %s\n", config.getClass().getSimpleName(),
            outsNeeded.get(of).getClass().getSimpleName(), of);
        // throw new RuntimeException("duplicate?!");
      }
      outsNeeded.put(of, config);
    }

    File od = new File(config.getResultsDir());
    for (File file : od.listFiles(new QOutFilter())) {
      outsFound.add(file);
    }
  }

  @Test
  public void checkDanglingQOut() {
    SetView<File> dangling = Sets.difference(outsFound, outsNeeded.keySet());
    assertTrue("dangling qouts: "+dangling,dangling.isEmpty());
  }

  @Ignore("Seems like there are some from this class as well...")
  @Test
  public void checkMissingQOut() {
    SetView<File> missing = Sets.difference(outsNeeded.keySet(), outsFound);
    System.out.println(missing);
    assertTrue(missing.isEmpty());
  }

  public static void main(String[] args) throws Exception {

    Params params = new Params();
    new JCommander(params, args);

    TestDanglingQOuts c = new TestDanglingQOuts();

    Set<File> unused = Sets.difference(c.outsFound, c.outsNeeded.keySet());

    for (File file : unused) {
      System.out.println(file);
      if (params.delete) {
        file.delete();
      }
    }
  }
}
