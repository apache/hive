/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.tez;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.junit.Assert;
import org.junit.Test;

public class TestTezSessionState {

  @Test
  public void testSymlinkedLocalFilesAreLocalizedOnce() throws Exception {
    Path jarPath = Files.createTempFile("jar", "");
    Path symlinkPath = Paths.get(jarPath.toString() + ".symlink");
    Files.createSymbolicLink(symlinkPath, jarPath);

    // write some data into the fake jar, it's not a 0 length file in real life
    Files.write(jarPath, "testSymlinkedLocalFilesToBeLocalized".getBytes(), StandardOpenOption.APPEND);

    Assert.assertTrue(Files.isSymbolicLink(symlinkPath));

    HiveConf hiveConf = new HiveConf();
    hiveConf.set(HiveConf.ConfVars.HIVE_JAR_DIRECTORY.varname, "/tmp");

    TezSessionState sessionState = new TezSessionState(DagUtils.getInstance(), hiveConf);

    LocalResource l1 = sessionState.createJarLocalResource(jarPath.toUri().toString());
    LocalResource l2 = sessionState.createJarLocalResource(symlinkPath.toUri().toString());

    // local resources point to the same original resource
    Assert.assertEquals(l1.getResource().toPath(), l2.getResource().toPath());
  }
}