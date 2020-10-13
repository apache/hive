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

package org.apache.hadoop.hive.ql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class TestPathCleaner {

  private static final String TEMP_DIR = TestPathCleaner.class.getName() + "-tempdir";

  @After
  public void tearDown() throws IOException {
    Path p = new Path(TEMP_DIR);
    FileSystem fs = p.getFileSystem(new Configuration());
    fs.delete(p, true);
  }

  @Test
  public void testDeleteManyFiles() throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    PathCleaner pathCleaner = new PathCleaner(TestPathCleaner.class.getName());
    pathCleaner.start();

    Collection<Path> files = createManyFiles(1000);

    for (Path p: files) {
      pathCleaner.deleteAsync(p, p.getFileSystem(conf));
    }

    pathCleaner.shutdown();
    pathCleaner.awaitTermination(600000);

    for (Path p : files) {
      FileSystem fs = p.getFileSystem(conf);
      assertNotNull(fs);
      assertFalse(p + " should not exist", fs.exists(p));
    }
  }

  private Path createFile(String name) throws IOException {
    Path p = new Path(TEMP_DIR, name);
    FileSystem fs = p.getFileSystem(new Configuration());
    fs.create(p);
    return p;
  }

  private Collection<Path> createManyFiles(int n) throws IOException {
    Collection<Path> files = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      files.add(createFile("many_" + i));
    }
    return files;
  }
}
