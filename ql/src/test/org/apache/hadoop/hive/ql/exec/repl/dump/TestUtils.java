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

package org.apache.hadoop.hive.ql.exec.repl.dump;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import static org.mockito.Mockito.times;

/**
 * Test class to test dump utils.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestUtils {

  @Mock
  Path outputFile;

  @Mock
  FileSystem fileSystem;

  @Mock
  FSDataOutputStream outputStream;

  @Test
  public void testCreate() throws SemanticException, IOException {
    HiveConf conf = new HiveConf();
    Mockito.when(outputFile.getFileSystem(conf)).thenReturn(fileSystem);
    Mockito.when(fileSystem.create(outputFile)).thenReturn(outputStream);
    Utils.create(outputFile, conf);
    Mockito.verify(outputStream, times(1)).close();
  }
}
