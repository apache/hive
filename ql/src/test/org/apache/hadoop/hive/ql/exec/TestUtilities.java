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

package org.apache.hadoop.hive.ql.exec;

import static org.apache.hadoop.hive.ql.exec.Utilities.getFileExtension;

import junit.framework.TestCase;

import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.mapred.JobConf;

public class TestUtilities extends TestCase {

  public void testGetFileExtension() {
    JobConf jc = new JobConf();
    assertEquals("No extension for uncompressed unknown format", "",
        getFileExtension(jc, false, null));
    assertEquals("No extension for compressed unknown format", "",
        getFileExtension(jc, true, null));
    assertEquals("No extension for uncompressed text format", "",
        getFileExtension(jc, false, new HiveIgnoreKeyTextOutputFormat()));
    assertEquals("Deflate for uncompressed text format", ".deflate",
        getFileExtension(jc, true, new HiveIgnoreKeyTextOutputFormat()));
    assertEquals("No extension for uncompressed default format", "",
        getFileExtension(jc, false));
    assertEquals("Deflate for uncompressed default format", ".deflate",
        getFileExtension(jc, true));

    String extension = ".myext";
    jc.set("hive.output.file.extension", extension);
    assertEquals("Custom extension for uncompressed unknown format", extension,
        getFileExtension(jc, false, null));
    assertEquals("Custom extension for compressed unknown format", extension,
        getFileExtension(jc, true, null));
    assertEquals("Custom extension for uncompressed text format", extension,
        getFileExtension(jc, false, new HiveIgnoreKeyTextOutputFormat()));
    assertEquals("Custom extension for uncompressed text format", extension,
        getFileExtension(jc, true, new HiveIgnoreKeyTextOutputFormat()));
  }
}
