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

package org.apache.hadoop.hive.ql.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Writer;

import junit.framework.TestCase;

public class TestDosToUnix extends TestCase {

  private static final String dataFile = System.getProperty("test.tmp.dir", ".") + "data_TestDosToUnix";
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    Writer output = null;
    String text = "#!/usr/bin/env ruby \r\n Test date \r\n More test data.\r\n";
    File file = new File(dataFile);
    output = new BufferedWriter(new FileWriter(file));
    output.write(text);
    output.close();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    File f = new File(dataFile);
    if(!f.delete()) {
      throw new RuntimeException("Could not delete the data file");
    }
  }

  public void testIsWindowsScript() {
    File file = new File(dataFile);
    assertEquals(true, DosToUnix.isWindowsScript(file));
  }

  public void testGetUnixScriptNameFor() {
    assertEquals("test_unix", DosToUnix.getUnixScriptNameFor("test"));
    assertEquals("test_unix.rb", DosToUnix.getUnixScriptNameFor("test.rb"));
  }

  public void testConvertWindowsScriptToUnix() {
    File file = new File(dataFile);
    try {
      assertEquals(true, DosToUnix.isWindowsScript(file));
      String convertedFile = DosToUnix.convertWindowsScriptToUnix(file);
      File cFile = new File(convertedFile);
      assertEquals(false, DosToUnix.isWindowsScript(cFile));
      if(!cFile.delete()) {
        throw new RuntimeException("Could not delete the converted data file");
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }

  }
}
