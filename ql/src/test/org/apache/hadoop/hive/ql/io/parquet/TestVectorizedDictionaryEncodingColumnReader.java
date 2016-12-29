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

package org.apache.hadoop.hive.ql.io.parquet;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class TestVectorizedDictionaryEncodingColumnReader extends VectorizedColumnReaderTestBase {
  static boolean isDictionaryEncoding = true;

  @BeforeClass
  public static void setup() throws IOException {
    removeFile();
    writeData(initWriterFromFile(), isDictionaryEncoding);
  }

  @AfterClass
  public static void cleanup() throws IOException {
    removeFile();
  }

  @Test
  public void testIntRead() throws Exception {
    intRead(isDictionaryEncoding);
  }

  @Test
  public void testLongRead() throws Exception {
    longRead(isDictionaryEncoding);
  }

  @Test
  public void testDoubleRead() throws Exception {
    doubleRead(isDictionaryEncoding);
  }

  @Test
  public void testFloatRead() throws Exception {
    floatRead(isDictionaryEncoding);
  }

  @Test
  public void testBinaryRead() throws Exception {
    binaryRead(isDictionaryEncoding);
  }

  @Test
  public void testStructRead() throws Exception {
    structRead(isDictionaryEncoding);
  }

  @Test
  public void testNestedStructRead() throws Exception {
    structRead(isDictionaryEncoding);
  }

  @Test
  public void structReadSomeNull() throws Exception {
    structReadSomeNull(isDictionaryEncoding);
  }

  @Test
  public void decimalRead() throws Exception {
    decimalRead(isDictionaryEncoding);
  }
}
