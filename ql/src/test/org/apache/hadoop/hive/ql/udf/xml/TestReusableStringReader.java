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

package org.apache.hadoop.hive.ql.udf.xml;

import java.io.IOException;
import java.io.Reader;

import org.apache.hadoop.hive.ql.udf.xml.UDFXPathUtil.ReusableStringReader;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestReusableStringReader {
  
  private static final String fox = "Quick brown fox jumps over the lazy dog."; 

  /**
   * Test empty {@link ReusableStringReader} 
   */
  @Test
  public void testEmpty() throws IOException {
    Reader reader = new ReusableStringReader();
    try {
      int ch = reader.read();
      fail("IOException expected.");
    } catch (IOException ioe) {
      // expected
    }
    try {
      boolean ready = reader.ready();
      fail("IOException expected.");
    } catch (IOException ioe) {
      // expected
    }
    reader.close();
  }
  
  @Test
  public void testMarkReset() throws IOException {
    Reader reader = new ReusableStringReader();
    if (reader.markSupported()) {
      ((ReusableStringReader)reader).set(fox);
      assertTrue(reader.ready());
      
      char[] cc = new char[6];
      int read;
      read = reader.read(cc);
      assertEquals(6, read);
      assertEquals("Quick ", new String(cc));
      
      reader.mark(100);
      
      read = reader.read(cc);
      assertEquals(6, read);
      assertEquals("brown ", new String(cc));
      
      reader.reset();
      read = reader.read(cc);
      assertEquals(6, read);
      assertEquals("brown ", new String(cc));
    }
    reader.close();
  }
  
  @Test
  public void testSkip() throws IOException {
    Reader reader = new ReusableStringReader();
    
    ((ReusableStringReader)reader).set(fox);
    // skip entire the data:
    long skipped = reader.skip(fox.length() + 1);
    assertEquals(fox.length(), skipped);  
    assertEquals(-1, reader.read());
    
    ((ReusableStringReader)reader).set(fox); // reset the data
    char[] cc = new char[6];
    int read;
    read = reader.read(cc);
    assertEquals(6, read);
    assertEquals("Quick ", new String(cc));
    
    // skip some piece of data:
    skipped = reader.skip(30);
    assertEquals(30, skipped);
    read = reader.read(cc);
    assertEquals(4, read);
    assertEquals("dog.", new String(cc, 0, read));

    // skip when already at EOF:
    skipped = reader.skip(300);
    assertEquals(0, skipped);  
    assertEquals(-1, reader.read());
    
    reader.close();
  }
}