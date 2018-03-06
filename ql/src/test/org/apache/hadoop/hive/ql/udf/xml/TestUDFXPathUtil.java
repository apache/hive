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

import javax.xml.xpath.XPathConstants;

import org.junit.Test;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import static org.junit.Assert.*;

public class TestUDFXPathUtil {

  @Test
  public void testEvalIllegalArgs() {
    UDFXPathUtil util = new UDFXPathUtil();
    
    // null args:
    assertNull(util.eval(null, "a/text()", XPathConstants.STRING));
    assertNull(util.eval("<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>", null, XPathConstants.STRING));
    assertNull(util.eval("<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/text()", null));
    
    // empty String args: 
    assertNull(util.eval("", "a/text()", XPathConstants.STRING));
    assertNull(util.eval("<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>", "", XPathConstants.STRING));
    
    // wrong expression:
    assertNull(util.eval("<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/text(", XPathConstants.STRING));
  }

  @Test
  public void testEvalPositive() {
    UDFXPathUtil util = new UDFXPathUtil();
    
    Object result = util.eval("<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/c[2]/text()", XPathConstants.STRING);
    assertEquals("c2", result);
    
    result = util.evalBoolean("<a><b>true</b><b>false</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/b[1]/text()");
    assertEquals(Boolean.TRUE, result);
    result = util.evalBoolean("<a><b>true</b><b>false</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/b[4]");
    assertEquals(Boolean.FALSE, result);
    
    result = util.evalString("<a><b>true</b><b>false</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/b[3]/text()");
    assertEquals("b3", result);
    result = util.evalString("<a><b>true</b><b>false</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/b[4]/text()");
    assertEquals("", result);
    result = util.evalString("<a><b>true</b><b k=\"foo\">FALSE</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/b[2]/@k");
    assertEquals("foo", result);
    
    result = util.evalNumber("<a><b>true</b><b>false</b><b>b3</b><c>c1</c><c>-77</c></a>", "a/c[2]");
    assertEquals(-77.0d, result);
    result = util.evalNumber("<a><b>true</b><b k=\"foo\">FALSE</b><b>b3</b><c>c1</c><c>c2</c></a>", "a/b[2]/@k");
    assertEquals(Double.NaN, result);
    
    result = util.evalNode("<a><b>true</b><b>false</b><b>b3</b><c>c1</c><c>-77</c></a>", "a/c[2]");
    assertNotNull(result);
    assertTrue(result instanceof Node);
    
    result = util.evalNodeList("<a><b>true</b><b>false</b><b>b3</b><c>c1</c><c>-77</c></a>", "a/*");
    assertNotNull(result);
    assertTrue(result instanceof NodeList);
    assertEquals(5, ((NodeList)result).getLength());
  }
  
}
