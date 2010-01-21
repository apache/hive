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

/**
 * JDBM LICENSE v1.00
 *
 * Redistribution and use of this software and associated documentation
 * ("Software"), with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * 1. Redistributions of source code must retain copyright
 *    statements and notices.  Redistributions must also contain a
 *    copy of this document.
 *
 * 2. Redistributions in binary form must reproduce the
 *    above copyright notice, this list of conditions and the
 *    following disclaimer in the documentation and/or other
 *    materials provided with the distribution.
 *
 * 3. The name "JDBM" must not be used to endorse or promote
 *    products derived from this Software without prior written
 *    permission of Cees de Groot.  For written permission,
 *    please contact cg@cdegroot.com.
 *
 * 4. Products derived from this Software may not be called "JDBM"
 *    nor may "JDBM" appear in their names without prior written
 *    permission of Cees de Groot.
 *
 * 5. Due credit should be given to the JDBM Project
 *    (http://jdbm.sourceforge.net/).
 *
 * THIS SOFTWARE IS PROVIDED BY THE JDBM PROJECT AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT
 * NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
 * CEES DE GROOT OR ANY CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * Copyright 2001 (C) Alex Boisvert. All Rights Reserved.
 * Contributions are Copyright (C) 2001 by their associated contributors.
 *
 */

package org.apache.hadoop.hive.ql.util.jdbm.helper;

/**
 * Miscelaneous conversion utility methods.
 * 
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @version $Id: Conversion.java,v 1.3 2002/05/31 06:33:20 boisvert Exp $
 */
public class Conversion {

  /**
   * Convert a string into a byte array.
   */
  public static byte[] convertToByteArray(String s) {
    try {
      // see the following page for character encoding
      // http://java.sun.com/products/jdk/1.1/docs/guide/intl/encoding.doc.html
      return s.getBytes("UTF8");
    } catch (java.io.UnsupportedEncodingException uee) {
      uee.printStackTrace();
      throw new Error("Platform doesn't support UTF8 encoding");
    }
  }

  /**
   * Convert a byte into a byte array.
   */
  public static byte[] convertToByteArray(byte n) {
    n = (byte) (n ^ ((byte) 0x80)); // flip MSB because "byte" is signed
    return new byte[] { n };
  }

  /**
   * Convert a short into a byte array.
   */
  public static byte[] convertToByteArray(short n) {
    n = (short) (n ^ ((short) 0x8000)); // flip MSB because "short" is signed
    byte[] key = new byte[2];
    pack2(key, 0, n);
    return key;
  }

  /**
   * Convert an int into a byte array.
   */
  public static byte[] convertToByteArray(int n) {
    n = (n ^ 0x80000000); // flip MSB because "int" is signed
    byte[] key = new byte[4];
    pack4(key, 0, n);
    return key;
  }

  /**
   * Convert a long into a byte array.
   */
  public static byte[] convertToByteArray(long n) {
    n = (n ^ 0x8000000000000000L); // flip MSB because "long" is signed
    byte[] key = new byte[8];
    pack8(key, 0, n);
    return key;
  }

  /**
   * Convert a byte array (encoded as UTF-8) into a String
   */
  public static String convertToString(byte[] buf) {
    try {
      // see the following page for character encoding
      // http://java.sun.com/products/jdk/1.1/docs/guide/intl/encoding.doc.html
      return new String(buf, "UTF8");
    } catch (java.io.UnsupportedEncodingException uee) {
      uee.printStackTrace();
      throw new Error("Platform doesn't support UTF8 encoding");
    }
  }

  /**
   * Convert a byte array into an integer (signed 32-bit) value.
   */
  public static int convertToInt(byte[] buf) {
    int value = unpack4(buf, 0);
    value = (value ^ 0x80000000); // flip MSB because "int" is signed
    return value;
  }

  /**
   * Convert a byte array into a long (signed 64-bit) value.
   */
  public static long convertToLong(byte[] buf) {
    long value = ((long) unpack4(buf, 0) << 32)
        + (unpack4(buf, 4) & 0xFFFFFFFFL);
    value = (value ^ 0x8000000000000000L); // flip MSB because "long" is signed
    return value;
  }

  static int unpack4(byte[] buf, int offset) {
    int value = (buf[offset] << 24) | ((buf[offset + 1] << 16) & 0x00FF0000)
        | ((buf[offset + 2] << 8) & 0x0000FF00)
        | ((buf[offset + 3] << 0) & 0x000000FF);

    return value;
  }

  static final void pack2(byte[] data, int offs, int val) {
    data[offs++] = (byte) (val >> 8);
    data[offs++] = (byte) val;
  }

  static final void pack4(byte[] data, int offs, int val) {
    data[offs++] = (byte) (val >> 24);
    data[offs++] = (byte) (val >> 16);
    data[offs++] = (byte) (val >> 8);
    data[offs++] = (byte) val;
  }

  static final void pack8(byte[] data, int offs, long val) {
    pack4(data, 0, (int) (val >> 32));
    pack4(data, 4, (int) val);
  }

  /**
   * Test static methods
   */
  public static void main(String[] args) {
    byte[] buf;

    buf = convertToByteArray(5);
    System.out.println("int value of 5 is: " + convertToInt(buf));

    buf = convertToByteArray(-1);
    System.out.println("int value of -1 is: " + convertToInt(buf));

    buf = convertToByteArray(22111000);
    System.out.println("int value of 22111000 is: " + convertToInt(buf));

    buf = convertToByteArray(5L);
    System.out.println("long value of 5 is: " + convertToLong(buf));

    buf = convertToByteArray(-1L);
    System.out.println("long value of -1 is: " + convertToLong(buf));

    buf = convertToByteArray(1112223334445556667L);
    System.out.println("long value of 1112223334445556667 is: "
        + convertToLong(buf));
  }

}
