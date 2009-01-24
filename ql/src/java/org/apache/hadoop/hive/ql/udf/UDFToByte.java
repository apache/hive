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

package org.apache.hadoop.hive.ql.udf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;


public class UDFToByte extends UDF {

  private static Log LOG = LogFactory.getLog(UDFToByte.class.getName());

  public UDFToByte() {
  }

  /**
   * Convert from void to a byte. This is called for CAST(... AS TINYINT)
   *
   * @param i The void value to convert
   * @return Byte
   */
  public Byte evaluate(Void i)  {
    return null;
  }  

  /**
   * Convert from boolean to a byte. This is called for CAST(... AS TINYINT)
   *
   * @param i The boolean value to convert
   * @return Byte
   */
  public Byte evaluate(Boolean i)  {
    if (i == null) {
      return null;
    } else {
      return i.booleanValue() ? (byte)1 : (byte)0;
    }
  }  

  /**
   * Convert from short to a byte. This is called for CAST(... AS TINYINT)
   *
   * @param i The short value to convert
   * @return Byte
   */
  public Byte evaluate(Short i)  {
    if (i == null) {
      return null;
    } else {
      return Byte.valueOf(i.byteValue());
    }
  }

  /**
   * Convert from integer to a byte. This is called for CAST(... AS TINYINT)
   *
   * @param i The integer value to convert
   * @return Byte
   */
  public Byte evaluate(Integer i)  {
    if (i == null) {
      return null;
    } else {
      return Byte.valueOf(i.byteValue());
    }
  }

  /**
   * Convert from long to a byte. This is called for CAST(... AS TINYINT)
   *
   * @param i The long value to convert
   * @return Byte
   */
  public Byte evaluate(Long i)  {
    if (i == null) {
      return null;
    } else {
      return Byte.valueOf(i.byteValue());
    }
  }  

  /**
   * Convert from float to a byte. This is called for CAST(... AS TINYINT)
   *
   * @param i The float value to convert
   * @return Byte
   */
  public Byte evaluate(Float i)  {
    if (i == null) {
      return null;
    } else {
      return Byte.valueOf(i.byteValue());
    }
  }

  /**
   * Convert from double to a byte. This is called for CAST(... AS TINYINT)
   *
   * @param i The double value to convert
   * @return Byte
   */  
  public Byte evaluate(Double i)  {
    if (i == null) {
      return null;
    } else {
      return Byte.valueOf(i.byteValue());
    }
  }

  /**
   * Convert from string to a byte. This is called for CAST(... AS TINYINT)
   *
   * @param i The string value to convert
   * @return Byte
   */  
  public Byte evaluate(String i)  {
    if (i == null) {
      return null;
    } else {
      try {
        return Byte.valueOf(i);
      } catch (NumberFormatException e) {
        // MySQL returns 0 if the string is not a well-formed numeric value.
        // return Byte.valueOf(0);
        // But we decided to return NULL instead, which is more conservative.
        return null;
      }
    }
  }
  
  /**
   * Convert from date to a Byte. This is called for CAST(... AS TINYINT)
   *
   * @param i The date value to convert
   * @return Byte
   */
  public Byte evaluate(java.sql.Date i)  {
    if (i == null) {
      return null;
    } else {
        return Long.valueOf(i.getTime()).byteValue();
    }
  }  
}
