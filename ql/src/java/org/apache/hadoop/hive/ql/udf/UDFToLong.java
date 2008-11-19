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


public class UDFToLong implements UDF {

  private static Log LOG = LogFactory.getLog(UDFToLong.class.getName());

  public UDFToLong() {
  }

  /**
   * Convert from boolean to a long. This is called for CAST(... AS BIGINT)
   *
   * @param i The boolean value to convert
   * @return Long
   */
  public Long evaluate(Boolean i)  {
    if (i == null) {
      return null;
    } else {
      return i.booleanValue() ? (long)1 : (long)0;
    }
  }

  /**
   * Convert from byte to a long. This is called for CAST(... AS BIGINT)
   *
   * @param i The byte value to convert
   * @return Long
   */
  public Long evaluate(Byte i)  {
    if (i == null) {
      return null;
    } else {
      return Long.valueOf(i.longValue());
    }
  }
  
  /**
   * Convert from short to a long. This is called for CAST(... AS BIGINT)
   *
   * @param i The short value to convert
   * @return Long
   */
  public Long evaluate(Short i)  {
    if (i == null) {
      return null;
    } else {
      return Long.valueOf(i.longValue());
    }
  }
  
  /**
   * Convert from integer to a long. This is called for CAST(... AS BIGINT)
   *
   * @param i The integer value to convert
   * @return Long
   */
  public Long evaluate(Integer i)  {
    if (i == null) {
      return null;
    } else {
      return Long.valueOf(i.longValue());
    }
  }

  /**
   * Convert from long to a long. This is called for CAST(... AS BIGINT)
   *
   * @param i The long value to convert
   * @return Long
   */
  public Long evaluate(Long i)  {
    return i;
  }

  /**
   * Convert from float to a long. This is called for CAST(... AS BIGINT)
   *
   * @param i The float value to convert
   * @return Long
   */
  public Long evaluate(Float i)  {
    if (i == null) {
      return null;
    } else {
      return Long.valueOf(i.longValue());
    }
  }
  
  /**
   * Convert from double to a long. This is called for CAST(... AS BIGINT)
   *
   * @param i The double value to convert
   * @return Long
   */
  public Long evaluate(Double i)  {
    if (i == null) {
      return null;
    } else {
      return Long.valueOf(i.longValue());
    }
  }
  
  /**
   * Convert from string to a long. This is called for CAST(... AS BIGINT)
   *
   * @param i The string value to convert
   * @return Long
   */
  public Long evaluate(String i)  {
    if (i == null) {
      return null;
    } else {
      try {
        return Long.valueOf(i);
      } catch (NumberFormatException e) {
        // MySQL returns 0 if the string is not a well-formed numeric value.
        // return Long.valueOf(0);
        // But we decided to return NULL instead, which is more conservative.
        return null;
      }
    }
  }
  
}
