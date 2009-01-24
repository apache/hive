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


public class UDFToBoolean extends UDF {

  private static Log LOG = LogFactory.getLog(UDFToBoolean.class.getName());

  public UDFToBoolean() {
  }

  /**
   * Convert a void to boolean. This is called for CAST(... AS BOOLEAN)
   *
   * @param i The value of a void type
   * @return Boolean
   */
  public Boolean evaluate(Void i)  {
      return null;
  }

  /**
   * Convert from a byte to boolean. This is called for CAST(... AS BOOLEAN)
   *
   * @param i The byte value to convert
   * @return Boolean
   */
  public Boolean evaluate(Byte i)  {
    if (i == null) {
      return null;
    } else {
      return Boolean.valueOf(i.byteValue() != 0);
    }
  }

  /**
   * Convert from a short to boolean. This is called for CAST(... AS BOOLEAN)
   *
   * @param i The short value to convert
   * @return Boolean
   */
  public Boolean evaluate(Short i)  {
    if (i == null) {
      return null;
    } else {
      return Boolean.valueOf(i.shortValue() != 0);
    }
  }

  /**
   * Convert from a integer to boolean. This is called for CAST(... AS BOOLEAN)
   *
   * @param i The integer value to convert
   * @return Boolean
   */
  public Boolean evaluate(Integer i)  {
    if (i == null) {
      return null;
    } else {
      return Boolean.valueOf(i.intValue() != 0);
    }
  }

  /**
   * Convert from a long to boolean. This is called for CAST(... AS BOOLEAN)
   *
   * @param i The long value to convert
   * @return Boolean
   */
  public Boolean evaluate(Long i)  {
    if (i == null) {
      return null;
    } else {
      return Boolean.valueOf(i.longValue() != 0);
    }
  }

  /**
   * Convert from a float to boolean. This is called for CAST(... AS BOOLEAN)
   *
   * @param i The float value to convert
   * @return Boolean
   */  
  public Boolean evaluate(Float i)  {
    if (i == null) {
      return null;
    } else {
      return Boolean.valueOf(i.floatValue() != 0);
    }
  }

  /**
   * Convert from a double to boolean. This is called for CAST(... AS BOOLEAN)
   *
   * @param i The double value to convert
   * @return Boolean
   */
  public Boolean evaluate(Double i)  {
    if (i == null) {
      return null;
    } else {
      return Boolean.valueOf(i.doubleValue() != 0);
    }
  }

  /**
   * Convert from a string to boolean. This is called for CAST(... AS BOOLEAN)
   *
   * @param i The string value to convert
   * @return Boolean
   */
  public Boolean evaluate(String i)  {
    if (i == null) {
      return null;
    } else {
      return Boolean.valueOf(i.length() != 0);
    }
  }

}
