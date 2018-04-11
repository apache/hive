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

package org.apache.hadoop.hive.serde2.typeinfo;

import org.apache.hadoop.hive.common.type.HiveBaseChar;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.io.HiveBaseCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;

public class BaseCharUtils {

  public static void validateVarcharParameter(int length) {
    if (length > HiveVarchar.MAX_VARCHAR_LENGTH || length < 1) {
      throw new RuntimeException("Varchar length " + length + " out of allowed range [1, " +
          HiveVarchar.MAX_VARCHAR_LENGTH + "]");
    }
  }

  public static void validateCharParameter(int length) {
    if (length > HiveChar.MAX_CHAR_LENGTH || length < 1) {
      throw new RuntimeException("Char length " + length + " out of allowed range [1, " +
          HiveChar.MAX_CHAR_LENGTH + "]");
    }
  }

  public static boolean doesWritableMatchTypeParams(HiveBaseCharWritable writable,
      BaseCharTypeInfo typeInfo) {
    return typeInfo.getLength() >= writable.getCharacterLength();
  }

  public static boolean doesPrimitiveMatchTypeParams(HiveBaseChar value,
      BaseCharTypeInfo typeInfo) {
    return typeInfo.getLength() == value.getCharacterLength();
  }
}
