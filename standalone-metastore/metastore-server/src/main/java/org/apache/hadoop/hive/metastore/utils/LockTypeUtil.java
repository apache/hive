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
package org.apache.hadoop.hive.metastore.utils;

import com.google.common.collect.BiMap;
import com.google.common.collect.EnumHashBiMap;
import org.apache.hadoop.hive.metastore.api.LockType;

import java.util.Optional;

/**
 * Provides utility methods for {@link org.apache.hadoop.hive.metastore.api.LockType}.
 * One use case is to encapsulate each lock type's persistence encoding, since
 * Thrift-generated enums cannot be extended with character values.
 */
public class LockTypeUtil {

  private static final char UNKNOWN_LOCK_TYPE_ENCODING = 'z';

  private static final BiMap<LockType, Character> PERSISTENCE_ENCODINGS = EnumHashBiMap.create(LockType.class);
  static {
    PERSISTENCE_ENCODINGS.put(LockType.SHARED_READ, 'r');
    PERSISTENCE_ENCODINGS.put(LockType.SHARED_WRITE, 'w');
    PERSISTENCE_ENCODINGS.put(LockType.EXCL_WRITE, 'x');
    PERSISTENCE_ENCODINGS.put(LockType.EXCLUSIVE, 'e');
  }

  public static char getEncoding(LockType lockType) {
    return PERSISTENCE_ENCODINGS.getOrDefault(lockType, UNKNOWN_LOCK_TYPE_ENCODING);
  }

  public static String getEncodingAsStr(LockType lockType) {
    return Character.toString(getEncoding(lockType));
  }

  private static String getEncodingAsQuotedStr(LockType lockType) {
    return "'" + getEncodingAsStr(lockType) + "'";
  }

  public static Optional<LockType> getLockTypeFromEncoding(char encoding) {
    return Optional.ofNullable(PERSISTENCE_ENCODINGS.inverse().get(encoding));
  }

  public static String exclusive(){
    return getEncodingAsQuotedStr(LockType.EXCLUSIVE);
  }
  public static String exclWrite(){
    return getEncodingAsQuotedStr(LockType.EXCL_WRITE);
  }
  public static String sharedWrite(){
    return getEncodingAsQuotedStr(LockType.SHARED_WRITE);
  }
  public static String sharedRead(){
    return getEncodingAsQuotedStr(LockType.SHARED_READ);
  }

}
