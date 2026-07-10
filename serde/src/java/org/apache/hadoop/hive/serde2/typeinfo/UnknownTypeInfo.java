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

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;

/**
 * UnknownTypeInfo represents the TypeInfo of an Iceberg V3 unknown type in Hive.
 * <p>
 * Unknown columns are schema placeholders for undetermined types. They must be optional,
 * default to null, and are not stored in data files.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public final class UnknownTypeInfo extends TypeInfo implements Serializable {

  @Serial
  private static final long serialVersionUID = 1L;

  private static final UnknownTypeInfo INSTANCE = new UnknownTypeInfo();

  public static UnknownTypeInfo get() {
    return INSTANCE;
  }

  @Override
  public Category getCategory() {
    return Category.UNKNOWN;
  }

  @Override
  public String getTypeName() {
    return "unknown";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    return o instanceof UnknownTypeInfo;
  }

  @Override
  public int hashCode() {
    return Objects.hash(UnknownTypeInfo.class, getTypeName());
  }
}
