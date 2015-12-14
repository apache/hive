/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.hcatalog.streaming.mutate.client;

public enum TableType {
  SOURCE((byte) 0),
  SINK((byte) 1);

  private static final TableType[] INDEX = buildIndex();

  private static TableType[] buildIndex() {
    TableType[] index = new TableType[TableType.values().length];
    for (TableType type : values()) {
      byte position = type.getId();
      if (index[position] != null) {
        throw new IllegalStateException("Overloaded index: " + position);
      }
      index[position] = type;
    }
    return index;
  }

  private byte id;

  private TableType(byte id) {
    this.id = id;
  }

  public byte getId() {
    return id;
  }

  public static TableType valueOf(byte id) {
    if (id < 0 || id >= INDEX.length) {
      throw new IllegalArgumentException("Invalid id: " + id);
    }
    return INDEX[id];
  }
}
