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

package org.apache.hadoop.hive.ql.io.orc;

/**
 * The name of a stream within a stripe.
 */
class StreamName implements Comparable<StreamName> {
  private final int column;
  private final OrcProto.Stream.Kind kind;

  public static enum Area {
    DATA, INDEX
  }

  public StreamName(int column, OrcProto.Stream.Kind kind) {
    this.column = column;
    this.kind = kind;
  }

  public boolean equals(Object obj) {
    if (obj != null && obj instanceof  StreamName) {
      StreamName other = (StreamName) obj;
      return other.column == column && other.kind == kind;
    } else {
      return false;
    }
  }

  @Override
  public int compareTo(StreamName streamName) {
    if (streamName == null) {
      return -1;
    }
    Area area = getArea(kind);
    Area otherArea = streamName.getArea(streamName.kind);
    if (area != otherArea) {
      return -area.compareTo(otherArea);
    }
    if (column != streamName.column) {
      return column < streamName.column ? -1 : 1;
    }
    return kind.compareTo(streamName.kind);
  }

  public int getColumn() {
    return column;
  }

  public OrcProto.Stream.Kind getKind() {
    return kind;
  }

  public Area getArea() {
    return getArea(kind);
  }

  public static Area getArea(OrcProto.Stream.Kind kind) {
    switch (kind) {
      case ROW_INDEX:
      case DICTIONARY_COUNT:
      case BLOOM_FILTER:
        return Area.INDEX;
      default:
        return Area.DATA;
    }
  }

  @Override
  public String toString() {
    return "Stream for column " + column + " kind " + kind;
  }

  @Override
  public int hashCode() {
    return column * 101 + kind.getNumber();
  }
}

