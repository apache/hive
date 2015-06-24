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

package org.apache.hadoop.hive.ql.io.parquet.convert;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

public class HiveCollectionConverter extends HiveGroupConverter {
  private final GroupType collectionType;
  private final ConverterParent parent;
  private final int index;
  private final Converter innerConverter;
  private final List<Writable> list = new ArrayList<Writable>();

  public static HiveGroupConverter forMap(GroupType mapType,
                                          ConverterParent parent,
                                          int index) {
    return new HiveCollectionConverter(
        mapType, parent, index, true /* its a map */ );
  }

  public static HiveGroupConverter forList(GroupType listType,
                                           ConverterParent parent,
                                           int index) {
    return new HiveCollectionConverter(
      listType, parent, index, false /* not a map */);
  }

  private HiveCollectionConverter(GroupType collectionType,
                                  ConverterParent parent,
                                  int index, boolean isMap) {
    setMetadata(parent.getMetadata());
    this.collectionType = collectionType;
    this.parent = parent;
    this.index = index;
    Type repeatedType = collectionType.getType(0);
    if (isMap) {
      this.innerConverter = new KeyValueConverter(
          repeatedType.asGroupType(), this);
    } else if (isElementType(repeatedType, collectionType.getName())) {
      this.innerConverter = getConverterFromDescription(repeatedType, 0, this);
    } else {
      this.innerConverter = new ElementConverter(
          repeatedType.asGroupType(), this);
    }
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    Preconditions.checkArgument(
        fieldIndex == 0, "Invalid field index: " + fieldIndex);
    return innerConverter;
  }

  @Override
  public void start() {
    list.clear();
  }

  @Override
  public void end() {
    parent.set(index, new ArrayWritable(
        Writable.class, list.toArray(new Writable[0])));
  }

  @Override
  public void set(int index, Writable value) {
    list.add(value);
  }

  private static class KeyValueConverter extends HiveGroupConverter {
    private final HiveGroupConverter parent;
    private final Converter keyConverter;
    private final Converter valueConverter;
    private Writable[] keyValue = null;

    public KeyValueConverter(GroupType keyValueType, HiveGroupConverter parent) {
      setMetadata(parent.getMetadata());
      this.parent = parent;
      this.keyConverter = getConverterFromDescription(
          keyValueType.getType(0), 0, this);
      this.valueConverter = getConverterFromDescription(
          keyValueType.getType(1), 1, this);
    }

    @Override
    public void set(int fieldIndex, Writable value) {
      keyValue[fieldIndex] = value;
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      switch (fieldIndex) {
        case 0:
          return keyConverter;
        case 1:
          return valueConverter;
        default:
          throw new IllegalArgumentException(
              "Invalid field index for map key-value: " + fieldIndex);
      }
    }

    @Override
    public void start() {
      this.keyValue = new Writable[2];
    }

    @Override
    public void end() {
      parent.set(0, new ArrayWritable(Writable.class, keyValue));
    }
  }

  private static class ElementConverter extends HiveGroupConverter {
    private final HiveGroupConverter parent;
    private final Converter elementConverter;
    private Writable element = null;

    public ElementConverter(GroupType repeatedType, HiveGroupConverter parent) {
      setMetadata(parent.getMetadata());
      this.parent = parent;
      this.elementConverter = getConverterFromDescription(
          repeatedType.getType(0), 0, this);
    }

    @Override
    public void set(int index, Writable value) {
      this.element = value;
    }

    @Override
    public Converter getConverter(int i) {
      return elementConverter;
    }

    @Override
    public void start() {
      this.element = null;
    }

    @Override
    public void end() {
      parent.set(0, element);
    }
  }

  private static boolean isElementType(Type repeatedType, String parentName) {
    if (repeatedType.isPrimitive() ||
        (repeatedType.asGroupType().getFieldCount() != 1)) {
      return true;
    } else if (repeatedType.getName().equals("array")) {
      return true; // existing avro data
    } else if (repeatedType.getName().equals(parentName + "_tuple")) {
      return true; // existing thrift data
    }
    // false for the following cases:
    // * name is "list", which matches the spec
    // * name is "bag", which indicates existing hive or pig data
    // * ambiguous case, which should be assumed is 3-level according to spec
    return false;
  }
}
