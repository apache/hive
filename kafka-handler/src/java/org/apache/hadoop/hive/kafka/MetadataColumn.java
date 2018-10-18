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

package org.apache.hadoop.hive.kafka;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveWritableObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Enum class for all the metadata columns appended to the Kafka row by the Hive Serializer/Deserializer.
 *
 * <p>
 *<b>Design Notes:</b>
 *
 * It is important to note that the order at which columns are appended matters, the order is governed by:
 * {@link MetadataColumn#KAFKA_METADATA_COLUMNS}.
 *
 * If you add a new Column make sure to added its Writable converter to {@link KafkaWritable}.
 *
 */
enum MetadataColumn {

  /**
   * Kafka Record's offset column name added as extra metadata column to row as long.
   */
  OFFSET("__offset", TypeInfoFactory.longTypeInfo),
  /**
   * Record Kafka Partition column name added as extra meta column of type int.
   */
  PARTITION("__partition", TypeInfoFactory.intTypeInfo),
  /**
   * Record Kafka key column name added as extra meta column of type binary blob.
   */
  KEY("__key", TypeInfoFactory.binaryTypeInfo),
  /**
   * Record Timestamp column name, added as extra meta column of type long.
   */
  TIMESTAMP("__timestamp", TypeInfoFactory.longTypeInfo),
  /**
   * Start offset given by the input split, this will reflect the actual start of TP or start given by split pruner.
   */
  // @TODO To be removed next PR it is here to make review easy
  START_OFFSET("__start_offset", TypeInfoFactory.longTypeInfo),
  /**
   * End offset given by input split at run time.
   */
  // @TODO To be removed next PR it is here to make review easy
  END_OFFSET("__end_offset", TypeInfoFactory.longTypeInfo);

  /**
   * Kafka metadata columns list that indicates the order of appearance for each column in final row.
   */
  private static final List<MetadataColumn>
      KAFKA_METADATA_COLUMNS =
      Arrays.asList(KEY, PARTITION, OFFSET, TIMESTAMP, START_OFFSET, END_OFFSET);

  static final List<ObjectInspector>
      KAFKA_METADATA_INSPECTORS =
      KAFKA_METADATA_COLUMNS.stream().map(MetadataColumn::getObjectInspector).collect(Collectors.toList());

  static final List<String>
      KAFKA_METADATA_COLUMN_NAMES =
      KAFKA_METADATA_COLUMNS.stream().map(MetadataColumn::getName).collect(Collectors.toList());

  private final String name;
  private final TypeInfo typeInfo;

  MetadataColumn(String name, TypeInfo typeInfo) {
    this.name = name;
    this.typeInfo = typeInfo;
  }

  public String getName() {
    return name;
  }

  public AbstractPrimitiveWritableObjectInspector getObjectInspector() {
    return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getPrimitiveTypeInfo(
        typeInfo.getTypeName()));
  }

  private static final Map<String, MetadataColumn>
      NAMES_MAP =
      Arrays.stream(MetadataColumn.values()).collect(Collectors.toMap(MetadataColumn::getName, Function.identity()));
  /**
   * Column name to MetadataColumn instance.
   * @param name column name.
   * @return instance of {@link MetadataColumn} or null if column name is absent
   */
  @Nullable
  static MetadataColumn forName(String name) {
    return NAMES_MAP.get(name);
  }

}
