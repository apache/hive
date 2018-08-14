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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.JsonSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hive.common.util.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Generic Kafka Serde that allow user to delegate Serde to other class like Avro, Json or any class that supports
 * {@link BytesRefWritable}.
 */
public class GenericKafkaSerDe extends AbstractSerDe {
  private static final Logger LOG = LoggerFactory.getLogger(GenericKafkaSerDe.class);
  // ORDER of fields and types matters here
  public static final ImmutableList<String>
      METADATA_COLUMN_NAMES =
      ImmutableList.of(KafkaStorageHandler.PARTITION_COLUMN,
          KafkaStorageHandler.OFFSET_COLUMN,
          KafkaStorageHandler.TIMESTAMP_COLUMN);
  public static final ImmutableList<PrimitiveTypeInfo>
      METADATA_PRIMITIVE_TYPE_INFO =
      ImmutableList.of(TypeInfoFactory.intTypeInfo, TypeInfoFactory.longTypeInfo, TypeInfoFactory.longTypeInfo);

  private AbstractSerDe delegateSerDe;
  private ObjectInspector objectInspector;
  private final List<String> columnNames = Lists.newArrayList();
  StructObjectInspector delegateObjectInspector;

  @Override public void initialize(@Nullable Configuration conf, Properties tbl) throws SerDeException {
    final String className = tbl.getProperty(KafkaStorageHandler.SERDE_CLASS_NAME, KafkaJsonSerDe.class.getName());
    delegateSerDe = createDelegate(className);
    delegateSerDe.initialize(conf, tbl);
    LOG.info("Using SerDe instance {}", delegateSerDe.getClass().getCanonicalName());
    if (!(delegateSerDe.getObjectInspector() instanceof StructObjectInspector)) {
      throw new SerDeException("Was expecting StructObject Inspector but have " + delegateSerDe.getObjectInspector()
          .getClass()
          .getName());
    }

    delegateObjectInspector = (StructObjectInspector) delegateSerDe.getObjectInspector();

    final List<ObjectInspector> inspectors;
    // Get column names and types
    String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
    final String
        columnNameDelimiter =
        tbl.containsKey(serdeConstants.COLUMN_NAME_DELIMITER) ?
            tbl.getProperty(serdeConstants.COLUMN_NAME_DELIMITER) :
            String.valueOf(SerDeUtils.COMMA);
    // all table column names
    if (!columnNameProperty.isEmpty()) {
      columnNames.addAll(Arrays.asList(columnNameProperty.split(columnNameDelimiter)));
    }

    columnNames.addAll(METADATA_COLUMN_NAMES);

    if (LOG.isDebugEnabled()) {
      LOG.debug("columns: {}, {}", columnNameProperty, columnNames);
    }

    inspectors = new ArrayList<>(columnNames.size());
    inspectors.addAll(delegateObjectInspector.getAllStructFieldRefs()
        .stream()
        .map(structField -> structField.getFieldObjectInspector())
        .collect(Collectors.toList()));
    inspectors.addAll(METADATA_PRIMITIVE_TYPE_INFO.stream()
        .map(KafkaJsonSerDe.typeInfoToObjectInspector)
        .collect(Collectors.toList()));

    objectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, inspectors);
  }

  private AbstractSerDe createDelegate(String className) {
    final Class<? extends AbstractSerDe> clazz;
    try {
      clazz = (Class<? extends AbstractSerDe>) Class.forName(className);
    } catch (ClassNotFoundException e) {
      LOG.error("Failed a loading delegate SerDe {}", className);
      throw new RuntimeException(e);
    }
    // we are not setting conf thus null is okay
    return ReflectionUtil.newInstance(clazz, null);
  }

  @Override public Class<? extends Writable> getSerializedClass() {
    return delegateSerDe.getSerializedClass();
  }

  @Override public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    return delegateSerDe.serialize(obj, objInspector);
  }

  @Override public SerDeStats getSerDeStats() {
    return delegateSerDe.getSerDeStats();
  }

  @Override public Object deserialize(Writable blob) throws SerDeException {
    KafkaRecordWritable record = (KafkaRecordWritable) blob;
    // switch case the serde nature
    final Object row;
    if (delegateSerDe instanceof JsonSerDe) {
      // @TODO Text constructor copies the data, this op is not needed
      row = delegateSerDe.deserialize(new Text(record.getValue()));
    } else if (delegateSerDe instanceof AvroSerDe) {
      AvroGenericRecordWritable avroGenericRecordWritable = new AvroGenericRecordWritable();
      try {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(record.getValue());
        avroGenericRecordWritable.readFields(new DataInputStream(byteArrayInputStream));
      } catch (IOException e) {
        throw new SerDeException(e);
      }
      row = delegateSerDe.deserialize(avroGenericRecordWritable);
    } else {
      // default assuming delegate Serde know how to deal with
      row = delegateSerDe.deserialize(new BytesRefWritable(record.getValue()));
    }

    return columnNames.stream().map(name -> {
      switch (name) {
      case KafkaStorageHandler.PARTITION_COLUMN:
        return new IntWritable(record.getPartition());
      case KafkaStorageHandler.OFFSET_COLUMN:
        return new LongWritable(record.getOffset());
      case KafkaStorageHandler.TIMESTAMP_COLUMN:
        return new LongWritable(record.getTimestamp());
      default:
        return delegateObjectInspector.getStructFieldData(row, delegateObjectInspector.getStructFieldRef(name));
      }
    }).collect(Collectors.toList());
  }

  @Override public ObjectInspector getObjectInspector() {
    return objectInspector;
  }
}
