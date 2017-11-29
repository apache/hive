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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class SerDeUtils {

  private static final String FROM_SERIALIZER = "from deserializer";
  private static final Logger LOG = LoggerFactory.getLogger(SerDeUtils.class.getName());

  private static String determineFieldComment(String comment) {
    return (comment == null) ? FROM_SERIALIZER : comment;
  }

  /**
   * Initializes a SerDe.
   * @param deserializer
   * @param conf
   * @param tblProps
   * @param partProps
   * @throws SerDeException
   */
  public static void initializeSerDeWithoutErrorCheck(Deserializer deserializer,
                                                      Configuration conf, Properties tblProps,
                                                      Properties partProps) throws SerDeException {
    if (deserializer instanceof AbstractSerDe) {
      ((AbstractSerDe) deserializer).initialize(conf, tblProps, partProps);
    } else {
      deserializer.initialize(conf, createOverlayedProperties(tblProps, partProps));
    }
  }

  /**
   * Returns the union of table and partition properties,
   * with partition properties taking precedence.
   * @param tblProps
   * @param partProps
   * @return the overlayed properties
   */
  public static Properties createOverlayedProperties(Properties tblProps, Properties partProps) {
    Properties props = new Properties();
    props.putAll(tblProps);
    if (partProps != null) {
      props.putAll(partProps);
    }
    return props;
  }

  /**
   * @param tableName name of the table
   * @param deserializer deserializer to use
   * @return the list of fields
   * @throws SerDeException if the serde throws an exception
   * @throws MetaException if one of the fields or types in the table is invalid
   */
  public static List<FieldSchema> getFieldsFromDeserializer(String tableName,
      Deserializer deserializer) throws SerDeException, MetaException {
    ObjectInspector oi = deserializer.getObjectInspector();
    String[] names = tableName.split("\\.");
    String last_name = names[names.length - 1];
    for (int i = 1; i < names.length; i++) {

      if (oi instanceof StructObjectInspector) {
        StructObjectInspector soi = (StructObjectInspector) oi;
        StructField sf = soi.getStructFieldRef(names[i]);
        if (sf == null) {
          throw new MetaException("Invalid Field " + names[i]);
        } else {
          oi = sf.getFieldObjectInspector();
        }
      } else if (oi instanceof ListObjectInspector
          && names[i].equalsIgnoreCase("$elem$")) {
        ListObjectInspector loi = (ListObjectInspector) oi;
        oi = loi.getListElementObjectInspector();
      } else if (oi instanceof MapObjectInspector
          && names[i].equalsIgnoreCase("$key$")) {
        MapObjectInspector moi = (MapObjectInspector) oi;
        oi = moi.getMapKeyObjectInspector();
      } else if (oi instanceof MapObjectInspector
          && names[i].equalsIgnoreCase("$value$")) {
        MapObjectInspector moi = (MapObjectInspector) oi;
        oi = moi.getMapValueObjectInspector();
      } else {
        throw new MetaException("Unknown type for " + names[i]);
      }
    }

    ArrayList<FieldSchema> str_fields = new ArrayList<>();
    // rules on how to recurse the ObjectInspector based on its type
    if (oi.getCategory() != Category.STRUCT) {
      str_fields.add(new FieldSchema(last_name, oi.getTypeName(),
          FROM_SERIALIZER));
    } else {
      List<? extends StructField> fields = ((StructObjectInspector) oi)
          .getAllStructFieldRefs();
      for (int i = 0; i < fields.size(); i++) {
        StructField structField = fields.get(i);
        String fieldName = structField.getFieldName();
        String fieldTypeName = structField.getFieldObjectInspector().getTypeName();
        String fieldComment = determineFieldComment(structField.getFieldComment());

        str_fields.add(new FieldSchema(fieldName, fieldTypeName, fieldComment));
      }
    }
    return str_fields;
  }

  /**
   * Initializes a SerDe.
   * @param deserializer
   * @param conf
   * @param tblProps
   * @param partProps
   * @throws SerDeException
   */
  public static void initializeSerDe(Deserializer deserializer, Configuration conf,
                                            Properties tblProps, Properties partProps)
                                                throws SerDeException {
    if (deserializer instanceof AbstractSerDe) {
      ((AbstractSerDe) deserializer).initialize(conf, tblProps, partProps);
      String msg = ((AbstractSerDe) deserializer).getConfigurationErrors();
      if (msg != null && !msg.isEmpty()) {
        throw new SerDeException(msg);
      }
    } else {
      deserializer.initialize(conf, createOverlayedProperties(tblProps, partProps));
    }
  }


  /**
   * getDeserializer
   *
   * Get the Deserializer for a table.
   *
   * @param conf
   *          - hadoop config
   * @param table
   *          the table
   * @return
   *   Returns instantiated deserializer by looking up class name of deserializer stored in
   *   storage descriptor of passed in table. Also, initializes the deserializer with schema
   *   of table.
   * @exception MetaException
   *              if any problems instantiating the Deserializer
   *
   *              todo - this should move somewhere into serde.jar
   *
   */
  static public Deserializer getDeserializer(Configuration conf,
      org.apache.hadoop.hive.metastore.api.Table table, boolean skipConfError) throws
          MetaException {
    String lib = table.getSd().getSerdeInfo().getSerializationLib();
    if (lib == null) {
      return null;
    }
    return getDeserializer(conf, table, skipConfError, lib);
  }

  public static Deserializer getDeserializer(Configuration conf,
      org.apache.hadoop.hive.metastore.api.Table table, boolean skipConfError,
      String lib) throws MetaException {
    try {
      Deserializer deserializer = JavaUtils.newInstance(conf.getClassByName(lib).
              asSubclass(Deserializer.class), conf);
      if (skipConfError) {
        SerDeUtils.initializeSerDeWithoutErrorCheck(deserializer, conf,
                MetaStoreUtils.getTableMetadata(table), null);
      } else {
        SerDeUtils.initializeSerDe(deserializer, conf, MetaStoreUtils.getTableMetadata(table), null);
      }
      return deserializer;
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable e) {
      LOG.error("error in initSerDe: " + e.getClass().getName() + " "
          + e.getMessage(), e);
      throw new MetaException(e.getClass().getName() + " " + e.getMessage());
    }
  }
}
