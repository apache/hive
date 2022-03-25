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

package org.apache.hadoop.hive.metastore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hive.common.util.ReflectionUtil;

public class HiveMetaStoreUtils {

  protected static final Logger LOG = LoggerFactory.getLogger("hive.log");
  private static volatile HiveClientCache hiveClientCache;

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
      org.apache.hadoop.hive.metastore.api.Table table, String metaTable, boolean skipConfError) throws
      MetaException {
    String lib = table.getSd().getSerdeInfo().getSerializationLib();
    if (lib == null) {
      return null;
    }
    return getDeserializer(conf, table, metaTable, skipConfError, lib);
  }

  public static Deserializer getDeserializer(Configuration conf, org.apache.hadoop.hive.metastore.api.Table table,
      String metaTable, boolean skipConfError, String lib) throws MetaException {
    AbstractSerDe deserializer;
    try {
      deserializer = ReflectionUtil.newInstance(conf.getClassByName(lib).asSubclass(AbstractSerDe.class), conf);
    } catch (Exception e) {
      throw new MetaException(e.getClass().getName() + " " + e.getMessage());
    }

    try {
      Properties properties = MetaStoreUtils.getTableMetadata(table);
      if (metaTable != null) {
        properties.put("metaTable", metaTable);
      }
      deserializer.initialize(conf, properties, null);
    } catch (SerDeException e) {
      if (!skipConfError) {
        LOG.error("error in initSerDe: " + e.getClass().getName() + " " + e.getMessage(), e);
        throw new MetaException(e.getClass().getName() + " " + e.getMessage());
      }
    }
    return deserializer;
  }

  public static Class<? extends Deserializer> getDeserializerClass(
      Configuration conf, org.apache.hadoop.hive.metastore.api.Table table) throws Exception {
    String lib = table.getSd().getSerdeInfo().getSerializationLib();
    return lib == null ? null : conf.getClassByName(lib).asSubclass(Deserializer.class);
  }

  /**
   * getDeserializer
   *
   * Get the Deserializer for a partition.
   *
   * @param conf
   *          - hadoop config
   * @param part
   *          the partition
   * @param table the table
   * @return
   *   Returns instantiated deserializer by looking up class name of deserializer stored in
   *   storage descriptor of passed in partition. Also, initializes the deserializer with
   *   schema of partition.
   * @exception MetaException
   *              if any problems instantiating the Deserializer
   *
   */
  static public Deserializer getDeserializer(Configuration conf,
      org.apache.hadoop.hive.metastore.api.Partition part,
      org.apache.hadoop.hive.metastore.api.Table table) throws MetaException {
    String lib = part.getSd().getSerdeInfo().getSerializationLib();
    try {
      AbstractSerDe deserializer = ReflectionUtil.newInstance(conf.getClassByName(lib).
        asSubclass(AbstractSerDe.class), conf);
      deserializer.initialize(conf, MetaStoreUtils.getTableMetadata(table),
          MetaStoreUtils.getPartitionMetadata(part, table));
      return deserializer;
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable e) {
      LOG.error("error in initSerDe: " + e.getClass().getName() + " "
          + e.getMessage(), e);
      throw new MetaException(e.getClass().getName() + " " + e.getMessage());
    }
  }

  /**
   * @param tableName name of the table
   * @param deserializer deserializer to use
   * @param conf the configuration - used to determine if the deserializer needs to generate the 'from deserializer'
   *             comments
   * @return the list of fields
   * @throws SerDeException if the serde throws an exception
   * @throws MetaException if one of the fields or types in the table is invalid
   */
  public static List<FieldSchema> getFieldsFromDeserializer(String tableName,
      Deserializer deserializer, Configuration conf) throws SerDeException, MetaException {
    Collection<String> noCommentSerdes =
        MetastoreConf.getStringCollection(conf, MetastoreConf.ConfVars.SERDES_WITHOUT_FROM_DESERIALIZER);
    boolean noCommentFromDeserializer = noCommentSerdes.contains(deserializer.getClass().getName());
    ObjectInspector oi = deserializer.getObjectInspector();
    String[] names = tableName.split("\\.");
    String last_name = names[names.length - 1];
    for (int i = 2; i < names.length; i++) {

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
          determineFieldComment(null, noCommentFromDeserializer)));
    } else {
      List<? extends StructField> fields = ((StructObjectInspector) oi)
          .getAllStructFieldRefs();
      for (int i = 0; i < fields.size(); i++) {
        StructField structField = fields.get(i);
        String fieldName = structField.getFieldName();
        String fieldTypeName = structField.getFieldObjectInspector().getTypeName();
        String fieldComment = determineFieldComment(structField.getFieldComment(), noCommentFromDeserializer);

        str_fields.add(new FieldSchema(fieldName, fieldTypeName, fieldComment));
      }
    }
    return str_fields;
  }

  private static final String FROM_SERIALIZER = "from deserializer";
  private static String determineFieldComment(String comment, boolean noCommentFromDeserializer) {
    return (comment != null || noCommentFromDeserializer) ? comment : FROM_SERIALIZER;
  }

  /**
   * Convert TypeInfo to FieldSchema.
   */
  public static FieldSchema getFieldSchemaFromTypeInfo(String fieldName,
      TypeInfo typeInfo) {
    return new FieldSchema(fieldName, typeInfo.getTypeName(),
        "generated by TypeInfoUtils.getFieldSchemaFromTypeInfo");
  }

  /**
   * Get or create a hive client depending on whether it exits in cache or not
   * @param hiveConf The hive configuration
   * @return the client
   * @throws MetaException When HiveMetaStoreClient couldn't be created
   * @throws IOException
   */
  public static IMetaStoreClient getHiveMetastoreClient(HiveConf hiveConf)
    throws MetaException, IOException {

    if (!HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.METASTORE_CLIENT_CACHE_ENABLED)){
      // If cache is disabled, don't use it.
      return HiveClientCache.getNonCachedHiveMetastoreClient(hiveConf);
    }

    // Singleton behaviour: create the cache instance if required.
    if (hiveClientCache == null) {
      synchronized (IMetaStoreClient.class) {
        if (hiveClientCache == null) {
          hiveClientCache = new HiveClientCache(hiveConf);
        }
      }
    }
    try {
      return hiveClientCache.get(hiveConf);
    } catch (LoginException e) {
      throw new IOException("Couldn't create hiveMetaStoreClient, Error getting UGI for user", e);
    }
  }

}
