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
package org.apache.hadoop.hive.hbase;

import static org.apache.hadoop.hive.hbase.HBaseSerDeParameters.AVRO_SERIALIZATION_TYPE;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.hbase.ColumnMappings.ColumnMapping;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.avro.AvroObjectInspectorGenerator;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils.AvroTableProperties;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyObjectBase;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.util.StringUtils;

/**
 * Helper class for {@link HBaseSerDe}
 * */
public class HBaseSerDeHelper {

  /**
   * Logger
   * */
  public static final Logger LOG = LoggerFactory.getLogger(HBaseSerDeHelper.class);

  /**
   * Autogenerates the columns from the given serialization class
   * 
   * @param tbl the hive table properties
   * @param columnsMapping the hbase columns mapping determining hbase column families and
   *          qualifiers
   * @param sb StringBuilder to form the list of columns
   * @throws IllegalArgumentException if any of the given arguments was null
   * */
  public static void generateColumns(Properties tbl, List<ColumnMapping> columnsMapping,
      StringBuilder sb) {
    // Generate the columns according to the column mapping provided
    // Note: The generated column names are same as the
    // family_name.qualifier_name. If the qualifier
    // name is null, each column is familyname_col[i] where i is the index of
    // the column ranging
    // from 0 to n-1 where n is the size of the column mapping. The filter
    // function removes any
    // special characters other than alphabets and numbers from the column
    // family and qualifier name
    // as the only special character allowed in a column name is "_" which is
    // used as a separator
    // between the column family and qualifier name.

    if (columnsMapping == null) {
      throw new IllegalArgumentException("columnsMapping cannot be null");
    }

    if (sb == null) {
      throw new IllegalArgumentException("StringBuilder cannot be null");
    }

    for (int i = 0; i < columnsMapping.size(); i++) {
      ColumnMapping colMap = columnsMapping.get(i);

      if (colMap.hbaseRowKey) {
        sb.append("key").append(StringUtils.COMMA_STR);
      } else if (colMap.qualifierName == null) {
        // this corresponds to a map<string,?>

        if (colMap.qualifierPrefix != null) {
          sb.append(filter(colMap.familyName)).append("_")
              .append(filter(colMap.qualifierPrefix) + i).append(StringUtils.COMMA_STR);
        } else {
          sb.append(filter(colMap.familyName)).append("_").append("col" + i)
              .append(StringUtils.COMMA_STR);
        }
      } else {
        // just an individual column
        sb.append(filter(colMap.familyName)).append("_").append(filter(colMap.qualifierName))
            .append(StringUtils.COMMA_STR);
      }
    }

    // trim off the ending ",", if any
    trim(sb);

    LOG.debug("Generated columns: [{}]", sb);
  }

  /**
   * Autogenerates the column types from the given serialization class
   * 
   * @param tbl the hive table properties
   * @param columnsMapping the hbase columns mapping determining hbase column families and
   *          qualifiers
   * @param sb StringBuilder to form the list of columns
   * @param conf configuration
   * @throws IllegalArgumentException if any of the given arguments was null
   * @throws SerDeException if there was an error generating the column types
   * */
  public static void generateColumnTypes(Properties tbl, List<ColumnMapping> columnsMapping,
      StringBuilder sb, Configuration conf) throws SerDeException {

    if (tbl == null) {
      throw new IllegalArgumentException("tbl cannot be null");
    }

    if (columnsMapping == null) {
      throw new IllegalArgumentException("columnsMapping cannot be null");
    }

    if (sb == null) {
      throw new IllegalArgumentException("StringBuilder cannot be null");
    }

    // Generate the columns according to the column mapping provided
    for (int i = 0; i < columnsMapping.size(); i++) {
      if (sb.length() > 0) {
        sb.append(":");
      }

      ColumnMapping colMap = columnsMapping.get(i);

      if (colMap.hbaseRowKey) {

        Map<String, String> compositeKeyParts = getCompositeKeyParts(tbl);
        StringBuilder keyStruct = new StringBuilder();

        if (compositeKeyParts == null || compositeKeyParts.isEmpty()) {
          String compKeyClass = tbl.getProperty(HBaseSerDe.HBASE_COMPOSITE_KEY_CLASS);
          String compKeyTypes = tbl.getProperty(HBaseSerDe.HBASE_COMPOSITE_KEY_TYPES);

          if (compKeyTypes == null) {

            if (compKeyClass != null) {
              // a composite key class was provided. But neither the types
              // property was set and
              // neither the getParts() method of HBaseCompositeKey was
              // overidden in the
              // implementation. Flag exception.
              throw new SerDeException(
                  "Either the hbase.composite.key.types property should be set or the getParts method must be overridden in "
                      + compKeyClass);
            }

            // the row key column becomes a STRING
            sb.append(serdeConstants.STRING_TYPE_NAME);
          } else {
            generateKeyStruct(compKeyTypes, keyStruct);
          }
        } else {
          generateKeyStruct(compositeKeyParts, keyStruct);
        }
        sb.append(keyStruct);
      } else if (colMap.qualifierName == null) {

        String serClassName = null;
        String serType = null;
        String schemaLiteral = null;
        String schemaUrl = null;

        if (colMap.qualifierPrefix != null) {

          serType =
              tbl.getProperty(colMap.familyName + "." + colMap.qualifierPrefix + "."
                  + HBaseSerDe.SERIALIZATION_TYPE);

          if (serType == null) {
            throw new SerDeException(HBaseSerDe.SERIALIZATION_TYPE
                + " property not provided for column family [" + colMap.familyName
                + "] and prefix [" + colMap.qualifierPrefix + "]");
          }

          // we are provided with a prefix
          serClassName =
              tbl.getProperty(colMap.familyName + "." + colMap.qualifierPrefix + "."
                  + serdeConstants.SERIALIZATION_CLASS);

          if (serClassName == null) {
            if (serType.equalsIgnoreCase(HBaseSerDeParameters.AVRO_SERIALIZATION_TYPE)) {
              // for avro type, the serialization class parameter is optional
              schemaLiteral =
                  tbl.getProperty(colMap.familyName + "." + colMap.qualifierPrefix + "."
                      + AvroTableProperties.SCHEMA_LITERAL.getPropName());
              schemaUrl =
                  tbl.getProperty(colMap.familyName + "." + colMap.qualifierPrefix + "."
                      + AvroTableProperties.SCHEMA_URL.getPropName());

              if (schemaLiteral == null && schemaUrl == null) {
                // either schema literal, schema url or serialization class must
                // be provided
                throw new SerDeException("For an avro schema, either "
                    + AvroTableProperties.SCHEMA_LITERAL.getPropName() + ", "
                        + AvroTableProperties.SCHEMA_URL.getPropName() + " or "
                    + serdeConstants.SERIALIZATION_CLASS + " property must be set.");
              }

              if (schemaUrl != null) {
                schemaLiteral = getSchemaFromFS(schemaUrl, conf).toString();
              }

            } else {
              throw new SerDeException(serdeConstants.SERIALIZATION_CLASS
                  + " property not provided for column family [" + colMap.familyName
                  + "] and prefix [" + colMap.qualifierPrefix + "]");
            }
          }
        } else {
          serType = tbl.getProperty(colMap.familyName + "." + HBaseSerDe.SERIALIZATION_TYPE);

          if (serType == null) {
            throw new SerDeException(HBaseSerDe.SERIALIZATION_TYPE
                + " property not provided for column family [" + colMap.familyName + "]");
          }

          serClassName =
              tbl.getProperty(colMap.familyName + "." + serdeConstants.SERIALIZATION_CLASS);

          if (serClassName == null) {

            if (serType.equalsIgnoreCase(AVRO_SERIALIZATION_TYPE)) {
              // for avro type, the serialization class parameter is optional
              schemaLiteral =
                  tbl.getProperty(colMap.familyName + "." + AvroTableProperties.SCHEMA_LITERAL.getPropName());
              schemaUrl = tbl.getProperty(colMap.familyName + "." + AvroTableProperties.SCHEMA_URL.getPropName());

              if (schemaLiteral == null && schemaUrl == null) {
                // either schema literal or serialization class must be provided
                throw new SerDeException("For an avro schema, either "
                    + AvroTableProperties.SCHEMA_LITERAL.getPropName() + " property or "
                    + serdeConstants.SERIALIZATION_CLASS + " property must be set.");
              }

              if (schemaUrl != null) {
                schemaLiteral = getSchemaFromFS(schemaUrl, conf).toString();
              }
            } else {
              throw new SerDeException(serdeConstants.SERIALIZATION_CLASS
                  + " property not provided for column family [" + colMap.familyName + "]");
            }
          }
        }

        StringBuilder generatedStruct = new StringBuilder();

        // generate struct for each of the given prefixes
        generateColumnStruct(serType, serClassName, schemaLiteral, colMap, generatedStruct);

        // a column family becomes a MAP
        sb.append(serdeConstants.MAP_TYPE_NAME + "<" + serdeConstants.STRING_TYPE_NAME + ","
            + generatedStruct + ">");

      } else {

        String qualifierName = colMap.qualifierName;

        if (colMap.qualifierName.endsWith("*")) {
          // we are provided with a prefix
          qualifierName = colMap.qualifierName.substring(0, colMap.qualifierName.length() - 1);
        }

        String serType =
            tbl.getProperty(colMap.familyName + "." + qualifierName + "."
                + HBaseSerDe.SERIALIZATION_TYPE);

        if (serType == null) {
          throw new SerDeException(HBaseSerDe.SERIALIZATION_TYPE
              + " property not provided for column family [" + colMap.familyName
              + "] and qualifier [" + qualifierName + "]");
        }

        String serClassName =
            tbl.getProperty(colMap.familyName + "." + qualifierName + "."
                + serdeConstants.SERIALIZATION_CLASS);

        String schemaLiteral = null;
        String schemaUrl = null;

        if (serClassName == null) {

          if (serType.equalsIgnoreCase(AVRO_SERIALIZATION_TYPE)) {
            // for avro type, the serialization class parameter is optional
            schemaLiteral =
                tbl.getProperty(colMap.familyName + "." + qualifierName + "."
                    + AvroTableProperties.SCHEMA_LITERAL.getPropName());
            schemaUrl =
                tbl.getProperty(colMap.familyName + "." + qualifierName + "."
                    + AvroTableProperties.SCHEMA_URL.getPropName());

            if (schemaLiteral == null && schemaUrl == null) {
              // either schema literal, schema url or serialization class must
              // be provided
              throw new SerDeException("For an avro schema, either "
                  + AvroTableProperties.SCHEMA_LITERAL.getPropName() + ", " + AvroTableProperties.SCHEMA_URL.getPropName() + " or "
                  + serdeConstants.SERIALIZATION_CLASS + " property must be set.");
            }

            if (schemaUrl != null) {
              schemaLiteral = getSchemaFromFS(schemaUrl, conf).toString();
            }
          } else {
            throw new SerDeException(serdeConstants.SERIALIZATION_CLASS
                + " property not provided for column family [" + colMap.familyName
                + "] and qualifier [" + qualifierName + "]");
          }
        }

        StringBuilder generatedStruct = new StringBuilder();

        generateColumnStruct(serType, serClassName, schemaLiteral, colMap, generatedStruct);

        sb.append(generatedStruct);
      }
    }

    // trim off ending ",", if any
    trim(sb);

    LOG.debug("Generated column types: [{}]", sb);
  }

  /**
   * Read the schema from the given hdfs url for the schema
   * */
  public static Schema getSchemaFromFS(String schemaFSUrl, Configuration conf)
      throws SerDeException {
    FSDataInputStream in = null;
    FileSystem fs = null;
    try {
      fs = FileSystem.get(new URI(schemaFSUrl), conf);
      in = fs.open(new Path(schemaFSUrl));
      Schema s = Schema.parse(in);
      return s;
    } catch (URISyntaxException e) {
      throw new SerDeException("Failure reading schema from filesystem", e);
    } catch (IOException e) {
      throw new SerDeException("Failure reading schema from filesystem", e);
    } finally {
      IOUtils.closeQuietly(in);
    }
  }

  /**
   * Create the {@link LazyObjectBase lazy field}
   * */
  public static LazyObjectBase createLazyField(ColumnMapping[] columnMappings, int fieldID,
      ObjectInspector inspector) {
    ColumnMapping colMap = columnMappings[fieldID];
    if (colMap.getQualifierName() == null && !colMap.isHbaseRowKey()) {
      // a column family
      return new LazyHBaseCellMap((LazyMapObjectInspector) inspector);
    }
    return LazyFactory.createLazyObject(inspector, colMap.getBinaryStorage().get(0));
  }

  /**
   * Auto-generates the key struct for composite keys
   * 
   * @param compositeKeyParts map of composite key part name to its type. Usually this would be
   *          provided by the custom implementation of {@link HBaseCompositeKey composite key}
   * @param sb StringBuilder object to construct the struct
   * */
  private static void generateKeyStruct(Map<String, String> compositeKeyParts, StringBuilder sb) {
    sb.append("struct<");

    for (Entry<String, String> entry : compositeKeyParts.entrySet()) {
      sb.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
    }

    // trim the trailing ","
    trim(sb);
    sb.append(">");
  }

  /**
   * Auto-generates the key struct for composite keys
   * 
   * @param compositeKeyTypes comma separated list of composite key types in order
   * @param sb StringBuilder object to construct the struct
   * */
  private static void generateKeyStruct(String compositeKeyTypes, StringBuilder sb) {
    sb.append("struct<");

    // composite key types is a comma separated list of different parts of the
    // composite keys in
    // order in which they appear in the key
    String[] keyTypes = compositeKeyTypes.split(",");

    for (int i = 0; i < keyTypes.length; i++) {
      sb.append("col" + i).append(":").append(keyTypes[i]).append(StringUtils.COMMA_STR);
    }

    // trim the trailing ","
    trim(sb);
    sb.append(">");
  }

  /**
   * Auto-generates the column struct
   * 
   * @param serType serialization type
   * @param serClassName serialization class name
   * @param schemaLiteral schema string
   * @param colMap hbase column mapping
   * @param sb StringBuilder to hold the generated struct
   * @throws SerDeException if something goes wrong while generating the struct
   * */
  private static void generateColumnStruct(String serType, String serClassName,
      String schemaLiteral, ColumnMapping colMap, StringBuilder sb) throws SerDeException {

    if (serType.equalsIgnoreCase(AVRO_SERIALIZATION_TYPE)) {

      if (serClassName != null) {
        generateAvroStructFromClass(serClassName, sb);
      } else {
        generateAvroStructFromSchema(schemaLiteral, sb);
      }
    } else {
      throw new SerDeException("Unknown " + HBaseSerDe.SERIALIZATION_TYPE
          + " found for column family [" + colMap.familyName + "]");
    }
  }

  /**
   * Auto-generate the avro struct from class
   * 
   * @param serClassName serialization class for avro struct
   * @param sb StringBuilder to hold the generated struct
   * @throws SerDeException if something goes wrong while generating the struct
   * */
  private static void generateAvroStructFromClass(String serClassName, StringBuilder sb)
      throws SerDeException {
    Class<?> serClass;
    try {
      serClass = JavaUtils.loadClass(serClassName);
    } catch (ClassNotFoundException e) {
      throw new SerDeException("Error obtaining descriptor for " + serClassName, e);
    }

    Schema schema = ReflectData.get().getSchema(serClass);

    generateAvroStructFromSchema(schema, sb);
  }

  /**
   * Auto-generate the avro struct from schema
   * 
   * @param schemaLiteral schema for the avro struct as string
   * @param sb StringBuilder to hold the generated struct
   * @throws SerDeException if something goes wrong while generating the struct
   * */
  private static void generateAvroStructFromSchema(String schemaLiteral, StringBuilder sb)
      throws SerDeException {
    Schema schema = Schema.parse(schemaLiteral);

    generateAvroStructFromSchema(schema, sb);
  }

  /**
   * Auto-generate the avro struct from schema
   * 
   * @param schema schema for the avro struct
   * @param sb StringBuilder to hold the generated struct
   * @throws SerDeException if something goes wrong while generating the struct
   * */
  private static void generateAvroStructFromSchema(Schema schema, StringBuilder sb)
      throws SerDeException {
    AvroObjectInspectorGenerator avig = new AvroObjectInspectorGenerator(schema);

    sb.append("struct<");

    // Get the column names and their corresponding types
    List<String> columnNames = avig.getColumnNames();
    List<TypeInfo> columnTypes = avig.getColumnTypes();

    if (columnNames.size() != columnTypes.size()) {
      throw new AssertionError("The number of column names should be the same as column types");
    }

    for (int i = 0; i < columnNames.size(); i++) {
      sb.append(columnNames.get(i));
      sb.append(":");
      sb.append(columnTypes.get(i).getTypeName());
      sb.append(",");
    }

    trim(sb).append(">");
  }

  /**
   * Trims by removing the trailing "," if any
   * 
   * @param sb StringBuilder to trim
   * @return StringBuilder trimmed StringBuilder
   * */
  private static StringBuilder trim(StringBuilder sb) {
    if (sb.charAt(sb.length() - 1) == StringUtils.COMMA) {
      return sb.deleteCharAt(sb.length() - 1);
    }

    return sb;
  }

  /**
   * Filters the given name by removing any special character and convert to lowercase
   * */
  private static String filter(String name) {
    return name.replaceAll("[^a-zA-Z0-9]+", "").toLowerCase();
  }

  /**
   * Return the types for the composite key.
   * 
   * @param tbl Properties for the table
   * @return a comma-separated list of composite key types
   * @throws SerDeException if something goes wrong while getting the composite key parts
   * */
  @SuppressWarnings("unchecked")
  private static Map<String, String> getCompositeKeyParts(Properties tbl) throws SerDeException {
    String compKeyClassName = tbl.getProperty(HBaseSerDe.HBASE_COMPOSITE_KEY_CLASS);

    if (compKeyClassName == null) {
      // no custom composite key class provided. return null
      return null;
    }

    CompositeHBaseKeyFactory<HBaseCompositeKey> keyFactory = null;

    Class<?> keyClass;
    try {
      keyClass = JavaUtils.loadClass(compKeyClassName);
      keyFactory = new CompositeHBaseKeyFactory(keyClass);
    } catch (Exception e) {
      throw new SerDeException(e);
    }

    HBaseCompositeKey compKey = keyFactory.createKey(null);
    return compKey.getParts();
  }
}
