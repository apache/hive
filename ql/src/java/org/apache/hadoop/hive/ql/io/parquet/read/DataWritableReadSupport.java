/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet.read;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.convert.DataWritableRecordConverter;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.util.StringUtils;

import parquet.hadoop.api.ReadSupport;
import parquet.io.api.RecordMaterializer;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.Type;
import parquet.schema.Types;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

/**
 *
 * A MapWritableReadSupport
 *
 * Manages the translation between Hive and Parquet
 *
 */
public class DataWritableReadSupport extends ReadSupport<ArrayWritable> {

  public static final String HIVE_TABLE_AS_PARQUET_SCHEMA = "HIVE_TABLE_SCHEMA";
  public static final String PARQUET_COLUMN_INDEX_ACCESS = "parquet.column.index.access";

  /**
   * From a string which columns names (including hive column), return a list
   * of string columns
   *
   * @param columns comma separated list of columns
   * @return list with virtual columns removed
   */
  private static List<String> getColumnNames(final String columns) {
    return (List<String>) VirtualColumn.
        removeVirtualColumns(StringUtils.getStringCollection(columns));
  }

  /**
   * Returns a list of TypeInfo objects from a string which contains column
   * types strings.
   *
   * @param types Comma separated list of types
   * @return A list of TypeInfo objects.
   */
  private static List<TypeInfo> getColumnTypes(final String types) {
    return TypeInfoUtils.getTypeInfosFromTypeString(types);
  }

  /**
   * Searchs for a fieldName into a parquet GroupType by ignoring string case.
   * GroupType#getType(String fieldName) is case sensitive, so we use this method.
   *
   * @param groupType Group of field types where to search for fieldName
   * @param fieldName The field what we are searching
   * @return The Type object of the field found; null otherwise.
   */
  private static Type getFieldTypeIgnoreCase(GroupType groupType, String fieldName) {
    for (Type type : groupType.getFields()) {
      if (type.getName().equalsIgnoreCase(fieldName)) {
        return type;
      }
    }

    return null;
  }

  /**
   * Searchs column names by name on a given Parquet schema, and returns its corresponded
   * Parquet schema types.
   *
   * @param schema Group schema where to search for column names.
   * @param colNames List of column names.
   * @param colTypes List of column types.
   * @return List of GroupType objects of projected columns.
   */
  private static List<Type> getProjectedGroupFields(GroupType schema, List<String> colNames, List<TypeInfo> colTypes) {
    List<Type> schemaTypes = new ArrayList<Type>();

    ListIterator columnIterator = colNames.listIterator();
    while (columnIterator.hasNext()) {
      TypeInfo colType = colTypes.get(columnIterator.nextIndex());
      String colName = (String) columnIterator.next();

      Type fieldType = getFieldTypeIgnoreCase(schema, colName);
      if (fieldType != null) {
        if (colType.getCategory() == ObjectInspector.Category.STRUCT) {
          if (fieldType.isPrimitive()) {
            throw new IllegalStateException("Invalid schema data type, found: PRIMITIVE, expected: STRUCT");
          }

          GroupType groupFieldType = fieldType.asGroupType();

          List<Type> groupFields = getProjectedGroupFields(
              groupFieldType,
              ((StructTypeInfo) colType).getAllStructFieldNames(),
              ((StructTypeInfo) colType).getAllStructFieldTypeInfos()
          );

          Type[] typesArray = groupFields.toArray(new Type[0]);
          schemaTypes.add(Types.buildGroup(groupFieldType.getRepetition())
              .addFields(typesArray)
              .named(fieldType.getName())
          );
        } else {
          schemaTypes.add(fieldType);
        }
      } else {
        // Add type for schema evolution
        schemaTypes.add(Types.optional(PrimitiveTypeName.BINARY).named(colName));
      }
    }

    return schemaTypes;
  }

  /**
   * Searchs column names by name on a given Parquet message schema, and returns its projected
   * Parquet schema types.
   *
   * @param schema Message type schema where to search for column names.
   * @param colNames List of column names.
   * @param colTypes List of column types.
   * @return A MessageType object of projected columns.
   */
  private static MessageType getSchemaByName(MessageType schema, List<String> colNames, List<TypeInfo> colTypes) {
    List<Type> projectedFields = getProjectedGroupFields(schema, colNames, colTypes);
    Type[] typesArray = projectedFields.toArray(new Type[0]);

    return Types.buildMessage()
        .addFields(typesArray)
        .named(schema.getName());
  }

  /**
   * Searchs column names by index on a given Parquet file schema, and returns its corresponded
   * Parquet schema types.
   *
   * @param schema Message schema where to search for column names.
   * @param colNames List of column names.
   * @param colIndexes List of column indexes.
   * @return A MessageType object of the column names found.
   */
  private static MessageType getSchemaByIndex(MessageType schema, List<String> colNames, List<Integer> colIndexes) {
    List<Type> schemaTypes = new ArrayList<Type>();

    for (Integer i : colIndexes) {
      if (i < colNames.size()) {
        if (i < schema.getFieldCount()) {
          schemaTypes.add(schema.getType(i));
        } else {
          //prefixing with '_mask_' to ensure no conflict with named
          //columns in the file schema
          schemaTypes.add(Types.optional(PrimitiveTypeName.BINARY).named("_mask_" + colNames.get(i)));
        }
      }
    }

    return new MessageType(schema.getName(), schemaTypes);
  }

  /**
   * It creates the readContext for Parquet side with the requested schema during the init phase.
   *
   * @param configuration    needed to get the wanted columns
   * @param keyValueMetaData // unused
   * @param fileSchema       parquet file schema
   * @return the parquet ReadContext
   */
  @Override
  public ReadContext init(final Configuration configuration, final Map<String, String> keyValueMetaData, final MessageType fileSchema) {
    String columnNames = configuration.get(IOConstants.COLUMNS);
    Map<String, String> contextMetadata = new HashMap<String, String>();
    boolean indexAccess = configuration.getBoolean(PARQUET_COLUMN_INDEX_ACCESS, false);

    if (columnNames != null) {
      List<String> columnNamesList = getColumnNames(columnNames);

      MessageType tableSchema;
      if (indexAccess) {
        List<Integer> indexSequence = new ArrayList<Integer>();

        // Generates a sequence list of indexes
        for(int i = 0; i < columnNamesList.size(); i++) {
          indexSequence.add(i);
        }

        tableSchema = getSchemaByIndex(fileSchema, columnNamesList, indexSequence);
      } else {
        String columnTypes = configuration.get(IOConstants.COLUMNS_TYPES);
        List<TypeInfo> columnTypesList = getColumnTypes(columnTypes);

        tableSchema = getSchemaByName(fileSchema, columnNamesList, columnTypesList);
      }

      contextMetadata.put(HIVE_TABLE_AS_PARQUET_SCHEMA, tableSchema.toString());

      List<Integer> indexColumnsWanted = ColumnProjectionUtils.getReadColumnIDs(configuration);
      MessageType requestedSchemaByUser = getSchemaByIndex(tableSchema, columnNamesList, indexColumnsWanted);

      return new ReadContext(requestedSchemaByUser, contextMetadata);
    } else {
      contextMetadata.put(HIVE_TABLE_AS_PARQUET_SCHEMA, fileSchema.toString());
      return new ReadContext(fileSchema, contextMetadata);
    }
  }

  /**
   *
   * It creates the hive read support to interpret data from parquet to hive
   *
   * @param configuration // unused
   * @param keyValueMetaData
   * @param fileSchema // unused
   * @param readContext containing the requested schema and the schema of the hive table
   * @return Record Materialize for Hive
   */
  @Override
  public RecordMaterializer<ArrayWritable> prepareForRead(final Configuration configuration,
      final Map<String, String> keyValueMetaData, final MessageType fileSchema,
          final parquet.hadoop.api.ReadSupport.ReadContext readContext) {
    final Map<String, String> metadata = readContext.getReadSupportMetadata();
    if (metadata == null) {
      throw new IllegalStateException("ReadContext not initialized properly. " +
        "Don't know the Hive Schema.");
    }
    String key = HiveConf.ConfVars.HIVE_PARQUET_TIMESTAMP_SKIP_CONVERSION.varname;
    if (!metadata.containsKey(key)) {
      metadata.put(key, String.valueOf(HiveConf.getBoolVar(
        configuration, HiveConf.ConfVars.HIVE_PARQUET_TIMESTAMP_SKIP_CONVERSION)));
    }
    return new DataWritableRecordConverter(readContext.getRequestedSchema(), metadata);
  }
}
