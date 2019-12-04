/*
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

import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.convert.DataWritableRecordConverter;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriteSupport;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.FieldNode;
import org.apache.hadoop.hive.ql.optimizer.NestedColumnFieldPruningUtils;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;

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
  private TypeInfo hiveTypeInfo;
  /**
   * From a string which columns names (including hive column), return a list
   * of string columns
   *
   * @param columns comma separated list of columns
   * @return list with virtual columns removed
   */
  public static List<String> getColumnNames(final String columns) {
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
  public static List<TypeInfo> getColumnTypes(final String types) {
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

    ListIterator<String> columnIterator = colNames.listIterator();
    while (columnIterator.hasNext()) {
      TypeInfo colType = colTypes.get(columnIterator.nextIndex());
      String colName = columnIterator.next();

      Type fieldType = getFieldTypeIgnoreCase(schema, colName);
      if (fieldType == null) {
        schemaTypes.add(Types.optional(PrimitiveTypeName.BINARY).named(colName));
      } else {
        schemaTypes.add(getProjectedType(colType, fieldType));
      }
    }

    return schemaTypes;
  }

  private static Type getProjectedType(TypeInfo colType, Type fieldType) {
    switch (colType.getCategory()) {
      case STRUCT:
        List<Type> groupFields = getProjectedGroupFields(
          fieldType.asGroupType(),
          ((StructTypeInfo) colType).getAllStructFieldNames(),
          ((StructTypeInfo) colType).getAllStructFieldTypeInfos()
        );

        Type[] typesArray = groupFields.toArray(new Type[0]);
        return Types.buildGroup(fieldType.getRepetition())
          .addFields(typesArray)
          .named(fieldType.getName());
      case LIST:
        TypeInfo elemType = ((ListTypeInfo) colType).getListElementTypeInfo();
        if (elemType.getCategory() == ObjectInspector.Category.STRUCT) {
          Type subFieldType = fieldType.asGroupType().getType(0);
          if (!subFieldType.isPrimitive()) {
            String subFieldName = subFieldType.getName();
            Text name = new Text(subFieldName);
            if (name.equals(ParquetHiveSerDe.ARRAY) || name.equals(ParquetHiveSerDe.LIST)) {
              subFieldType = new GroupType(Repetition.REPEATED, subFieldName,
                getProjectedType(elemType, subFieldType.asGroupType().getType(0)));
            } else {
              subFieldType = getProjectedType(elemType, subFieldType);
            }
            return Types.buildGroup(Repetition.OPTIONAL).as(OriginalType.LIST).addFields(
              subFieldType).named(fieldType.getName());
          }
        }
        break;
      default:
    }
    return fieldType;
  }

  /**
   * Searches column names by name on a given Parquet message schema, and returns its projected
   * Parquet schema types.
   *
   * @param schema Message type schema where to search for column names.
   * @param colNames List of column names.
   * @param colTypes List of column types.
   * @return A MessageType object of projected columns.
   */
  public static MessageType getSchemaByName(MessageType schema, List<String> colNames, List<TypeInfo> colTypes) {
    List<Type> projectedFields = getProjectedGroupFields(schema, colNames, colTypes);
    Type[] typesArray = projectedFields.toArray(new Type[0]);

    return Types.buildMessage()
        .addFields(typesArray)
        .named(schema.getName());
  }

  /**
   * Searches column names by indexes on a given Parquet file schema, and returns its corresponded
   * Parquet schema types.
   *
   * @param schema Message schema where to search for column names.
   * @param colNames List of column names.
   * @param colIndexes List of column indexes.
   * @return A MessageType object of the column names found.
   */
  public static MessageType getSchemaByIndex(MessageType schema, List<String> colNames, List<Integer> colIndexes) {
    List<Type> schemaTypes = new ArrayList<Type>();

    for (Integer i : colIndexes) {
      if (i < colNames.size()) {
        if (i < schema.getFieldCount()) {
          schemaTypes.add(schema.getType(i));
        } else {
          //prefixing with '_mask_' to ensure no conflict with named
          //columns in the file schema
          schemaTypes.add(
            Types.optional(PrimitiveTypeName.BINARY).named("_mask_" + colNames.get(i)));
        }
      }
    }

    return new MessageType(schema.getName(), schemaTypes);
  }

  /**
   * Generate the projected schema from colIndexes and nested column paths. If the column is
   * contained by colIndex, it will be added directly, otherwise it will build a group type which
   * contains all required sub types using nestedColumnPaths.
   * @param schema original schema
   * @param colNames
   * @param colIndexes the index of needed columns
   * @param nestedColumnPaths the paths for nested columns
   * @return
   */
  public static MessageType getProjectedSchema(
    MessageType schema,
    List<String> colNames,
    List<Integer> colIndexes,
    Set<String> nestedColumnPaths) {
    List<Type> schemaTypes = new ArrayList<Type>();

    Map<String, FieldNode> prunedCols = getPrunedNestedColumns(nestedColumnPaths);
    for (Integer i : colIndexes) {
      if (i < colNames.size()) {
        if (i < schema.getFieldCount()) {
          Type t = schema.getType(i);
          String tn = t.getName().toLowerCase();
          if (!prunedCols.containsKey(tn)) {
            schemaTypes.add(schema.getType(i));
          } else {
            if (t.isPrimitive()) {
              // For primitive type, add directly.
              schemaTypes.add(t);
            } else {
              // For group type, we need to build the projected group type with required leaves
              List<Type> g =
                projectLeafTypes(Arrays.asList(t), Arrays.asList(prunedCols.get(tn)));
              if (!g.isEmpty()) {
                schemaTypes.addAll(g);
              }
            }
          }
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
   * Get a valid zoneId from some metadata, otherwise return null.
   */
  public static ZoneId getWriterTimeZoneId(Map<String, String> metadata) {
    if (metadata == null) {
      return null;
    }
    String value = metadata.get(DataWritableWriteSupport.WRITER_TIMEZONE);
    try {
      if (value != null) {
        return ZoneId.of(value);
      }
    } catch (DateTimeException e) {
      throw new RuntimeException("Can't parse writer time zone stored in file metadata", e);
    }

    return null;
  }

  /**
   * Get the proleptic from some metadata, otherwise return null.
   */
  public static Boolean getWriterDateProleptic(Map<String, String> metadata) {
    if (metadata == null) {
      return null;
    }
    String value = metadata.get(DataWritableWriteSupport.WRITER_DATE_PROLEPTIC);
    try {
      if (value != null) {
        return Boolean.valueOf(value);
      }
    } catch (DateTimeException e) {
      throw new RuntimeException("Can't parse writer proleptic property stored in file metadata", e);
    }

    return null;
  }

  /**
   * Return the columns which contains required nested attribute level
   * E.g., given struct a:<x:int, y:int> while 'x' is required and 'y' is not, the method will return
   * a pruned struct for 'a' which only contains the attribute 'x'
   *
   * @param nestedColPaths the paths for required nested attribute
   * @return a map from the column to its selected nested column paths, of which the keys are all lower-cased.
   */
  private static Map<String, FieldNode> getPrunedNestedColumns(Set<String> nestedColPaths) {
    Map<String, FieldNode> resMap = new HashMap<>();
    if (nestedColPaths.isEmpty()) {
      return resMap;
    }
    for (String s : nestedColPaths) {
      String c = StringUtils.split(s, '.')[0].toLowerCase();
      if (!resMap.containsKey(c)) {
        FieldNode f = NestedColumnFieldPruningUtils.addNodeByPath(null, s);
        resMap.put(c, f);
      } else {
        resMap.put(c, NestedColumnFieldPruningUtils.addNodeByPath(resMap.get(c), s));
      }
    }
    return resMap;
  }

  private static GroupType buildProjectedGroupType(
    GroupType originalType,
    List<Type> types) {
    if (types == null || types.isEmpty()) {
      return null;
    }
    return new GroupType(originalType.getRepetition(), originalType.getName(), types);
  }

  private static List<Type> projectLeafTypes(
    List<Type> types,
    List<FieldNode> nodes) {
    List<Type> res = new ArrayList<>();
    if (nodes.isEmpty()) {
      return res;
    }
    Map<String, FieldNode> fieldMap = new HashMap<>();
    for (FieldNode n : nodes) {
      fieldMap.put(n.getFieldName().toLowerCase(), n);
    }
    for (Type type : types) {
      String tn = type.getName().toLowerCase();

      if (fieldMap.containsKey(tn)) {
        FieldNode f = fieldMap.get(tn);
        if (f.getNodes().isEmpty()) {
          // no child, no need for pruning
          res.add(type);
        } else {
          if (type instanceof GroupType) {
            GroupType groupType = type.asGroupType();
            List<Type> ts = projectLeafTypes(groupType.getFields(), f.getNodes());
            GroupType g = buildProjectedGroupType(groupType, ts);
            if (g != null) {
              res.add(g);
            }
          } else {
            throw new RuntimeException(
              "Primitive type " + f.getFieldName() + "should not " + "doesn't match type" + f
                .toString());
          }
        }
      }
    }
    return res;
  }

  /**
   * It creates the readContext for Parquet side with the requested schema during the init phase.
   *
   * @param context
   * @return the parquet ReadContext
   */
  @Override
  public org.apache.parquet.hadoop.api.ReadSupport.ReadContext init(InitContext context) {
    Configuration configuration = context.getConfiguration();
    MessageType fileSchema = context.getFileSchema();
    String columnNames = configuration.get(IOConstants.COLUMNS);
    Map<String, String> contextMetadata = new HashMap<String, String>();
    boolean indexAccess = configuration.getBoolean(PARQUET_COLUMN_INDEX_ACCESS, false);

    if (columnNames != null) {
      List<String> columnNamesList = getColumnNames(columnNames);
      String columnTypes = configuration.get(IOConstants.COLUMNS_TYPES);
      List<TypeInfo> columnTypesList = getColumnTypes(columnTypes);

      MessageType tableSchema =
        getRequestedSchemaForIndexAccess(indexAccess, columnNamesList, columnTypesList, fileSchema);

      contextMetadata.put(HIVE_TABLE_AS_PARQUET_SCHEMA, tableSchema.toString());
      contextMetadata.put(PARQUET_COLUMN_INDEX_ACCESS, String.valueOf(indexAccess));
      this.hiveTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNamesList, columnTypesList);

      return new ReadContext(getRequestedPrunedSchema(columnNamesList, tableSchema, configuration),
        contextMetadata);
    } else {
      contextMetadata.put(HIVE_TABLE_AS_PARQUET_SCHEMA, fileSchema.toString());
      return new ReadContext(fileSchema, contextMetadata);
    }
  }

  /**
   * It's used for vectorized code path.
   * @param indexAccess
   * @param columnNamesList
   * @param columnTypesList
   * @param fileSchema
   * @param configuration
   * @return
   */
  public static MessageType getRequestedSchema(
    boolean indexAccess,
    List<String> columnNamesList,
    List<TypeInfo> columnTypesList,
    MessageType fileSchema,
    Configuration configuration) {
    MessageType tableSchema =
      getRequestedSchemaForIndexAccess(indexAccess, columnNamesList, columnTypesList, fileSchema);

    List<Integer> indexColumnsWanted = ColumnProjectionUtils.getReadColumnIDs(configuration);
    //TODO Duplicated code for init method since vectorization reader path doesn't support Nested
    // column pruning so far. See HIVE-15156
    if (!ColumnProjectionUtils.isReadAllColumns(configuration) && !indexColumnsWanted.isEmpty()) {
      return DataWritableReadSupport
        .getSchemaByIndex(tableSchema, columnNamesList, indexColumnsWanted);
    } else {
      return tableSchema;
    }
  }

  private static MessageType getRequestedSchemaForIndexAccess(
    boolean indexAccess,
    List<String> columnNamesList,
    List<TypeInfo> columnTypesList,
    MessageType fileSchema) {
    if (indexAccess) {
      List<Integer> indexSequence = new ArrayList<Integer>();

      // Generates a sequence list of indexes
      for (int i = 0; i < columnNamesList.size(); i++) {
        indexSequence.add(i);
      }

      return getSchemaByIndex(fileSchema, columnNamesList, indexSequence);
    } else {
      return getSchemaByName(fileSchema, columnNamesList, columnTypesList);
    }
  }

  private static MessageType getRequestedPrunedSchema(
    List<String> columnNamesList,
    MessageType fileSchema,
    Configuration configuration) {
    Set<String> groupPaths = ColumnProjectionUtils.getNestedColumnPaths(configuration);
    List<Integer> indexColumnsWanted = ColumnProjectionUtils.getReadColumnIDs(configuration);
    if (!ColumnProjectionUtils.isReadAllColumns(configuration) && !indexColumnsWanted.isEmpty()) {
      return getProjectedSchema(fileSchema, columnNamesList, indexColumnsWanted, groupPaths);
    } else {
      return fileSchema;
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
          final org.apache.parquet.hadoop.api.ReadSupport.ReadContext readContext) {
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

    String writerTimezone = DataWritableWriteSupport.WRITER_TIMEZONE;
    if (!metadata.containsKey(writerTimezone)) {
      if (keyValueMetaData.containsKey(writerTimezone)) {
        metadata.put(writerTimezone, keyValueMetaData.get(writerTimezone));
      }
    } else if (!metadata.get(writerTimezone).equals(keyValueMetaData.get(writerTimezone))) {
      throw new IllegalStateException("Metadata contains a writer time zone that does not match "
          + "file footer's writer time zone.");
    }

    String writerProleptic = DataWritableWriteSupport.WRITER_DATE_PROLEPTIC;
    if (!metadata.containsKey(writerProleptic)) {
      if (keyValueMetaData.containsKey(writerProleptic)) {
        metadata.put(writerProleptic, keyValueMetaData.get(writerProleptic));
      }
    } else if (!metadata.get(writerProleptic).equals(keyValueMetaData.get(writerProleptic))) {
      throw new IllegalStateException("Metadata contains a writer proleptic property value that does not match "
          + "file footer's value.");
    }

    String prolepticDefault = ConfVars.HIVE_PARQUET_DATE_PROLEPTIC_GREGORIAN_DEFAULT.varname;
    if (!metadata.containsKey(prolepticDefault)) {
      metadata.put(prolepticDefault, String.valueOf(HiveConf.getBoolVar(
          configuration, HiveConf.ConfVars.HIVE_PARQUET_DATE_PROLEPTIC_GREGORIAN_DEFAULT)));
    }

    return new DataWritableRecordConverter(readContext.getRequestedSchema(), metadata, hiveTypeInfo);
  }
}
