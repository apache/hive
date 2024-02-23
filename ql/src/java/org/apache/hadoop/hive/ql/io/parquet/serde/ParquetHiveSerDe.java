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
package org.apache.hadoop.hive.ql.io.parquet.serde;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.FieldNode;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SchemaInference;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.io.ParquetHiveRecord;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.ListLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.MapLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A ParquetHiveSerDe for Hive (with the deprecated package mapred). Parquet
 * format and stats is collected in ParquetRecordWriterWrapper when writer gets
 * closed.
 */
@SerDeSpec(schemaProps = {serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES,
        ParquetOutputFormat.COMPRESSION})
public class ParquetHiveSerDe extends AbstractSerDe implements SchemaInference {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetHiveSerDe.class);

  public static final Text MAP_KEY = new Text("key");
  public static final Text MAP_VALUE = new Text("value");
  public static final Text MAP = new Text("map");
  public static final Text ARRAY = new Text("bag");
  public static final Text LIST = new Text("list");

  // Map precision to the number bytes needed for binary conversion.
  public static final int PRECISION_TO_BYTE_COUNT[] = new int[38];

  static {
    for (int prec = 1; prec <= 38; prec++) {
      // Estimated number of bytes needed.
      PRECISION_TO_BYTE_COUNT[prec - 1] = (int)
          Math.ceil((Math.log(Math.pow(10, prec) - 1) / Math.log(2) + 1) / 8);
    }
  }

  private ObjectInspector objInspector;
  private ParquetHiveRecord parquetRow;

  public ParquetHiveSerDe() {
    parquetRow = new ParquetHiveRecord();
  }

  @Override
  public void initialize(Configuration configuration, Properties tableProperties, Properties partitionProperties)
      throws SerDeException {
    super.initialize(configuration, tableProperties, partitionProperties);

    // Create row related objects
    StructTypeInfo completeTypeInfo =
        (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(getColumnNames(), getColumnTypes());
    StructTypeInfo prunedTypeInfo = null;
    if (this.configuration.isPresent()) {
      Configuration conf = this.configuration.get();
      String rawPrunedColumnPaths = conf.get(ColumnProjectionUtils.READ_NESTED_COLUMN_PATH_CONF_STR);
      if (rawPrunedColumnPaths != null) {
        List<String> prunedColumnPaths = processRawPrunedPaths(rawPrunedColumnPaths);
        prunedTypeInfo = pruneFromPaths(completeTypeInfo, prunedColumnPaths);
      }
    }
    this.objInspector = new ArrayWritableObjectInspector(completeTypeInfo, prunedTypeInfo);
  }

  @Override
  public Object deserialize(final Writable blob) throws SerDeException {
    if (blob instanceof ArrayWritable) {
      return blob;
    } else {
      return null;
    }
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return objInspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return ParquetHiveRecord.class;
  }

  @Override
  public Writable serialize(final Object obj, final ObjectInspector objInspector)
      throws SerDeException {
    if (!objInspector.getCategory().equals(Category.STRUCT)) {
      throw new SerDeException("Cannot serialize " + objInspector.getCategory() + ". Can only serialize a struct");
    }

    parquetRow.value = obj;
    parquetRow.inspector= (StructObjectInspector)objInspector;
    return parquetRow;
  }

  /**
   * @param table
   * @return true if the table has the parquet serde defined
   */
  public static boolean isParquetTable(Table table) {
    return  table == null ? false : ParquetHiveSerDe.class.getName().equals(table.getSerializationLib());
  }

  /**
   * Given a list of raw pruned paths separated by ',', return a list of merged pruned paths.
   * For instance, if the 'prunedPaths' is "s.a, s, s", this returns ["s"].
   */
  private static List<String> processRawPrunedPaths(String prunedPaths) {
    List<FieldNode> fieldNodes = new ArrayList<>();
    for (String p : prunedPaths.split(",")) {
      fieldNodes = FieldNode.mergeFieldNodes(fieldNodes, FieldNode.fromPath(p));
    }
    List<String> prunedPathList = new ArrayList<>();
    for (FieldNode fn : fieldNodes) {
      prunedPathList.addAll(fn.toPaths());
    }
    return prunedPathList;
  }

  /**
   * Given a complete struct type info and pruned paths containing selected fields
   * from the type info, return a pruned struct type info only with the selected fields.
   *
   * For instance, if 'originalTypeInfo' is: s:struct<a:struct<b:int, c:boolean>, d:string>
   *   and 'prunedPaths' is ["s.a.b,s.d"], then the result will be:
   *   s:struct<a:struct<b:int>, d:string>
   *
   * @param originalTypeInfo the complete struct type info
   * @param prunedPaths a string representing the pruned paths, separated by ','
   * @return the pruned struct type info
   */
  private static StructTypeInfo pruneFromPaths(
      StructTypeInfo originalTypeInfo, List<String> prunedPaths) {
    PrunedStructTypeInfo prunedTypeInfo = new PrunedStructTypeInfo(originalTypeInfo);
    for (String path : prunedPaths) {
      pruneFromSinglePath(prunedTypeInfo, path);
    }
    return prunedTypeInfo.prune();
  }

  private static void pruneFromSinglePath(PrunedStructTypeInfo prunedInfo, String path) {
    Preconditions.checkArgument(prunedInfo != null,
      "PrunedStructTypeInfo for path '" + path + "' should not be null");

    int index = path.indexOf('.');
    if (index < 0) {
      index = path.length();
    }

    String fieldName = path.substring(0, index);
    prunedInfo.markSelected(fieldName);
    if (index < path.length()) {
      pruneFromSinglePath(prunedInfo.getChild(fieldName), path.substring(index + 1));
    }
  }

  private static class PrunedStructTypeInfo {
    final StructTypeInfo typeInfo;
    final Map<String, PrunedStructTypeInfo> children;
    final boolean[] selected;

    PrunedStructTypeInfo(StructTypeInfo typeInfo) {
      this.typeInfo = typeInfo;
      this.children = new HashMap<>();
      this.selected = new boolean[typeInfo.getAllStructFieldTypeInfos().size()];
      for (int i = 0; i < typeInfo.getAllStructFieldTypeInfos().size(); ++i) {
        TypeInfo ti = typeInfo.getAllStructFieldTypeInfos().get(i);
        if (ti.getCategory() == Category.STRUCT) {
          this.children.put(typeInfo.getAllStructFieldNames().get(i).toLowerCase(),
              new PrunedStructTypeInfo((StructTypeInfo) ti));
        }
      }
    }

    PrunedStructTypeInfo getChild(String fieldName) {
      return children.get(fieldName.toLowerCase());
    }

    void markSelected(String fieldName) {
      for (int i = 0; i < typeInfo.getAllStructFieldNames().size(); ++i) {
        if (typeInfo.getAllStructFieldNames().get(i).equalsIgnoreCase(fieldName)) {
          selected[i] = true;
          break;
        }
      }
    }

    StructTypeInfo prune() {
      List<String> newNames = new ArrayList<>();
      List<TypeInfo> newTypes = new ArrayList<>();
      List<String> oldNames = typeInfo.getAllStructFieldNames();
      List<TypeInfo> oldTypes = typeInfo.getAllStructFieldTypeInfos();
      for (int i = 0; i < oldNames.size(); ++i) {
        String fn = oldNames.get(i);
        if (selected[i]) {
          newNames.add(fn);
          if (children.containsKey(fn.toLowerCase())) {
            newTypes.add(children.get(fn.toLowerCase()).prune());
          } else {
            newTypes.add(oldTypes.get(i));
          }
        }
      }
      return (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(newNames, newTypes);
    }
  }

  // ReadSchema interface implementation
  private String convertGroupType(GroupType group, boolean inferBinaryAsString) throws SerDeException {
    boolean first = true;
    StringBuilder sb = new StringBuilder(serdeConstants.STRUCT_TYPE_NAME + "<");
    for (Type field: group.getFields()) {
      if (first) {
        first = false;
      } else {
        sb.append(",");
      }
      // fieldName:typeName
      sb.append(field.getName()).append(":").append(convertParquetTypeToFieldType(field, inferBinaryAsString));
    }
    sb.append(">");
    // struct<fieldName1:int, fieldName2:map<string : int>, etc
    return sb.toString();
  }

  private String convertPrimitiveType(PrimitiveType primitive, boolean inferBinaryAsString) throws SerDeException {
    switch (primitive.getPrimitiveTypeName()) {
      case INT96:
        return serdeConstants.TIMESTAMP_TYPE_NAME;
      case INT32:
        return serdeConstants.INT_TYPE_NAME;
      case INT64:
        return serdeConstants.BIGINT_TYPE_NAME;
      case BOOLEAN:
        return serdeConstants.BOOLEAN_TYPE_NAME;
      case FLOAT:
        return serdeConstants.FLOAT_TYPE_NAME;
      case DOUBLE:
        return serdeConstants.DOUBLE_TYPE_NAME;
      case BINARY:
        if (inferBinaryAsString) {
          return serdeConstants.STRING_TYPE_NAME;
        } else {
          return serdeConstants.BINARY_TYPE_NAME;
        }
      default:
        throw new SerDeException(ErrorMsg.PARQUET_UNHANDLED_TYPE.getErrorCodedMsg(primitive.getPrimitiveTypeName().name()));
    }
  }

  private String convertParquetIntLogicalType(Type parquetType) throws SerDeException {
    IntLogicalTypeAnnotation intLogicalType = (IntLogicalTypeAnnotation) parquetType.getLogicalTypeAnnotation();
    PrimitiveType primitiveType = parquetType.asPrimitiveType();
    // check to see if primitive type handling is implemented
    switch (primitiveType.getPrimitiveTypeName()) {
      case INT32:
      case INT64:
      break;
      default:
      throw new SerDeException(ErrorMsg.PARQUET_UNHANDLED_TYPE.getErrorCodedMsg(intLogicalType.toString()));
    }

    if (!intLogicalType.isSigned()) {
      // signed types are not supported
      throw new SerDeException(ErrorMsg.PARQUET_UNHANDLED_TYPE.getErrorCodedMsg(intLogicalType.toString()));
    }

    switch (intLogicalType.getBitWidth()) {
      case 8: return serdeConstants.TINYINT_TYPE_NAME;
      case 16: return serdeConstants.SMALLINT_TYPE_NAME;
      case 32: return serdeConstants.INT_TYPE_NAME;
      case 64: return serdeConstants.BIGINT_TYPE_NAME;
    }

    throw new SerDeException(ErrorMsg.PARQUET_UNHANDLED_TYPE.getErrorCodedMsg(intLogicalType.toString()));
  }

  private String createMapType(String keyType, String valueType) {
    // examples: map<string, int>, map<string : struct<i : int>>
    return serdeConstants.MAP_TYPE_NAME + "<" + keyType + "," + valueType + ">";
  }

  private String convertParquetMapLogicalTypeAnnotation(Type parquetType, boolean inferBinaryAsString)
          throws SerDeException {
    MapLogicalTypeAnnotation mType = (MapLogicalTypeAnnotation) parquetType.getLogicalTypeAnnotation();
    GroupType gType = parquetType.asGroupType();
    Type innerField = gType.getType(0);
    GroupType innerGroup = innerField.asGroupType();
    Type key = innerGroup.getType(0);
    Type value = innerGroup.getType(1);
    return createMapType(convertParquetTypeToFieldType(key, inferBinaryAsString),
            convertParquetTypeToFieldType(value, inferBinaryAsString));
  }

  private String createArrayType(String fieldType) {
    // examples: array<int>, array<struct<i:int>>, array<map<string : int>>
    return serdeConstants.LIST_TYPE_NAME + "<" + fieldType + ">";
  }

  private String convertParquetListLogicalTypeAnnotation(Type parquetType, boolean inferBinaryAsString)
          throws SerDeException {
    ListLogicalTypeAnnotation mType = (ListLogicalTypeAnnotation) parquetType.getLogicalTypeAnnotation();
    GroupType gType = parquetType.asGroupType();
    Type innerField = gType.getType(0);
    if (innerField.isPrimitive() || innerField.getOriginalType() != null) {
      return createArrayType(convertParquetTypeToFieldType(innerField, inferBinaryAsString));
    }

    GroupType innerGroup = innerField.asGroupType();
    if (innerGroup.getFieldCount() != 1) {
      return createArrayType(convertGroupType(innerGroup, inferBinaryAsString));
    }

    return createArrayType(convertParquetTypeToFieldType(innerGroup.getType(0), inferBinaryAsString));
  }

  private String createDecimalType(int precision, int scale) {
    // example: decimal(10, 4)
    return serdeConstants.DECIMAL_TYPE_NAME + "(" + precision + "," + scale + ")";
  }

  private String convertLogicalType(Type type, boolean inferBinaryAsString) throws SerDeException {
    LogicalTypeAnnotation lType = type.getLogicalTypeAnnotation();
    if (lType instanceof IntLogicalTypeAnnotation) {
      return convertParquetIntLogicalType(type);
    } else if (lType instanceof StringLogicalTypeAnnotation) {
      return serdeConstants.STRING_TYPE_NAME;
    } else if (lType instanceof DecimalLogicalTypeAnnotation) {
      DecimalLogicalTypeAnnotation dType = (DecimalLogicalTypeAnnotation) lType;
      return createDecimalType(dType.getPrecision(), dType.getScale());
    } else if (lType instanceof MapLogicalTypeAnnotation) {
      return convertParquetMapLogicalTypeAnnotation(type, inferBinaryAsString);
    } else if (lType instanceof ListLogicalTypeAnnotation) {
      return convertParquetListLogicalTypeAnnotation(type, inferBinaryAsString);
    } else if (lType instanceof DateLogicalTypeAnnotation) {
      // assuming 32 bit int
      return serdeConstants.DATE_TYPE_NAME;
    }
    throw new SerDeException(ErrorMsg.PARQUET_UNHANDLED_TYPE.getErrorCodedMsg(lType.toString()));
  }

  private String convertParquetTypeToFieldType(Type type, boolean inferBinaryAsString) throws SerDeException {
    if (type.getLogicalTypeAnnotation() != null) {
      return convertLogicalType(type, inferBinaryAsString);
    } else if (type.isPrimitive()) {
      return convertPrimitiveType(type.asPrimitiveType(), inferBinaryAsString);
    }
    return convertGroupType(type.asGroupType(), inferBinaryAsString);
  }

  private FieldSchema convertParquetTypeToFieldSchema(Type type, boolean inferBinaryAsString) throws SerDeException {
    String columnName = type.getName();
    String typeName = convertParquetTypeToFieldType(type, inferBinaryAsString);
    return new FieldSchema(columnName, typeName, "Inferred from Parquet file.");
  }

  public List<FieldSchema> readSchema(Configuration conf, String file) throws SerDeException {
      FileMetaData metadata;
      try {
        HadoopInputFile inputFile = HadoopInputFile.fromPath(new Path(file), conf);
        try(ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
          metadata = reader.getFileMetaData();
        }
      } catch (Exception e) {
        throw new SerDeException(ErrorMsg.PARQUET_FOOTER_ERROR.getErrorCodedMsg(), e);
      }

      MessageType msg = metadata.getSchema();
      List<FieldSchema> schema = new ArrayList<>();
      String inferBinaryAsStringValue = conf.get(HiveConf.ConfVars.HIVE_PARQUET_INFER_BINARY_AS.varname);
      boolean inferBinaryAsString = "string".equalsIgnoreCase(inferBinaryAsStringValue);

      for (Type field: msg.getFields()) {
        FieldSchema fieldSchema = convertParquetTypeToFieldSchema(field, inferBinaryAsString);
        schema.add(fieldSchema);
        LOG.debug("Inferred field schema {}", fieldSchema);
      }
      return schema;
  }
}
