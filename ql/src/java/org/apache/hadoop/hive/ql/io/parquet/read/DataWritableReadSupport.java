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
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.convert.DataWritableRecordConverter;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.util.StringUtils;

import parquet.column.ColumnDescriptor;
import parquet.hadoop.api.ReadSupport;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type;
import parquet.schema.Type.Repetition;

/**
 *
 * A MapWritableReadSupport
 *
 * Manages the translation between Hive and Parquet
 *
 */
public class DataWritableReadSupport extends ReadSupport<ArrayWritable> {

  private static final String TABLE_SCHEMA = "table_schema";
  public static final String HIVE_SCHEMA_KEY = "HIVE_TABLE_SCHEMA";
  public static final String PARQUET_COLUMN_INDEX_ACCESS = "parquet.column.index.access";

  /**
   * From a string which columns names (including hive column), return a list
   * of string columns
   *
   * @param columns comma separated list of columns
   * @return list with virtual columns removed
   */
  private static List<String> getColumns(final String columns) {
    return (List<String>) VirtualColumn.
        removeVirtualColumns(StringUtils.getStringCollection(columns));
  }

  /**
   *
   * It creates the readContext for Parquet side with the requested schema during the init phase.
   *
   * @param configuration needed to get the wanted columns
   * @param keyValueMetaData // unused
   * @param fileSchema parquet file schema
   * @return the parquet ReadContext
   */
  @Override
  public parquet.hadoop.api.ReadSupport.ReadContext init(final Configuration configuration,
      final Map<String, String> keyValueMetaData, final MessageType fileSchema) {
    final String columns = configuration.get(IOConstants.COLUMNS);
    final Map<String, String> contextMetadata = new HashMap<String, String>();
    final boolean indexAccess = configuration.getBoolean(PARQUET_COLUMN_INDEX_ACCESS, false);
    if (columns != null) {
      final List<String> listColumns = getColumns(columns);
      final Map<String, String> lowerCaseFileSchemaColumns = new HashMap<String,String>();
      for (ColumnDescriptor c : fileSchema.getColumns()) {
        lowerCaseFileSchemaColumns.put(c.getPath()[0].toLowerCase(), c.getPath()[0]);
      }
      final List<Type> typeListTable = new ArrayList<Type>();
      if(indexAccess) {
        for (int index = 0; index < listColumns.size(); index++) {
          //Take columns based on index or pad the field
          if(index < fileSchema.getFieldCount()) {
            typeListTable.add(fileSchema.getType(index));
          } else {
            //prefixing with '_mask_' to ensure no conflict with named
            //columns in the file schema
            typeListTable.add(new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "_mask_"+listColumns.get(index)));
          }
        }
      } else {
        for (String col : listColumns) {
          col = col.toLowerCase();
          // listColumns contains partition columns which are metadata only
          if (lowerCaseFileSchemaColumns.containsKey(col)) {
            typeListTable.add(fileSchema.getType(lowerCaseFileSchemaColumns.get(col)));
          } else {
            // below allows schema evolution
            typeListTable.add(new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, col));
          }
        }
      }
      MessageType tableSchema = new MessageType(TABLE_SCHEMA, typeListTable);
      contextMetadata.put(HIVE_SCHEMA_KEY, tableSchema.toString());

      final List<Integer> indexColumnsWanted = ColumnProjectionUtils.getReadColumnIDs(configuration);

      final List<Type> typeListWanted = new ArrayList<Type>();

      for (final Integer idx : indexColumnsWanted) {
        if (idx < listColumns.size()) {
          String col = listColumns.get(idx);
          if (indexAccess) {
              typeListWanted.add(fileSchema.getFields().get(idx));
          } else {
            col = col.toLowerCase();
            if (lowerCaseFileSchemaColumns.containsKey(col)) {
              typeListWanted.add(tableSchema.getType(lowerCaseFileSchemaColumns.get(col)));
            }
          }
        }
      }
      MessageType requestedSchemaByUser = new MessageType(fileSchema.getName(), typeListWanted);
      return new ReadContext(requestedSchemaByUser, contextMetadata);
    } else {
      contextMetadata.put(HIVE_SCHEMA_KEY, fileSchema.toString());
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
