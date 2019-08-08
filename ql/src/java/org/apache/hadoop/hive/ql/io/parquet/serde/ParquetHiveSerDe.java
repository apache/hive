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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.convert.ParquetSchemaReader;
import org.apache.hadoop.hive.ql.io.parquet.convert.ParquetToHiveSchemaConverter;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.FieldNode;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.io.ParquetHiveRecord;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * A ParquetHiveSerDe for Hive (with the deprecated package mapred)
 *
 */
@SerDeSpec(schemaProps = {serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES,
        ParquetOutputFormat.COMPRESSION})
public class ParquetHiveSerDe extends AbstractSerDe {
  public static final Logger LOG = LoggerFactory.getLogger(ParquetHiveSerDe.class.getName());

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
  public final void initialize(final Configuration conf, final Properties tbl) throws SerDeException {
    List<String> columnNames;
    List<TypeInfo> columnTypes;
    // Get column names and sort order
    final String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
    final String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    final String columnNameDelimiter = tbl.containsKey(serdeConstants.COLUMN_NAME_DELIMITER) ? tbl
        .getProperty(serdeConstants.COLUMN_NAME_DELIMITER) : String.valueOf(SerDeUtils.COMMA);
    if (columnNameProperty.length() == 0 && columnTypeProperty.length() == 0) {
      final String locationProperty = tbl.getProperty("location", null);
      Path parquetFile = locationProperty != null ? getParquetFile(conf,
          new Path(locationProperty)) : null;
      if (parquetFile == null) {
        /**
         * Attempt to determine hive schema failed, but can not throw
         * an exception, as Hive calls init on the serde during
         * any call, including calls to update the serde properties, meaning
         * if the serde is in a bad state, there is no way to update that state.
         */
        LOG.error("Failed to create hive schema for the parquet backed table.\n" +
            "Either provide schema for table,\n" +
            "OR make sure that external table's path has at least one parquet file with required " +
            "metadata");
        columnNames = new ArrayList<String>();
        columnTypes = new ArrayList<TypeInfo>();
      } else {
        StructTypeInfo structTypeInfo = null;
        try {
          structTypeInfo = new ParquetToHiveSchemaConverter(tbl).convert(
              ParquetSchemaReader.read(parquetFile));
        } catch (IOException ioe) {
          LOG.error(ioe.getMessage(), ioe);
        } catch (UnsupportedOperationException ue) {
          LOG.error(ue.getMessage(), ue);
        } catch (RuntimeException ex) {
          LOG.error(ex.getMessage(), ex);
        }
        if (structTypeInfo == null) {
          columnNames = new ArrayList<String>();
          columnTypes = new ArrayList<TypeInfo>();
        } else {
          columnNames = structTypeInfo.getAllStructFieldNames();
          columnTypes = structTypeInfo.getAllStructFieldTypeInfos();
        }
      }
    } else {
      if (columnNameProperty.length() == 0) {
        columnNames = new ArrayList<String>();
      } else {
        columnNames = Arrays.asList(columnNameProperty.split(","));
      }
      if (columnTypeProperty.length() == 0) {
        columnTypes = new ArrayList<TypeInfo>();
      } else {
        columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
      }
    }

    if (columnNames.size() != columnTypes.size()) {
      LOG.error("ParquetHiveSerde initialization failed. Number of column name and column type " +
          "differs. columnNames = " + columnNames + ", columnTypes = " + columnTypes);
      columnNames = new ArrayList<String>();
      columnTypes = new ArrayList<TypeInfo>();
    }
    // Create row related objects
    StructTypeInfo completeTypeInfo =
        (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
    StructTypeInfo prunedTypeInfo = null;
    if (conf != null) {
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
   * Return null for Parquet format and stats is collected in ParquetRecordWriterWrapper when writer gets
   * closed.
   *
   * @return null
   */
  @Override
  public SerDeStats getSerDeStats() {
    return null;
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

  private Path getParquetFile(Configuration conf, Path loc) {
    if (loc == null) {
      return null;
    }

    Path parquetFile;
    try {
      parquetFile = getAFile(FileSystem.get(new URI(loc.toString()), conf), loc);
    } catch (Exception e) {
      LOG.error("Unable to read file from " + loc + ": " + e, e);
      parquetFile = null;
    }

    return parquetFile;
  }

  private Path getAFile(FileSystem fs, Path path) throws IOException {
    FileStatus status = fs.getFileStatus(path);

    if (status.isFile()) {
      if (status.getLen() > 0) {
        return path;
      } else {
        return null;
      }
    }

    for(FileStatus childStatus: fs.listStatus(path)) {
      Path file = getAFile(fs, childStatus.getPath());

      if (file != null) {
        return file;
      }
    }

    return null;
  }

  @Override public boolean shouldStoreFieldsInMetastore(Map<String, String> tableParams) {
    return true;
  }
}
