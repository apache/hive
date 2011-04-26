package org.apache.hadoop.hive.cassandra.serde;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.cassandra.input.HiveCassandraStandardRowResult;
import org.apache.hadoop.hive.cassandra.input.LazyCassandraRow;
import org.apache.hadoop.hive.cassandra.output.CassandraColumn;
import org.apache.hadoop.hive.cassandra.output.CassandraPut;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.SerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;

public class StandardColumnSerDe implements SerDe {

  public static final Log LOG = LogFactory.getLog(StandardColumnSerDe.class.getName());
  public static final String CASSANDRA_KEYSPACE_NAME = "cassandra.ks.name"; // keyspace
  public static final String CASSANDRA_KEYSPACE_REPFACTOR = "cassandra.ks.repfactor"; //keyspace replication factor
  public static final String CASSANDRA_KEYSPACE_STRATEGY = "cassandra.ks.strategy"; //keyspace replica placement strategy

  public static final String CASSANDRA_CF_NAME = "cassandra.cf.name"; // column family
  public static final String CASSANDRA_RANGE_BATCH_SIZE = "cassandra.range.size";
  public static final String CASSANDRA_SLICE_PREDICATE_SIZE = "cassandra.slice.predicate.size";
  public static final String CASSANDRA_HOST = "cassandra.host"; // initialHost
  public static final String CASSANDRA_PORT = "cassandra.port"; // rcpPort
  public static final String CASSANDRA_PARTITIONER = "cassandra.partitioner"; // partitioner
  public static final String CASSANDRA_COL_MAPPING = "cassandra.columns.mapping";

  public static final String CASSANDRA_SPECIAL_COLUMN_KEY = "row_key";
  public static final String CASSANDRA_SPECIAL_COLUMN_COL = "column_name";
  public static final String CASSANDRA_SPECIAL_COLUMN_SCOL= "sub_column_name";
  public static final String CASSANDRA_SPECIAL_COLUMN_VAL = "value";

  public static final String CASSANDRA_KEY_COLUMN       = ":key";
  public static final String CASSANDRA_COLUMN_COLUMN    = ":column";
  public static final String CASSANDRA_SUBCOLUMN_COLUMN = ":subcolumn";
  public static final String CASSANDRA_VALUE_COLUMN     = ":value";

  public static final String CASSANDRA_CONSISTENCY_LEVEL = "cassandra.consistency.level";
  public static final String CASSANDRA_THRIFT_MODE = "cassandra.thrift.mode";

  /* names of columns from SerdeParameters */
  private List<String> cassandraColumnNames;
  /* index of key column in results */
  private int iKey;

  private ObjectInspector cachedObjectInspector;
  private SerDeParameters serdeParams;
  private LazyCassandraRow cachedCassandraRow;
  private String cassandraColumnFamily;
  private List<byte[]> cassandraColumnNamesBytes;
  private final ByteStream.Output serializeStream = new ByteStream.Output();
  private boolean useJSONSerialize;

  private byte[] separators; // the separators array
  private boolean escaped; // whether we need to escape the data when writing out
  private byte escapeChar; // which char to use as the escape char, e.g. '\\'
  private boolean[] needsEscape; // which chars need to be escaped. This array should have size

  // of 128. Negative byte values (or byte values >= 128) are
  // never escaped.

  @Override
  public void initialize(Configuration conf, Properties tbl) throws SerDeException {
    initCassandraSerDeParameters(conf, tbl, getClass().getName());
    cachedObjectInspector = LazyFactory.createLazyStructInspector(
        serdeParams.getColumnNames(),
        serdeParams.getColumnTypes(),
        serdeParams.getSeparators(),
        serdeParams.getNullSequence(),
        serdeParams.isLastColumnTakesRest(),
        serdeParams.isEscaped(),
        serdeParams.getEscapeChar());

    cachedCassandraRow = new LazyCassandraRow(
        (LazySimpleStructObjectInspector) cachedObjectInspector);

    if (LOG.isDebugEnabled()) {
      LOG.debug("CassandraSerDe initialized with : columnNames = "
          + StringUtils.join(serdeParams.getColumnNames(), ",")
          + " columnTypes = "
          + StringUtils.join(serdeParams.getColumnTypes(), ",")
          + " cassandraColumnMapping = "
          + cassandraColumnNames);
    }
  }

  /**
   * Initialize the cassandra serialization and deserialization parameters from table properties and configuration.
   *
   * @param job
   * @param tbl
   * @param serdeName
   * @throws SerDeException
   */
  private void initCassandraSerDeParameters(Configuration job, Properties tbl, String serdeName)
      throws SerDeException {

    //Figure out columnFamily
    cassandraColumnFamily = tbl.getProperty(StandardColumnSerDe.CASSANDRA_CF_NAME);

    if (cassandraColumnFamily == null) {

      cassandraColumnFamily = tbl
          .getProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_NAME);

      if (cassandraColumnFamily == null) {
        throw new SerDeException("CassandraColumnFamily not defined" + tbl.toString());
      }

      if (cassandraColumnFamily.indexOf(".") != -1) {
        cassandraColumnFamily = cassandraColumnFamily
            .substring(cassandraColumnFamily.indexOf(".") + 1);
      }
    }

    cassandraColumnNames = parseOrCreateColumnMapping(tbl, tbl.getProperty(Constants.LIST_COLUMNS));

    iKey = cassandraColumnNames.indexOf(StandardColumnSerDe.CASSANDRA_KEY_COLUMN);

    cassandraColumnNamesBytes = initColumnNamesBytes(cassandraColumnNames);

    serdeParams = LazySimpleSerDe.initSerdeParams(job, tbl, serdeName);

    if (cassandraColumnNames.size() != serdeParams.getColumnNames().size()) {
      throw new SerDeException(serdeName + ": columns has " +
          serdeParams.getColumnNames().size() +
          " elements while cassandra.columns.mapping has " +
          cassandraColumnNames.size() + " elements" +
          " (counting the key if implicit)");
    }

    separators = serdeParams.getSeparators();
    escaped = serdeParams.isEscaped();
    escapeChar = serdeParams.getEscapeChar();
    needsEscape = serdeParams.getNeedsEscape();

    // we just can make sure that "StandardColumn:" is mapped to MAP<String,?>
    for (int i = 0; i < cassandraColumnNames.size(); i++) {
      String cassandraColName = cassandraColumnNames.get(i);
      if (cassandraColName.endsWith(":")) {
        TypeInfo typeInfo = serdeParams.getColumnTypes().get(i);
        if ((typeInfo.getCategory() != Category.MAP) ||
            (((MapTypeInfo) typeInfo).getMapKeyTypeInfo().getTypeName()
                != Constants.STRING_TYPE_NAME)) {

          throw new SerDeException(
              serdeName + ": Cassandra column family '"
                  + cassandraColName
                  + "' should be mapped to map<string,?> but is mapped to "
                  + typeInfo.getTypeName());
        }
      }
    }
  }

  /*
   *
   * @see org.apache.hadoop.hive.serde2.Deserializer#deserialize(org.apache.hadoop.io.Writable)
   * Turns a Cassandra row into a Hive row.
   */
  @Override
  public Object deserialize(Writable w) throws SerDeException {

    if (!(w instanceof HiveCassandraStandardRowResult)) {
      throw new SerDeException(getClass().getName() + ": expects Cassandra Row Result");
    }

    HiveCassandraStandardRowResult crr = (HiveCassandraStandardRowResult) w;
    cachedCassandraRow.init(crr, cassandraColumnNames, cassandraColumnNamesBytes);
    return cachedCassandraRow;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return cachedObjectInspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return CassandraPut.class;
  }

  /*
   * Turns obj (a Hive Row) into a CassandraPut which is key and a MapWritable
   * of column/values
   */
  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    if (objInspector.getCategory() != Category.STRUCT) {
      throw new SerDeException(getClass().toString()
                + " can only serialize struct types, but we got: "
                + objInspector.getTypeName());
    }
    // Prepare the field ObjectInspectors
    StructObjectInspector soi = (StructObjectInspector) objInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    List<Object> list = soi.getStructFieldsDataAsList(obj);
    List<? extends StructField> declaredFields =
          (serdeParams.getRowTypeInfo() != null &&
            ((StructTypeInfo) serdeParams.getRowTypeInfo())
                .getAllStructFieldNames().size() > 0) ?
                ((StructObjectInspector) getObjectInspector()).getAllStructFieldRefs()
                : null;
    CassandraPut put = null;
    try {
      ByteBuffer key = serializeField(iKey, null, fields, list, declaredFields);
      if (key == null) {
        throw new SerDeException("Cassandra row key cannot be NULL");
      }
      put = new CassandraPut(key);
      // Serialize each field except key (done already)
      for (int i = 0; i < fields.size(); i++) {
        if (i != iKey) {
          serializeField(i, put, fields, list, declaredFields);
        }
      }
    } catch (IOException e) {
      throw new SerDeException(e);
    }
    return put;
  }

  private ByteBuffer serializeField(int i, CassandraPut put, List<? extends StructField> fields,
            List<Object> list, List<? extends StructField> declaredFields) throws IOException {

    // column name
    String cassandraColumn = cassandraColumnNames.get(i);

    // Get the field objectInspector and the field object.
    ObjectInspector foi = fields.get(i).getFieldObjectInspector();
    Object f = (list == null ? null : list.get(i));
    if (f == null) {
      return null;
    }

    // If the field corresponds to a column family in cassandra
    // (when would someone ever need this?)
    if (cassandraColumn.endsWith(":")) {
      MapObjectInspector moi = (MapObjectInspector) foi;
      ObjectInspector koi = moi.getMapKeyObjectInspector();
      ObjectInspector voi = moi.getMapValueObjectInspector();

      Map<?, ?> map = moi.getMap(f);
      if (map == null) {
        return null;
      } else {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          // Get the Key
          serializeStream.reset();
          serialize(entry.getKey(), koi, 3);

          // Get the column-qualifier
          byte[] columnQualifier = new byte[serializeStream.getCount()];
          System.arraycopy(serializeStream.getData(), 0, columnQualifier, 0,
              serializeStream.getCount());

          // Get the Value
          serializeStream.reset();

          boolean isNotNull = serialize(entry.getValue(), voi, 3);
          if (!isNotNull) {
            continue;
          }
          byte[] value = new byte[serializeStream.getCount()];
          System.arraycopy(serializeStream.getData(), 0, value, 0, serializeStream.getCount());

          CassandraColumn cc = new CassandraColumn();
          cc.setTimeStamp(System.currentTimeMillis());
          cc.setColumnFamily(cassandraColumnFamily);
          cc.setColumn(columnQualifier);
          cc.setValue(value);
          put.getColumns().add(cc);

        }
      }
    } else {

      // If the field that is passed in is NOT a primitive, and either the
      // field is not declared (no schema was given at initialization), or
      // the field is declared as a primitive in initialization, serialize
      // the data to JSON string. Otherwise serialize the data in the
      // delimited way.

      serializeStream.reset();
      boolean isNotNull;
      if (!foi.getCategory().equals(Category.PRIMITIVE)
                  && (declaredFields == null ||
                      declaredFields.get(i).getFieldObjectInspector().getCategory()
                          .equals(Category.PRIMITIVE) || useJSONSerialize)) {
        isNotNull = serialize(SerDeUtils.getJSONString(f, foi),
                    PrimitiveObjectInspectorFactory.javaStringObjectInspector, 1);
      } else {
        isNotNull = serialize(f, foi, 1);
      }
      if (!isNotNull) {
        return null;
      }
      byte[] key = new byte[serializeStream.getCount()];
      System.arraycopy(serializeStream.getData(), 0, key, 0, serializeStream.getCount());
      if (i == iKey) {
        return ByteBuffer.wrap(key);
      }
      CassandraColumn cc = new CassandraColumn();
      cc.setTimeStamp(System.currentTimeMillis());
      cc.setColumnFamily(cassandraColumnFamily);
      cc.setColumn(cassandraColumn.getBytes());
      cc.setValue(key);
      put.getColumns().add(cc);
    }
    return null;
  }

  private boolean serialize(Object obj, ObjectInspector objInspector, int level)
      throws IOException {

    switch (objInspector.getCategory()) {
    case PRIMITIVE: {
      LazyUtils.writePrimitiveUTF8(
              serializeStream, obj,
              (PrimitiveObjectInspector) objInspector,
              escaped, escapeChar, needsEscape);
      return true;
    }
    case LIST: {
      char separator = (char) separators[level];
      ListObjectInspector loi = (ListObjectInspector) objInspector;
      List<?> list = loi.getList(obj);
      ObjectInspector eoi = loi.getListElementObjectInspector();
      if (list == null) {
        return false;
      } else {
        for (int i = 0; i < list.size(); i++) {
          if (i > 0) {
            serializeStream.write(separator);
          }
          serialize(list.get(i), eoi, level + 1);
        }
      }
      return true;
    }
    case MAP: {
      char separator = (char) separators[level];
      char keyValueSeparator = (char) separators[level + 1];
      MapObjectInspector moi = (MapObjectInspector) objInspector;
      ObjectInspector koi = moi.getMapKeyObjectInspector();
      ObjectInspector voi = moi.getMapValueObjectInspector();

      Map<?, ?> map = moi.getMap(obj);
      if (map == null) {
        return false;
      } else {
        boolean first = true;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          if (first) {
            first = false;
          } else {
            serializeStream.write(separator);
          }
          serialize(entry.getKey(), koi, level + 2);
          serializeStream.write(keyValueSeparator);
          serialize(entry.getValue(), voi, level + 2);
        }
      }
      return true;
    }
    case STRUCT: {
      char separator = (char) separators[level];
      StructObjectInspector soi = (StructObjectInspector) objInspector;
      List<? extends StructField> fields = soi.getAllStructFieldRefs();
      List<Object> list = soi.getStructFieldsDataAsList(obj);
      if (list == null) {
        return false;
      } else {
        for (int i = 0; i < list.size(); i++) {
          if (i > 0) {
            serializeStream.write(separator);
          }
          serialize(list.get(i), fields.get(i).getFieldObjectInspector(), level + 1);
        }
      }
      return true;
    }
    }
    throw new RuntimeException("Unknown category type: " + objInspector.getCategory());
  }

  /**
   * Parse the column mappping from table properties and table columns. If cassandra.columns.mapping
   * is defined in the property, use it to create the mapping. Otherwise, create the mapping from table
   * columns using the default mapping.
   *
   * @param tbl table properties
   * @param tblColumnStr table column names
   * @return A list of column names
   * @throws SerDeException
   */
  public static List<String> parseOrCreateColumnMapping(Properties tbl, String tblColumnStr) throws SerDeException {

    String prop = tbl.getProperty(CASSANDRA_COL_MAPPING);

    if (prop != null) {
      return parseColumnMapping(prop);
    } else if (tblColumnStr != null) {
      //auto-create
      String mappingStr = createColumnMappingString(tblColumnStr);

      return Arrays.asList(mappingStr.split(","));
    } else {
      throw new SerDeException("Can't find table column definitions");
    }
  }

  /*
   * Creates the cassandra column mappings from the hive column names.
   * This would be triggered when no cassandra.columns.mapping has been defined
   * in the user query.
   */
  public static String createColumnMappingString(String tblColumnStr)
  {
    if(tblColumnStr == null || tblColumnStr.trim().isEmpty()) {
      throw new IllegalArgumentException("table must have columns");
    }

    String[] colNames = tblColumnStr.split(",");

    //First check of this is a "transposed_table" by seeing if all
    //values match our special column names
    boolean isTransposedTable = true;
    boolean hasKey = false;
    boolean hasVal = false;
    boolean hasCol = false;
    boolean hasSubCol = false;
    String transposedMapping = "";
    for(String column : colNames) {
      if (column.equalsIgnoreCase(CASSANDRA_SPECIAL_COLUMN_KEY)){
        transposedMapping += ","+CASSANDRA_KEY_COLUMN;
        hasKey = true;
      } else if(column.equalsIgnoreCase(CASSANDRA_SPECIAL_COLUMN_COL)){
        transposedMapping += ","+CASSANDRA_COLUMN_COLUMN;
        hasCol = true;
      } else if(column.equalsIgnoreCase(CASSANDRA_SPECIAL_COLUMN_SCOL)){
        transposedMapping += ","+CASSANDRA_SUBCOLUMN_COLUMN;
        hasSubCol = true;
      } else if(column.equalsIgnoreCase(CASSANDRA_SPECIAL_COLUMN_VAL)){
        transposedMapping += ","+CASSANDRA_VALUE_COLUMN;
        hasVal = true;
      } else {
        isTransposedTable = false;
        break;
      }
    }

    if(isTransposedTable && !(colNames.length == 1 && hasKey)){

      if(!hasKey || !hasVal || !hasCol ) {
        throw new IllegalArgumentException("Transposed table definition missing required fields");
      }

      return transposedMapping.substring(1);//skip leading ,
    }

    //Regular non-transposed logic
    boolean first = true;
    String mappingStr = null;
    for (String column : colNames) {
      if (first) {
        mappingStr = CASSANDRA_KEY_COLUMN;
        first = false;
      }else{
        mappingStr += ","+column;
      }
    }

    return mappingStr;
  }

  /**
   * If only request row key, return false;
   *
   * @param columnNames
   * @return
   */
  public static boolean isTransposed(List<String> columnNames)
  {
    if(columnNames == null || columnNames.size() == 0) {
      throw new IllegalArgumentException("no cassandra column information found");
    }

    boolean isTransposedTable = true;
    boolean hasKey = false;
    boolean hasValue = false;
    boolean hasSubColumn = false;
    boolean hasColumn = false;

    for(String column : columnNames) {
      if(column.equalsIgnoreCase(CASSANDRA_KEY_COLUMN)){
          hasKey = true;
      }else if(column.equalsIgnoreCase(CASSANDRA_COLUMN_COLUMN)){
          hasColumn = true;
      }else if( column.equalsIgnoreCase(CASSANDRA_SUBCOLUMN_COLUMN)){
        hasSubColumn = true;
      }else if(column.equalsIgnoreCase(CASSANDRA_VALUE_COLUMN)){
        hasValue = true;
      } else {
        isTransposedTable = false;
        break;
      }
    }

    if(!isTransposedTable) {
      return isTransposedTable;
    }

    //only requested row key
    if(columnNames.size() == 1 && hasKey) {
      return false;
    }

    if(!hasKey || !hasValue || !hasColumn) {
      return false;
    }

    return isTransposedTable;
  }

  /**
   * Parses the cassandra columns mapping to identify the column name.
   * One of the Hive table columns maps to the HBase row key, by default the
   * first column.
   *
   * @param columnMapping - the column mapping specification to be parsed
   * @return a list of cassandra column names
   */
  public static List<String> parseColumnMapping(String columnMapping)
  {
    //TODO: columnMappings should never be empty or null. Add column mapping verification.
    String[] columnArray = columnMapping.split(",");

    List<String> columnList = Arrays.asList(columnArray);

    int iKey = columnList.indexOf(CASSANDRA_KEY_COLUMN);

    if (iKey == -1) {
      columnList = new ArrayList<String>(columnList);
      columnList.add(0, CASSANDRA_KEY_COLUMN);
    }

    return columnList;
  }

  public static List<byte[]> initColumnNamesBytes(List<String> columnNames) {
    List<byte[]> columnBytes = new ArrayList<byte[]>();

    for (String column : columnNames) {
      columnBytes.add(column.getBytes());
    }

    return columnBytes;
  }
}
