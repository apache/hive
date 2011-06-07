package org.apache.hadoop.hive.cassandra.serde;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.cassandra.input.HiveCassandraStandardRowResult;
import org.apache.hadoop.hive.cassandra.input.LazyCassandraRow;
import org.apache.hadoop.hive.cassandra.output.CassandraPut;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.SerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
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
  public static final String CASSANDRA_SPLIT_SIZE = "cassandra.input.split.size";
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

  public static final int DEFAULT_SPLIT_SIZE = 64 * 1024;
  public static final int DEFAULT_RANGE_BATCH_SIZE = 1000;
  public static final int DEFAULT_SLICE_PREDICATE_SIZE = 1000;
  public static final String DEFAULT_CASSANDRA_HOST = "localhost";
  public static final String DEFAULT_CASSANDRA_PORT = "9160";
  public static final String DEFAULT_CONSISTENCY_LEVEL = "ONE";

  /* names of columns from SerdeParameters */
  private List<String> cassandraColumnNames;
  /* index of key column in results */
  private int iKey;
  private TableMapping mapping;

  private ObjectInspector cachedObjectInspector;
  private SerDeParameters serdeParams;
  private LazyCassandraRow cachedCassandraRow;
  private String cassandraColumnFamily;
  private List<byte[]> cassandraColumnNamesBytes;


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
    cassandraColumnFamily = getCassandraColumnFamily(tbl);
    cassandraColumnNames = parseOrCreateColumnMapping(tbl);
    iKey = cassandraColumnNames.indexOf(StandardColumnSerDe.CASSANDRA_KEY_COLUMN);

    serdeParams = LazySimpleSerDe.initSerdeParams(job, tbl, serdeName);

    setTableMapping();

    if (cassandraColumnNames.size() != serdeParams.getColumnNames().size()) {
      throw new SerDeException(serdeName + ": columns has " +
          serdeParams.getColumnNames().size() +
          " elements while cassandra.columns.mapping has " +
          cassandraColumnNames.size() + " elements" +
          " (counting the key if implicit)");
    }


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

  /**
   * Set the table mapping. We only support transposed mapping and regular table mapping for now.
   *
   * @throws SerDeException
   */
  private void setTableMapping() throws SerDeException {
    if (isTransposed(cassandraColumnNames)) {
      mapping = new TransposedMapping(cassandraColumnFamily, cassandraColumnNames, serdeParams);
    } else {
      mapping = new RegularTableMapping(cassandraColumnFamily, cassandraColumnNames, serdeParams);
    }
  }

  /**
   * Parse cassandra column family name from table properties.
   *
   * @param tbl table properties
   * @return cassandra column family name
   * @throws SerDeException error parsing column family name
   */
  private String getCassandraColumnFamily(Properties tbl) throws SerDeException {
    String result = tbl.getProperty(StandardColumnSerDe.CASSANDRA_CF_NAME);

    if (result == null) {

      result = tbl
          .getProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_NAME);

      if (result == null) {
        throw new SerDeException("CassandraColumnFamily not defined" + tbl.toString());
      }

      if (result.indexOf(".") != -1) {
        result = result.substring(result.indexOf(".") + 1);
      }
    }

    return result;
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
   * Turns obj (a Hive Row) into a cassandra data format.
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
    try {
      assert iKey >= 0;
      return mapping.getWritable(fields, list, declaredFields);
    } catch (IOException e) {
      throw new SerDeException("Unable to serialize this object! " + e);
    }
  }

  /**
   * Parse the column mappping from table properties. If cassandra.columns.mapping
   * is defined in the property, use it to create the mapping. Otherwise, create the mapping from table
   * columns using the default mapping.
   *
   * @param tbl table properties
   * @return A list of column names
   * @throws SerDeException
   */
  private List<String> parseOrCreateColumnMapping(Properties tbl) throws SerDeException {
    String prop = tbl.getProperty(CASSANDRA_COL_MAPPING);

    if (prop != null) {
      return parseColumnMapping(prop);
    } else {
      String tblColumnStr = tbl.getProperty(Constants.LIST_COLUMNS);

      if (tblColumnStr != null) {

        //auto-create
        String mappingStr = createColumnMappingString(tblColumnStr);

        return Arrays.asList(mappingStr.split(","));

      } else {
        throw new SerDeException("Can't find table column definitions");
      }
    }
  }



  /*
   * Creates the cassandra column mappings from the hive column names.
   * This would be triggered when no cassandra.columns.mapping has been defined
   * in the user query.
   *
   * row_key is a special column name, it maps to the key of a row in cassandra;
   * column_name maps to the name of a column/supercolumn;
   * value maps to the value of a column;
   * sub_column_name maps to the name of a column (This can only be used for a super column family.)
   *
   * @param tblColumnStr hive table column names
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
        throw new IllegalArgumentException("Transposed table definition missing required fields!");
      }

      return transposedMapping.substring(1);//skip leading ,
    }

    //Regular non-transposed logic. The first column maps to the key automatically.
    StringBuffer mappingStr = new StringBuffer(CASSANDRA_KEY_COLUMN);
    for (int i = 1; i < colNames.length; i++) {
      mappingStr.append("," + colNames[i]);
    }

    return mappingStr.toString();
  }

  /**
   * Return if a table is a transposed. A table is transposed when the column mapping is like
   * (:key, :column, :value) or (:key, :column, :subcolumn, :value).
   *
   * @param column mapping
   * @return true if a table is transposed, otherwise false
   */
  public static boolean isTransposed(List<String> columnNames)
  {
    if(columnNames == null || columnNames.size() == 0) {
      throw new IllegalArgumentException("no cassandra column information found");
    }

    boolean hasKey = false;
    boolean hasColumn = false;
    boolean hasValue = false;
    boolean hasSubColumn = false;

    for (String column : columnNames) {
      if (column.equalsIgnoreCase(CASSANDRA_KEY_COLUMN)) {
          hasKey = true;
      } else if (column.equalsIgnoreCase(CASSANDRA_COLUMN_COLUMN)) {
          hasColumn = true;
      } else if (column.equalsIgnoreCase(CASSANDRA_SUBCOLUMN_COLUMN)) {
        hasSubColumn = true;
      } else if (column.equalsIgnoreCase(CASSANDRA_VALUE_COLUMN)) {
        hasValue = true;
      } else {
        return false;
      }
    }

    //only requested row key
    if(columnNames.size() == 1 && hasKey) {
      return false;
    }

    if(!hasKey || !hasValue || !hasColumn) {
      return false;
    }

    return true;
  }

  /**
   * Parses the cassandra columns mapping to identify the column name.
   * One of the Hive table columns maps to the cassandra row key, by default the
   * first column.
   *
   * @param columnMapping - the column mapping specification to be parsed
   * @return a list of cassandra column names
   */
  public static List<String> parseColumnMapping(String columnMapping)
  {
    assert columnMapping != null && !columnMapping.equals("");
    String[] columnArray = columnMapping.split(",");

    List<String> columnList = Arrays.asList(columnArray);

    int iKey = columnList.indexOf(CASSANDRA_KEY_COLUMN);

    if (iKey == -1) {
      columnList = new ArrayList<String>(columnList);
      columnList.add(0, CASSANDRA_KEY_COLUMN);
    }

    return columnList;
  }


  /**
   * @return 0-based offset of the key column within the table
   */
  public int getKeyColumnOffset() {
    return iKey;
  }
}
