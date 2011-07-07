package org.apache.hadoop.hive.cassandra.serde;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class StandardColumnSerDe extends AbstractColumnSerDe {

  public static final Log LOG = LogFactory.getLog(StandardColumnSerDe.class.getName());

  /**
   * Initialize the cassandra serialization and deserialization parameters from table properties and configuration.
   *
   * @param job
   * @param tbl
   * @param serdeName
   * @throws SerDeException
   */
  @Override
  protected void initCassandraSerDeParameters(Configuration job, Properties tbl, String serdeName)
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

  @Override
  protected ObjectInspector createObjectInspector() {
    return LazyFactory.createLazyStructInspector(
              serdeParams.getColumnNames(),
              serdeParams.getColumnTypes(),
              serdeParams.getSeparators(),
              serdeParams.getNullSequence(),
              serdeParams.isLastColumnTakesRest(),
              serdeParams.isEscaped(),
              serdeParams.getEscapeChar());
  }

}
