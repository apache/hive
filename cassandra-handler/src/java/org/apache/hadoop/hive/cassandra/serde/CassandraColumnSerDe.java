package org.apache.hadoop.hive.cassandra.serde;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;

public class CassandraColumnSerDe extends AbstractColumnSerDe {
  public static final String CASSANDRA_VALIDATOR_TYPE = "cassandra.cf.validatorType"; // validator type

  public static final AbstractType DEFAULT_VALIDATOR_TYPE = BytesType.instance;

  private List<AbstractType> validatorType;

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

    cassandraColumnNamesBytes = new ArrayList<BytesWritable>();
    for (String columnName : cassandraColumnNames) {
      cassandraColumnNamesBytes.add(new BytesWritable(columnName.getBytes()));
    }

    iKey = cassandraColumnNames.indexOf(AbstractColumnSerDe.CASSANDRA_KEY_COLUMN);

    serdeParams = LazySimpleSerDe.initSerdeParams(job, tbl, serdeName);

    validatorType = parseOrCreateValidatorType(tbl);

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
    return CassandraLazyFactory.createLazyStructInspector(
              serdeParams.getColumnNames(),
              serdeParams.getColumnTypes(),
              validatorType,
              serdeParams.getSeparators(),
              serdeParams.getNullSequence(),
              serdeParams.isLastColumnTakesRest(),
              serdeParams.isEscaped(),
              serdeParams.getEscapeChar());
  }

  /**
   * Parse or create the validator types. If <code>CASSANDRA_VALIDATOR_TYPE</code> is defined in the property,
   * it will be used for parsing; Otherwise an empty list will be returned;
   *
   * @param tbl property list
   * @return a list of validator type or an empty list if no property is defined
   * @throws SerDeException when the number of validator types is fewer than the number of columns or when no matching
   * validator type is found in Cassandra.
   */
  private List<AbstractType> parseOrCreateValidatorType(Properties tbl)
    throws SerDeException {
    String prop = tbl.getProperty(CASSANDRA_VALIDATOR_TYPE);
    List<AbstractType> result = new ArrayList<AbstractType>();

    if (prop != null) {
      assert StringUtils.isNotBlank(prop);
      String[] validators = prop.split(",");
      String[] trimmedValidators = trim(validators);

      List<String> columnList = Arrays.asList(trimmedValidators);
      result = parseValidatorType(columnList);

      if (result.size() < cassandraColumnNames.size()) {
        throw new SerDeException("There are fewer validator types defined than the column names. " +
            "ColumnaName size: " + cassandraColumnNames.size() + " ValidatorType size: " + result.size());
      }
    }

    return result;
  }

  /**
   * Parses the cassandra columns mapping to identify the column name.
   * One of the Hive table columns maps to the cassandra row key, by default the
   * first column.
   *
   * @param columnList a list of column validator type in String format
   * @return a list of cassandra validator type
   */
  private List<AbstractType> parseValidatorType(List<String> columnList)
    throws SerDeException {
    List<AbstractType> types = new ArrayList<AbstractType>();

    for (String str : columnList) {
      if (StringUtils.isBlank(str)) {
        types.add(DEFAULT_VALIDATOR_TYPE);
      } else {
        try {
          types.add(TypeParser.parse(str));
        } catch (ConfigurationException e) {
          throw new SerDeException("Invalid Cassandra validator type ' " + str + "'");
        }
      }
    }

    return types;
  }

}
