package org.apache.hadoop.hive.metastore.dataconnector.jdbc;

import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;

public class RedshiftConnectorProvider extends AbstractJDBCConnectorProvider {
    private static Logger LOG = LoggerFactory.getLogger(RedshiftConnectorProvider.class);

    private static final String DRIVER_CLASS = "com.amazon.redshift.jdbc42.Driver".intern();

    public RedshiftConnectorProvider(String dbName, DataConnector dataConn) {
        super(dbName, dataConn, DRIVER_CLASS);
    }

    protected String getDataType(String dbDataType, int size) {
        String mappedType = super.getDataType(dbDataType, size);

        // The VOID type points to the corresponding datatype not existing in hive. These datatypes are datastore
        // specific. They need special handling. An example would be the Geometric type that is not supported in Hive.
        // The other cases where a datatype in redshift is resolved to a VOID type are during the use of aliases like
        // float8, int8 etc. These can be mapped to existing hive types and are done below.
        if (!mappedType.equalsIgnoreCase(ColumnType.VOID_TYPE_NAME)) {
            return mappedType;
        }

        // map any db specific types here.
        //TODO: Geometric and other redshift specific types need to be supported.
        switch (dbDataType.toLowerCase())
        {
            // The below mappings were tested by creating table definitions in redshift with the respective datatype
            // and querying from the tables.
            // e.g.
            // create table test_int_8(i int8);
            // insert into test_int_8 values(1);
            // 0: jdbc:hive2://> select * from test_int_8;
            //+---------------+
            //| test_int_8.i  |
            //+---------------+
            //| 1             |
            //+---------------+
            //1 row selected (13.381 seconds)
            case "bpchar":
                mappedType = ColumnType.CHAR_TYPE_NAME + wrapSize(size);
                break;
            case "int2":
                mappedType = ColumnType.SMALLINT_TYPE_NAME;
                break;
            case "int4":
                mappedType = ColumnType.INT_TYPE_NAME;
                break;
            case "int8":
                mappedType = ColumnType.BIGINT_TYPE_NAME;
                break;
            case "real":
                mappedType = ColumnType.FLOAT_TYPE_NAME;
                break;
            case "float4":
                mappedType = ColumnType.FLOAT_TYPE_NAME;
                break;
            case "float8":
                mappedType = ColumnType.DOUBLE_TYPE_NAME;
                break;
            case "time":
            case "time without time zone":
                // A table with a time column gets resolved to "time without time zone" in hive.
                mappedType = ColumnType.TIMESTAMP_TYPE_NAME;
                break;
            case "timez":
            case "time with time zone":
                mappedType = ColumnType.TIMESTAMPTZ_TYPE_NAME;
                break;
            default:
                mappedType = ColumnType.VOID_TYPE_NAME;
                break;
        }
        return mappedType;
    }
}
