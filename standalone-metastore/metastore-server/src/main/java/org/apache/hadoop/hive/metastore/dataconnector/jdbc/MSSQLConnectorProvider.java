package org.apache.hadoop.hive.metastore.dataconnector.jdbc;

import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.sql.*;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

public class MSSQLConnectorProvider extends AbstractJDBCConnectorProvider {
    private static Logger LOG = LoggerFactory.getLogger(MySQLConnectorProvider.class);
    private static final String DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver".intern();

    public MSSQLConnectorProvider(String dbName, DataConnector dataConn) {
        super(dbName, dataConn);
        driverClassName = DRIVER_CLASS;
    }

    /**
     * Returns Hive Table objects from the remote database for tables that match a name pattern.
     * @return List A collection of objects that match the name pattern, null otherwise.
     * @throws MetaException To indicate any failures with executing this API
     * @param regex
     */
    @Override public List<Table> getTables(String regex) throws MetaException {
        return null;
    }

    @Override protected ResultSet fetchTableMetadata(String tableName) throws MetaException {
        ResultSet rs = null;
        try {
            test();
            rs = getConnection().getMetaData().getColumns(null, scoped_db, tableName, null);
        } catch (SQLException | ClassNotFoundException sqle) {
            LOG.warn("Could not retrieve table names from remote datasource, cause:" + sqle.getMessage());
            throw new MetaException("Could not retrieve table names from remote datasource, cause:" + sqle.getMessage());
        }
        return rs;
    }

    private void test() throws ClassNotFoundException {
        Class.forName(DRIVER_CLASS);
        String dbName = "new_hive";
        String user = "hiveuser";
        String password = "hivePassw0rd";
        try{
            String mssqlurl = "jdbc:sqlserver://localhost:1433;DatabaseName=";
            Connection con = DriverManager.getConnection(mssqlurl+dbName, user, password);
            Statement st = con.createStatement();
            ResultSet rs = st.executeQuery("select * from sample");
            List<String> vals = new ArrayList<>();
            while (rs.next()) {
                FieldSchema fs = new FieldSchema();
                vals.add(rs.getString("id"));
            }
            System.out.println(vals);
        }catch(Exception e){

        }
    }

    @Override protected ResultSet fetchTableNames() throws MetaException {
        return null;
    }

    @Override public List<String> getTableNames() throws MetaException {
        ResultSet rs = null;
        try {
            rs = getConnection().getMetaData().getTables(null, scoped_db, null, null);
            if (rs != null) {
                List<String> tables = new ArrayList<String>();
                while(rs.next()) {
                    tables.add(rs.getString(3));
                }
                return tables;
            }
        } catch (SQLException sqle) {
            LOG.warn("Could not retrieve table names from remote datasource, cause:" + sqle.getMessage());
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                    rs = null;
                }
            } catch(Exception e) { /* ignore */
                LOG.info("Error in MSSQL Data Provider: "+e.getMessage());
            }
        }
        return null;
    }

    protected String getDataType(String dbDataType, int size) {
        String mappedType = super.getDataType(dbDataType, size);
        if (!mappedType.equalsIgnoreCase(ColumnType.VOID_TYPE_NAME)) {
            return mappedType;
        }

        // map any db specific types here.
        //TODO: bit data types of oracle needs to be supported.
        switch (dbDataType.toLowerCase())
        {
            case "nvarchar":
            case "nchar":
                mappedType = ColumnType.VARCHAR_TYPE_NAME + wrapSize(size);
                break;
            case "bit":
                mappedType = ColumnType.BOOLEAN_TYPE_NAME;
                break;
            case "number":
                mappedType =  ColumnType.INT_TYPE_NAME;
                break;
            default:
                mappedType = ColumnType.VOID_TYPE_NAME;
        }
        return mappedType;
    }
}
