/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.storage.jdbc;

import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hive.storage.jdbc.*;
import org.apache.hive.storage.jdbc.conf.DatabaseType;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfig;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.*;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.TableType;

public class TestSQL {
//    private Driver driver;
//    public TestSQL(Driver d){
//        this.driver = d;
//    }

    @Test
    public void testMysql() throws Exception{
        //Registering the Driver
        DriverManager.registerDriver(new com.mysql.jdbc.Driver());
        String dbName = "mydatabase";
        //Getting the connection
        String mysqlUrl = "jdbc:mysql://localhost/"+dbName;
        Connection con = DriverManager.getConnection(mysqlUrl, "root", null);
        System.out.println("Connection established......");

        String tablename = "tb1";
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT table_name, column_name, is_nullable, data_type, character_maximum_length FROM INFORMATION_SCHEMA.Columns where table_schema='"+dbName+"' and table_name='"+tablename+"'");
        List<FieldSchema> cols = new ArrayList<>();
        while (rs.next()) {
            FieldSchema fs = new FieldSchema();
            fs.setName(rs.getString("COLUMN_NAME"));
            fs.setType(getDataType(rs.getString("DATA_TYPE")));
            cols.add(fs);
        }
        //Setting the storage descriptor.
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(cols);
        sd.setSerdeInfo(new SerDeInfo());
        sd.getSerdeInfo().setName(tablename);
        sd.setInputFormat(JdbcInputFormat.class.getName());
        sd.setOutputFormat(JdbcOutputFormat.class.getName());

        //Setting the table properties.
        Map<String, String> tblProps = new HashMap<>();
        tblProps.put(Constants.JDBC_DATABASE_TYPE, DatabaseType.MYSQL.toString());
        tblProps.put(Constants.JDBC_DRIVER, "com.mysql.jdbc.Driver");
        tblProps.put(Constants.JDBC_URL, mysqlUrl); // "jdbc://localhost:3306/hive"
        tblProps.put(Constants.JDBC_USERNAME, "saihemanth");
        tblProps.put(Constants.JDBC_PASSWORD, null);
        tblProps.put(Constants.JDBC_TABLE, tablename);
        // TODO: Need to include schema, catalog info in the paramters list.

        //Setting the required table information
        Table table = new Table();
        table.setTableName(tablename);
        table.setDbName(dbName);
        table.setTableType(TableType.EXTERNAL_TABLE.toString());
        table.setSd(sd);
        table.setParameters(tblProps);
//        return table;
    }

    private String getDataType(String mySqlType){
        //TODO: Geomentric, network, bit, array data types of postgresql needs to be supported.
        switch(mySqlType)
        {
            case "char":
                return ColumnType.CHAR_TYPE_NAME;
            case "varchar":
            case "tinytext":
                return ColumnType.VARCHAR_TYPE_NAME;
            case "text":
            case "mediumtext":
            case "enum":
            case "set":
            case "tsvector":
            case "tsquery":
            case "uuid":
            case "json":
                return ColumnType.STRING_TYPE_NAME;
            case "blob":
            case "mediumblob":
            case "longblob":
            case "bytea":
                return ColumnType.BINARY_TYPE_NAME;
            case "tinyint":
                return ColumnType.TINYINT_TYPE_NAME;
            case "smallint":
            case "smallserial":
                return ColumnType.SMALLINT_TYPE_NAME;
            case "mediumint":
            case "int":
            case "serial":
                return ColumnType.INT_TYPE_NAME;
            case "bigint":
            case "bigserial":
            case "money":
                return ColumnType.BIGINT_TYPE_NAME;
            case "float":
            case "real":
                return ColumnType.FLOAT_TYPE_NAME;
            case "double":
            case "double precision":
                return ColumnType.DOUBLE_TYPE_NAME;
            case "decimal":
            case "numeric":
                return ColumnType.DECIMAL_TYPE_NAME;
            case "date":
                return ColumnType.DATE_TYPE_NAME;
            case "datetime":
                return ColumnType.DATETIME_TYPE_NAME;
            case "timestamp":
            case "time":
            case "interval":
                return ColumnType.TIMESTAMP_TYPE_NAME;
            case "timestampz":
            case "timez":
                return ColumnType.TIMESTAMPTZ_TYPE_NAME;
            case "boolean":
                return ColumnType.BOOLEAN_TYPE_NAME;
            default:
                return ColumnType.VOID_TYPE_NAME;
        }
    }

    @Test
    public void testPostgresSQL() throws Exception{
        String dbName = "mydatabasename";
        String tableName = "t1";
        String user = "saihemanth";
        DriverManager.registerDriver(new org.postgresql.Driver());
        try {
            String postgresUrl = "jdbc:postgresql://localhost:5432/";
            Connection con = DriverManager.getConnection(postgresUrl+dbName, user, null);
            System.out.println("Connection established......");
            Statement st = con.createStatement();
            ResultSet rs = st.executeQuery("SELECT table_name, column_name, is_nullable, data_type, character_maximum_length FROM INFORMATION_SCHEMA.Columns where table_catalog='"+dbName+"' and table_name='"+tableName+"'");
            List<FieldSchema> cols = new ArrayList<>();

            while (rs.next()) {
                FieldSchema fs = new FieldSchema();
                fs.setName(rs.getString("COLUMN_NAME"));
                fs.setType(getDataType(rs.getString("DATA_TYPE")));
                cols.add(fs);
            }
            //Setting the storage descriptor.
            StorageDescriptor sd = new StorageDescriptor();
            sd.setCols(cols);
            sd.setSerdeInfo(new SerDeInfo());
            sd.getSerdeInfo().setName(tableName);
            sd.setInputFormat(JdbcInputFormat.class.getName());
            sd.setOutputFormat(JdbcOutputFormat.class.getName());

            //Setting the table properties.
            Map<String, String> tblProps = new HashMap<>();
            tblProps.put(Constants.JDBC_DATABASE_TYPE, DatabaseType.POSTGRES.toString());
            tblProps.put(Constants.JDBC_DRIVER, "org.postgresql.Driver");
            tblProps.put(Constants.JDBC_URL, postgresUrl); // "jdbc://localhost:3306/hive"
            tblProps.put(Constants.JDBC_USERNAME, "saihemanth");
            tblProps.put(Constants.JDBC_PASSWORD, null);
            tblProps.put(Constants.JDBC_TABLE, tableName);
            // TODO: Need to include schema, catalog info in the paramters list.

            //Setting the required table information
            Table table = new Table();
            table.setTableName(tableName);
            table.setDbName(dbName);
            table.setTableType(TableType.EXTERNAL_TABLE.toString());
            table.setSd(sd);
            table.setParameters(tblProps);
//        return table;

        } catch (SQLException ex) {
            System.out.println("Exception: "+ex.getErrorCode()+" : "+ex.getMessage());
        }
    }
}

