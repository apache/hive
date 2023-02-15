package org.apache.hadoop.hive.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.mapreduce.TableInputFormatBase;

import java.io.IOException;

public class HiveHBaseTableInputFormatDelegate extends TableInputFormatBase {
    void initializeTableDelegate(Connection connection, TableName tableName) throws IOException {
        initializeTable(connection, tableName);
    }

    void closeTableDelegate() throws IOException {
        closeTable();
    }
}
