package org.apache.hadoop.hive.metastore.tools;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.TxnInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.hadoop.hive.metastore.tools.Util.createSchema;
import static org.apache.hadoop.hive.metastore.tools.Util.throwingSupplierWrapper;

public class BenchmarkUtils {
  private static final Logger LOG = LoggerFactory.getLogger(BenchmarkUtils.class);


  static void createManyTables(HMSClient client, int howMany, String dbName, String format) {
    List<FieldSchema> columns = createSchema(Arrays.asList("name", "string"));
    List<FieldSchema> partitions = createSchema(Arrays.asList("date", "string"));
    IntStream.range(0, howMany)
        .forEach(i ->
            throwingSupplierWrapper(() -> client.createTable(
                new Util.TableBuilder(dbName, String.format(format, i))
                    .withType(TableType.MANAGED_TABLE)
                    .withColumns(columns)
                    .withPartitionKeys(partitions)
                    .build())));
  }

  static void dropManyTables(HMSClient client, int howMany, String dbName, String format) {
    IntStream.range(0, howMany)
        .forEach(i ->
            throwingSupplierWrapper(() -> client.dropTable(dbName, String.format(format, i))));
  }

  // Create a simple table with a single column and single partition
  static void createPartitionedTable(HMSClient client, String dbName, String tableName) {
    throwingSupplierWrapper(() -> client.createTable(
        new Util.TableBuilder(dbName, tableName)
            .withType(TableType.MANAGED_TABLE)
            .withColumns(createSchema(Collections.singletonList("name:string")))
            .withPartitionKeys(createSchema(Collections.singletonList("date")))
            .build()));
  }

  static boolean checkTxnsCleaned(HMSClient client, List<Long> txnsOpenedByBenchmark) throws InterruptedException {
    // let's wait the default cleaner run period
    Thread.sleep(100000);
    List<Long> notCleanedTxns = new ArrayList<>();
    throwingSupplierWrapper(() -> {
      List<TxnInfo> txnInfos = client.getOpenTxnsInfo();
      return txnInfos.stream().anyMatch(txnsOpenedByBenchmark::contains);
    });

    return false;
  }
}
