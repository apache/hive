package org.apache.hive.hcatalog.streaming.mutate.worker;

import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class that can create new table partitions within the {@link IMetaStoreClient meta store}. */
class CreatePartitionHelper {

  private static final Logger LOG = LoggerFactory.getLogger(CreatePartitionHelper.class);

  private final IMetaStoreClient metaStoreClient;
  private final String databaseName;
  private final String tableName;

  CreatePartitionHelper(IMetaStoreClient metaStoreClient, String databaseName, String tableName) {
    this.metaStoreClient = metaStoreClient;
    this.databaseName = databaseName;
    this.tableName = tableName;
  }

  /** Returns the expected {@link Path} for a given partition value. */
  Path getPathForPartition(List<String> newPartitionValues) throws WorkerException {
    try {
      String location;
      if (newPartitionValues.isEmpty()) {
        location = metaStoreClient.getTable(databaseName, tableName).getSd().getLocation();
      } else {
        location = metaStoreClient.getPartition(databaseName, tableName, newPartitionValues).getSd().getLocation();
      }
      LOG.debug("Found path {} for partition {}", location, newPartitionValues);
      return new Path(location);
    } catch (NoSuchObjectException e) {
      throw new WorkerException("Table not found '" + databaseName + "." + tableName + "'.", e);
    } catch (TException e) {
      throw new WorkerException("Failed to get path for partitions '" + newPartitionValues + "' on table '"
          + databaseName + "." + tableName + "' with meta store: " + metaStoreClient, e);
    }
  }

  /** Creates the specified partition if it does not already exist. Does nothing if the table is unpartitioned. */
  void createPartitionIfNotExists(List<String> newPartitionValues) throws WorkerException {
    if (newPartitionValues.isEmpty()) {
      return;
    }

    try {
      LOG.debug("Attempting to create partition (if not exists) {}.{}:{}", databaseName, tableName, newPartitionValues);
      Table table = metaStoreClient.getTable(databaseName, tableName);

      Partition partition = new Partition();
      partition.setDbName(table.getDbName());
      partition.setTableName(table.getTableName());
      StorageDescriptor partitionSd = new StorageDescriptor(table.getSd());
      partitionSd.setLocation(table.getSd().getLocation() + Path.SEPARATOR
          + Warehouse.makePartName(table.getPartitionKeys(), newPartitionValues));
      partition.setSd(partitionSd);
      partition.setValues(newPartitionValues);

      metaStoreClient.add_partition(partition);
    } catch (AlreadyExistsException e) {
      LOG.debug("Partition already exisits: {}.{}:{}", databaseName, tableName, newPartitionValues);
    } catch (NoSuchObjectException e) {
      LOG.error("Failed to create partition : " + newPartitionValues, e);
      throw new PartitionCreationException("Table not found '" + databaseName + "." + tableName + "'.", e);
    } catch (TException e) {
      LOG.error("Failed to create partition : " + newPartitionValues, e);
      throw new PartitionCreationException("Failed to create partition '" + newPartitionValues + "' on table '"
          + databaseName + "." + tableName + "'", e);
    }
  }

}
