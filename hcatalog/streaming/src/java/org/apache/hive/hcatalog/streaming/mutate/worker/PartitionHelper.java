package org.apache.hive.hcatalog.streaming.mutate.worker;

import java.io.Closeable;
import java.util.List;

import org.apache.hadoop.fs.Path;

/** Implementations are responsible for creating and obtaining path information about partitions. */
interface PartitionHelper extends Closeable {

  /** Return the location of the partition described by the provided values. */
  Path getPathForPartition(List<String> newPartitionValues) throws WorkerException;

  /** Create the partition described by the provided values if it does not exist already. */
  void createPartitionIfNotExists(List<String> newPartitionValues) throws WorkerException;

}