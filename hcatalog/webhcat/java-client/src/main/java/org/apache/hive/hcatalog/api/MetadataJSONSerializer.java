package org.apache.hive.hcatalog.api;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TJSONProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MetadataSerializer implementation, that serializes HCat API elements into JSON.
 */
class MetadataJSONSerializer extends MetadataSerializer {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataJSONSerializer.class);

  MetadataJSONSerializer() throws HCatException {}

  @Override
  public String serializeTable(HCatTable hcatTable) throws HCatException {
    try {
      return new TSerializer(new TJSONProtocol.Factory())
          .toString(hcatTable.toHiveTable(), "UTF-8");
    }
    catch (TException exception) {
      throw new HCatException("Could not serialize HCatTable: " + hcatTable, exception);
    }
  }

  @Override
  public HCatTable deserializeTable(String hcatTableStringRep) throws HCatException {
    try {
      Table table = new Table();
      new TDeserializer(new TJSONProtocol.Factory()).deserialize(table, hcatTableStringRep, "UTF-8");
      return new HCatTable(table);
    }
    catch(TException exception) {
      if (LOG.isDebugEnabled())
        LOG.debug("Could not de-serialize from: " + hcatTableStringRep);
      throw new HCatException("Could not de-serialize HCatTable.", exception);
    }
  }

  @Override
  public String serializePartition(HCatPartition hcatPartition) throws HCatException {
    try {
      return new TSerializer(new TJSONProtocol.Factory())
          .toString(hcatPartition.toHivePartition(), "UTF-8");
    }
    catch (TException exception) {
      throw new HCatException("Could not serialize HCatPartition: " + hcatPartition, exception);
    }
  }

  @Override
  public HCatPartition deserializePartition(String hcatPartitionStringRep) throws HCatException {
    try {
      Partition partition = new Partition();
      new TDeserializer(new TJSONProtocol.Factory()).deserialize(partition, hcatPartitionStringRep, "UTF-8");
      return new HCatPartition(null, partition);
    }
    catch(TException exception) {
      if (LOG.isDebugEnabled())
        LOG.debug("Could not de-serialize partition from: " + hcatPartitionStringRep);
      throw new HCatException("Could not de-serialize HCatPartition.", exception);
    }
  }
}
