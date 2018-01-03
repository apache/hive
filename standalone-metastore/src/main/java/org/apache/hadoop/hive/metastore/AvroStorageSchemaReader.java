package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.AvroSchemaUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public class AvroStorageSchemaReader implements StorageSchemaReader {
  private static final Logger LOG = LoggerFactory.getLogger(AvroStorageSchemaReader.class);

  @Override
  public List<FieldSchema> readSchema(Table tbl, EnvironmentContext envContext,
      Configuration conf) throws MetaException {
    Properties tblMetadataProperties = MetaStoreUtils.getTableMetadata(tbl);
    try {
      return AvroSchemaUtils.getFieldsFromAvroSchema(conf, tblMetadataProperties);
    } catch (AvroSerdeException e) {
      LOG.warn("Exception received while reading avro schema for table " + tbl.getTableName(), e);
      throw new MetaException(e.getMessage());
    }
  }
}
