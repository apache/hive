package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Cloudera specific test case for CDHMetaStoreSchemaInfo
 *
 */
public class TestCDHMetaStoreSchemaFactory {
  public HiveConf conf;

  @Before
  public void setup() {
    conf = new HiveConf(this.getClass());
  }

  @Test
  public void testWithCdhMetastoreSchemaInfo() {
    conf.set(HiveConf.ConfVars.METASTORE_SCHEMA_INFO_CLASS.varname,
      CDHMetaStoreSchemaInfo.class.getCanonicalName());
    IMetaStoreSchemaInfo metastoreSchemaInfo = MetaStoreSchemaInfoFactory.get(conf);
    Assert.assertNotNull(metastoreSchemaInfo);
    Assert.assertTrue("Unexpected instance type of the class MetaStoreSchemaInfo",
      metastoreSchemaInfo instanceof CDHMetaStoreSchemaInfo);
  }

  @Test
  public void testCdhDefault() {
    IMetaStoreSchemaInfo metastoreSchemaInfo = MetaStoreSchemaInfoFactory.get(conf);
    Assert.assertNotNull(metastoreSchemaInfo);
    Assert.assertTrue("Unexpected instance type of the class MetaStoreSchemaInfo",
      metastoreSchemaInfo instanceof CDHMetaStoreSchemaInfo);
  }
}