package org.apache.hadoop.hive.metastore;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.CDHMetaStoreSchemaInfo.CDHVersion;
import org.apache.hadoop.hive.metastore.tools.HiveSchemaHelper.MetaStoreConnectionInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class TestCDHMetaStoreSchemaInfo {
  private IMetaStoreSchemaInfo metastoreSchemaInfo;
  private static Configuration conf;

  @BeforeClass
  public static void beforeClass() {
    conf = new HiveConf(TestCDHMetaStoreSchemaInfo.class);
  }

  @Before
  public void setup() {
    metastoreSchemaInfo = MetaStoreSchemaInfoFactory.get(conf,
      System.getProperty("test.tmp.dir", "target/tmp"), "derby");
    Assert.assertNotNull(metastoreSchemaInfo);
    Assert.assertTrue("Unexpected instance of IMetaStoreSchemaInfo",
      metastoreSchemaInfo instanceof CDHMetaStoreSchemaInfo);
  }

  @Test
  public void testGetHiveSchemaVersion() {
    String hiveSchemaVersion = metastoreSchemaInfo.getHiveSchemaVersion();
    Assert.assertTrue("HiveSchema version should contain CDH version",
      hiveSchemaVersion.contains("-cdh"));
  }

  @Test
  public void testUpgradeScripts() throws Exception {
    MetaStoreConnectionInfo mockConnectionInfo = Mockito.mock(MetaStoreConnectionInfo.class);
    Mockito.when(mockConnectionInfo.getDbType()).thenReturn("derby");
    String[] dummyCDHUpgradeOrder =
      new String[] { "1.1.0-to-1.1.0-cdh5.12.0", "1.1.0-cdh5.12.0-to-1.1.0-cdh5.13.0",
        "1.1.0-cdh5.13.0-to-1.1.0-cdh5.15.0", "1.1.0-cdh5.15.0-to-2.1.0-cdh6.0.0" };
    CDHMetaStoreSchemaInfo cdhSchemaInfo = Mockito.mock(CDHMetaStoreSchemaInfo.class);
    Mockito.when(cdhSchemaInfo.loadAllCDHUpgradeScripts(Mockito.anyString()))
      .thenReturn(dummyCDHUpgradeOrder);
    // case 1. when hive version is 1.1.0 and db version is 1.1.0
    // no upgrade is neccessary
    Mockito.when(cdhSchemaInfo.getHiveSchemaVersion()).thenReturn("1.1.0");
    // Mockito
    // .when(cdhSchemaInfo.getMetaStoreSchemaVersion(Mockito.any(MetaStoreConnectionInfo.class)))
    // .thenReturn("1.1.0");
    Mockito.when(cdhSchemaInfo.getUpgradeScripts(Mockito.anyString())).thenCallRealMethod();

    List<String> upgradeOrder = cdhSchemaInfo.getUpgradeScripts("1.1.0");
    Assert.assertTrue(
      "Upgrade scripts should be have been empty when hive version and db version is same",
      upgradeOrder.isEmpty());

    // when hive version is 1.1.0-cdh-5.12.0 and db version is 1.1.0
    Mockito.when(cdhSchemaInfo.getHiveSchemaVersion()).thenReturn("1.1.0-cdh5.12.0");
    upgradeOrder = cdhSchemaInfo.getUpgradeScripts("1.1.0");
    Assert.assertEquals("upgrade order should contain only one script", 1, upgradeOrder.size());
    Assert.assertTrue("Upgrade script should contain upgrade script to CDH5.12.0",
      upgradeOrder.get(0).startsWith("upgrade-1.1.0-to-1.1.0-cdh5.12.0"));

    // when hive version is 1.1.0-cdh-5.13.0 and db version is 1.1.0
    Mockito.when(cdhSchemaInfo.getHiveSchemaVersion()).thenReturn("1.1.0-cdh5.13.0");
    upgradeOrder = cdhSchemaInfo.getUpgradeScripts("1.1.0");
    Assert.assertEquals("upgrade order should contain 2 scripts", 2, upgradeOrder.size());
    Assert.assertTrue("Upgrade script should contain upgrade script to CDH5.12.0",
      upgradeOrder.get(0).startsWith("upgrade-1.1.0-to-1.1.0-cdh5.12.0"));
    Assert.assertTrue("Upgrade script should contain upgrade script to CDH5.13.0",
      upgradeOrder.get(1).startsWith("upgrade-1.1.0-cdh5.12.0-to-1.1.0-cdh5.13.0"));

    // when db version is 1.1.0-cdh5.12.0 and hive version is 1.1.0-cdh5.13.0
    Mockito.when(cdhSchemaInfo.getHiveSchemaVersion()).thenReturn("1.1.0-cdh5.13.0");
    upgradeOrder = cdhSchemaInfo.getUpgradeScripts("1.1.0-cdh5.12.0");
    Assert.assertEquals("upgrade order should contain only one script", 1, upgradeOrder.size());
    Assert.assertTrue("Upgrade script should contain upgrade script to CDH5.12.0",
      upgradeOrder.get(0).startsWith("upgrade-1.1.0-cdh5.12.0-to-1.1.0-cdh5.13.0"));

    // when db version is higher than hive version no upgrade is necessary
    // this can happen if the env is rollbacked/downgraded to earlier version
    // of CDH.
    Mockito.when(cdhSchemaInfo.getHiveSchemaVersion()).thenReturn("1.1.0-cdh5.12.0");
    upgradeOrder = cdhSchemaInfo.getUpgradeScripts("1.1.0-cdh5.13.0");
    Assert.assertEquals("upgrade order should not contain any scripts", 0, upgradeOrder.size());

    //upgrade from cdh5.12 to cdh6.0 which involves a rebase
    Mockito.when(cdhSchemaInfo.getHiveSchemaVersion()).thenReturn("2.1.0-cdh6.0.0");
    upgradeOrder = cdhSchemaInfo.getUpgradeScripts("1.1.0-cdh5.12.0");
    Assert.assertEquals("upgrade order should contain 3 scripts", 3, upgradeOrder.size());
    Assert.assertTrue(upgradeOrder.get(0).startsWith("upgrade-1.1.0-cdh5.12.0-to-1.1.0-cdh5.13.0"));
    Assert.assertTrue(upgradeOrder.get(1).startsWith("upgrade-1.1.0-cdh5.13.0-to-1.1.0-cdh5.15.0"));
    Assert.assertTrue(upgradeOrder.get(2).startsWith("upgrade-1.1.0-cdh5.15.0-to-2.1.0-cdh6.0.0"));

    //case when hive version is not present in upgrade order
    Mockito.when(cdhSchemaInfo.getHiveSchemaVersion()).thenReturn("1.1.0-cdh5.14.0");
    upgradeOrder = cdhSchemaInfo.getUpgradeScripts("1.1.0-cdh5.12.0");
    Assert.assertEquals("upgrade order should contain 1 scripts", 1, upgradeOrder.size());
    Assert.assertTrue(upgradeOrder.get(0).startsWith("upgrade-1.1.0-cdh5.12.0-to-1.1.0-cdh5.13.0"));
  }

  @Test
  public void testSameVersionCompatibility() {
    Assert.assertTrue("Same version string should be always compatible",
      metastoreSchemaInfo.isVersionCompatible("1.1.0-cdh5.12.0", "1.1.0-cdh5.12.0"));
  }

  @Test
  public void testVersionCompatibility() throws HiveMetaException {
    Collection<String> dummySchemaChangeVersions = Arrays.asList(new String[] {
      "1.1.0-cdh5.12.0",
      "1.1.0-cdh5.12.5",
      "1.1.0-cdh5.14.0",
      "2.1.1-cdh6.0.0",
      "2.1.1-cdh6.1.2"
    });
    CDHMetaStoreSchemaInfo cdhSchemaInfo = createMockMetaStoreSchemaInfo(dummySchemaChangeVersions);
    //both the cdh and db versions are before the first schema change version
    Assert.assertTrue(cdhSchemaInfo.isVersionCompatible("1.1.0-cdh5.11.0", "1.1.0-cdh5.11.3"));
    // Schema changes in metastore are backwards compatible. So cdh version less than
    // db version should not break
    Assert.assertTrue(cdhSchemaInfo.isVersionCompatible("1.1.0-cdh5.11.0", "1.1.0-cdh5.12.3"));
    //cdh and db versions both have schema changes from 5.12.0
    Assert.assertTrue(cdhSchemaInfo.isVersionCompatible("1.1.0-cdh5.12.1", "1.1.0-cdh5.12.3"));
    Assert.assertTrue(cdhSchemaInfo.isVersionCompatible("1.1.0-cdh5.12.0", "1.1.0-cdh5.12.4"));
    //db version 5.12.0 does not have the schema changes required by cdh 5.12.5
    Assert.assertFalse(cdhSchemaInfo.isVersionCompatible("1.1.0-cdh5.12.5", "1.1.0-cdh5.12.0"));
    //db version 5.12.6 does not have schema changes required by from 5.14.3
    Assert.assertFalse(cdhSchemaInfo.isVersionCompatible("1.1.0-cdh5.14.3", "1.1.0-cdh5.12.6"));
    //major version difference
    Assert.assertFalse("Major version difference should not be compatible",
      cdhSchemaInfo.isVersionCompatible("2.1.1-cdh6.0.0", "1.1.0-cdh6.0.0"));
    Assert.assertTrue(
      "maintainence release which does not have any schema changes should be compatible",
      cdhSchemaInfo.isVersionCompatible("2.1.1-cdh6.0.0", "2.1.1-cdh6.0.1"));
  }

  @Test
  public void testCDHVersionsWithSchemaChanges() throws Exception {
    Collection<CDHVersion> cdhVersionsWithSchemaChanges =
      ((CDHMetaStoreSchemaInfo) metastoreSchemaInfo).getCDHVersionsWithSchemaChanges();
    Assert.assertNotNull("CDH versions with schema changes should never be null",
      cdhVersionsWithSchemaChanges);
    Assert.assertFalse(
      "As of CDH 5.12.0 there is atleast one schema change so this collection should not be empty",
      cdhVersionsWithSchemaChanges.isEmpty());
  }

  private CDHMetaStoreSchemaInfo createMockMetaStoreSchemaInfo(
    Collection<String> dummySchemaChangeVersions) throws HiveMetaException {
    Collection<CDHVersion> cdhVersions = new TreeSet<>(); 
    for(String version : dummySchemaChangeVersions) {
      cdhVersions.add(new CDHVersion(version));
    }
    CDHMetaStoreSchemaInfo cdhSchemaInfo = Mockito.mock(CDHMetaStoreSchemaInfo.class);
    Mockito.when(cdhSchemaInfo.getCDHVersionsWithSchemaChanges()).thenReturn(cdhVersions);
    Mockito.when(cdhSchemaInfo.isVersionCompatible(Mockito.anyString(), Mockito.anyString()))
      .thenCallRealMethod();
    return cdhSchemaInfo;
  }
}
