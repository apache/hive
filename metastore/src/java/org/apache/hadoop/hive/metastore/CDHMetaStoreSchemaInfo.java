package org.apache.hadoop.hive.metastore;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hive.common.util.HiveVersionInfo;

import com.google.common.collect.ImmutableMap;

/**
 * This class defines the Cloudera specific implementation of IMetaStoreSchemaInfo It overrides the
 * default implementation MetaStoreSchemaInfo which is available upstream such that it exposes the
 * CDH specific initialization and upgrade scripts for the the metastore schema. Starting CDH-5.12.0
 * this class is used by HiveSchemaTool to initialize and upgrade metastore schema which can include
 * CDH specific metastore schema patches. The CDH version is recorded in the metastore db in the
 * VERSION table and the hive schema version is fetched from the Hive libraries. Based on these and
 * the upgrade scripts returned by getUpgradeScripts method an upgrade path is determined.
 */
public class CDHMetaStoreSchemaInfo extends MetaStoreSchemaInfo {
  private static String CDH_VERSION_UPGRADE_LIST = "cdh.upgrade.order";

  private static final Log LOG = LogFactory.getLog(CDHMetaStoreSchemaInfo.class.getName());

  public CDHMetaStoreSchemaInfo(String hiveHome, String dbType)
    throws HiveMetaException {
    super(hiveHome, dbType);
  }

  private String[] loadAllCDHUpgradeScripts(String dbType) throws HiveMetaException {
    List<String> cdhUpgradeOrderList = new ArrayList<String>();

    String upgradeListFile =
      getMetaStoreScriptDir() + File.separator + CDH_VERSION_UPGRADE_LIST + "." + dbType;
    if (new File(upgradeListFile).exists()) {
      try (BufferedReader bfReader = new BufferedReader(new FileReader(upgradeListFile))) {
        String line;
        while ((line = bfReader.readLine()) != null) {
          cdhUpgradeOrderList.add(line.trim());
        }
      } catch (IOException e) {
        throw new HiveMetaException("Error reading " + upgradeListFile, e);
      }
    }
    return cdhUpgradeOrderList.toArray(new String[cdhUpgradeOrderList.size()]);
  }

  /***
   * Get the list of sql scripts required to upgrade from the given version to current version
   *
   * @param fromVersion the version of schema (typically from database) from which schematool needs
   *          to upgrade from
   * @return list of script names which need to be run to upgrade the schema from the given from
   *         version
   * @throws HiveMetaException
   */
  @Override
  public List<String> getUpgradeScripts(String fromVersion)
      throws HiveMetaException {
    // the upgrade script list contains the upgrade sqls upto 1.1.0
    // eg. if the fromVersion is 0.14 (CDH-5.4.x) and the to version is 2.1 (CDH-6.0.0)
    // the upgradeScriptList will contain the sqls needed for upgrading from
    // 0.14 to 1.1.0 and then cdhUpgradeScriptList will contain scripts required
    // from CDH-5.12.0 to CDH-6.0.0
    List<String> upgradeScriptList = new ArrayList<>();
    try {
      upgradeScriptList = super.getUpgradeScripts(fromVersion);
    } catch (HiveMetaException ex) {
      //if the fromVersion specified from command line
      //is same as Hive versions super.getUpgradeScripts
      //throw HiveMetaException. We still need to check
      //if there are CDH patches which need to be applied
    }
    List <String> cdhScriptList = getCDHSchemaUpgrades(fromVersion);
    if (cdhScriptList.size() > 0) {
      upgradeScriptList.addAll(cdhScriptList);
    }
    return upgradeScriptList;
  }

  /***
   * Get the name of the script to initialize the schema for given version
   * @param toVersion Target version. If it's null, then the current server version is used
   * @return
   * @throws HiveMetaException
   */
  @Override
  public String generateInitFileName(String toVersion) throws HiveMetaException {
    if (toVersion == null) {
      toVersion = getHiveSchemaVersion();
    }

    toVersion = getMajorVersion(toVersion);
    return super.generateInitFileName(toVersion);
  }

  // format the upgrade script name eg upgrade-x-y-dbType.sql
  private String generateUpgradeFileName(String fileVersion) {
    return UPGRADE_FILE_PREFIX +  fileVersion + "." + dbType + SQL_FILE_EXTENSION;
  }

  private String getMajorVersion(String fullVersion) {
    return fullVersion.split("-")[0];
  }

  @Override
  public String getHiveSchemaVersion() {
    return HiveVersionInfo.getVersion().replaceAll("-SNAPSHOT", "");
  }

  private List<String> getCDHSchemaUpgrades(String from)
      throws HiveMetaException {
    List<String> minorUpgradeList = new ArrayList<String>();
    String cdhVersion = getHiveSchemaVersion();
    if (cdhVersion.equals(from)) {
      return minorUpgradeList;
    }

    // the cdh.upgrade.order file will list all the upgrade paths
    // to reach the current distribution version.
    String[] cdhSchemaVersions = loadAllCDHUpgradeScripts(dbType);
    for (int i=0; i < cdhSchemaVersions.length; i++) {
      if (compareCDHVersions((cdhSchemaVersions[i].split("-to-"))[1].split("-")[1], cdhVersion.split("-")[1]) <= 0) {
        String scriptFile = generateUpgradeFileName(cdhSchemaVersions[i]);
        minorUpgradeList.add(scriptFile);
      } else {
        System.out.println("Upgrade script version is newer than current hive version, skipping file "
                      + cdhSchemaVersions[i]);
      }
    }
    return minorUpgradeList;
  }

  // Compare the 2 version strings based on the version's numerical values.
  // returns a negative, zero or positive integer based on whether version1
  private static int compareCDHVersions(String version1, String version2) {
    System.out.println("Comparing " + version1 + " with " + version2);
    version1 = version1.toLowerCase().replaceAll("cdh","");
    version2 = version2.toLowerCase().replaceAll("cdh","");

    String[] aVersionParts = version1.split("\\.");
    String[] bVersionParts = version2.split("\\.");

    for (int i = 0; i < aVersionParts.length; i++) {
      Integer aVersionPart = Integer.valueOf(aVersionParts[i]);
      Integer bVersionPart = Integer.valueOf(bVersionParts[i]);
      if (aVersionPart > bVersionPart) {
        System.out.println("Version " + aVersionPart + " is higher than " + bVersionPart);
        return 1;
      } else if (aVersionPart < bVersionPart) {
        System.out.println("Version " + aVersionPart + " is lower than " + bVersionPart);
        return -1;
      } else {
        continue; // compare next part
      }
    }
    return 0; // versions are equal
  }

  /**
   * A dbVersion is compatible with hive version if it is greater or equal to
   * the hive version. This is result of the db schema upgrade design principles
   * followed in hive project.
   *
   * @param cdhHiveVersion
   *          version of hive software
   * @param dbVersion
   *         version of metastore rdbms schema
   * @return true if versions are compatible
   */
  @Override
  public boolean isVersionCompatible(String cdhHiveVersion, String dbVersion) {
    // first compare major version
    boolean isCompatible = super.isVersionCompatible(getMajorVersion(cdhHiveVersion),
                             getMajorVersion(dbVersion));

    LOG.info("Upstream versions are compatible, comparing downstream");
    if (!isCompatible)
      return isCompatible;

    String[] cdhFullVersion = cdhHiveVersion.split("-");
    String[] hmsFullVersion = dbVersion.split("-");

    if (cdhFullVersion.length != hmsFullVersion.length) {
      // CDH would be switching to using full version in VERSION table
      // starting CDH5.12.0. so prior versions are deemed incompatible with CDH5.12.0
      return false;
    }

    if (cdhFullVersion.length < 2) {
      // CDH product versions should be of format <hiveversion>-<cdhversion>
      // for ex: 1.1.0-cdh5.12.0
      throw new RuntimeException("Invalid CDH version string " + cdhHiveVersion
        + ". The version string should be of the format <hiveversion>-<cdhversion>");
    }

    return (compareCDHVersions(cdhFullVersion[1], hmsFullVersion[1]) > 0) ? false : true;
  }
}
