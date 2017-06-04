package org.apache.hadoop.hive.metastore;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.tools.HiveSchemaHelper;
import org.apache.hadoop.hive.metastore.tools.HiveSchemaHelper.MetaStoreConnectionInfo;
import org.apache.hive.common.util.HiveVersionInfo;

import com.google.common.annotations.VisibleForTesting;


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

  /**
   * This method returns a collection of CDHVersions which introduce CDH specific schema
   * changes. This is used to determine the compatibility of the running CDH hive version and the
   * version stored in the database. It uses the scripts directory and gets the file names using
   * cdh.upgrade.order.<dbtype> file
   *
   * @return sorted collection of CDHVersions which introduce schema changes
   * @throws HiveMetaException
   */
  @VisibleForTesting
  Collection<CDHVersion> getCDHVersionsWithSchemaChanges() throws HiveMetaException {
    String[] cdhUpgradeScriptNames = loadAllCDHUpgradeScripts(dbType);
    TreeSet<CDHVersion> cdhVersionsWithSchemaChanges = new TreeSet<>();
    for (String cdhUpgradeScriptName : cdhUpgradeScriptNames) {
      String toVersionFromUpgradePath = cdhUpgradeScriptName.split("-to-")[1];
      LOG.debug("Adding " + toVersionFromUpgradePath + " to cdh versions with schema changes");
      cdhVersionsWithSchemaChanges.add(new CDHVersion(toVersionFromUpgradePath));
    }
    return cdhVersionsWithSchemaChanges;
  }

  public CDHMetaStoreSchemaInfo(String hiveHome, String dbType)
    throws HiveMetaException {
    super(hiveHome, dbType);
  }

  @VisibleForTesting
  String[] loadAllCDHUpgradeScripts(String dbType) throws HiveMetaException {
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

  /**
   * returns the CDH version from the HiveVersionAnnotation.java This annotation is created during
   * the build time. Check saveVersion.sh and common/pom.xml for more details
   * 
   * @return CDH version string excluding the SNAPSHOT
   */
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

    CDHVersion currentCdhVersion = new CDHVersion(cdhVersion);
    // the cdh.upgrade.order file will list all the upgrade paths
    // to reach the current distribution version.
    String[] cdhSchemaVersions = loadAllCDHUpgradeScripts(dbType);
    CDHVersion fromCdhVersion = null;
    if (from.indexOf('-') != -1) {
      // from version contains cdh version
      fromCdhVersion = new CDHVersion(from);
    }
    for (int i = 0; i < cdhSchemaVersions.length; i++) {
      // we should skip all the upgrade paths where target is lower than current version
      CDHVersion toVersionFromUpgradePath = new CDHVersion(cdhSchemaVersions[i].split("-to-")[1]);
      if (fromCdhVersion != null
        && fromCdhVersion.compareTo(toVersionFromUpgradePath) >= 0) {
        LOG.info("Current version is higher than or equal to " + toVersionFromUpgradePath
          + " Skipping file " + cdhSchemaVersions[i]);
        continue;
      }
      if (toVersionFromUpgradePath.compareTo(currentCdhVersion) <= 0) {
        String scriptFile = generateUpgradeFileName(cdhSchemaVersions[i]);
        minorUpgradeList.add(scriptFile);
      } else {
        LOG.info("Upgrade script version is newer than current hive version, skipping file "
            + cdhSchemaVersions[i]);
      }
    }
    return minorUpgradeList;
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

    if (!isCompatible) {
      return isCompatible;
    }

    LOG.debug("Upstream versions are compatible, comparing downstream");
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
    return isCDHVersionCompatible(new CDHVersion(cdhHiveVersion), new CDHVersion(dbVersion));
  }

  private boolean isCDHVersionCompatible(CDHVersion cdhVersion, CDHVersion dbVersion) {
    // happy path both the versions are same
    if (cdhVersion.equals(dbVersion)) {
      return true;
    }
    // in order to determine if the current CDH version will work with
    // the schema version stored in DB the DB version should be atleast
    // equal to the highest schema change version which is less than current cdh version
    // In this context incompatibility means that HMS cannot be booted unless the db version is
    // upgraded to certain version using schemaTool upgradeSchema
    // This version is referred to as minimumRequiredSchemaVersion below.
    // eg. if there is schema changes introduced in 5.12.0, 5.12.2 and 5.14.2 like seen in timeline
    // below. In this case any CDH version which < 5.12.0 does not really need the schema changes
    // introduced in 5.12.0. So the minimumRequiredSchemaVersion in this case is null (compatible)
    // So in the example below all CDH versions will work irrespective of what is the DB
    // version.
    // In case of CDH versions >=5.12.0 and <5.12.2 the db version should be atleast 5.12.0
    // In case of CDH versions >=5.12.2 and <5.14.2 the db version should be atleast 5.12.2
    // In case of CDH versions >=5.14.2 the db version should be atleast 5.14.2
    // ---------------------------------------------------------------------------------------------
    // Schema change versions: ----------------------5.12.0------------5.12.2-------------5.14.2----
    // CDH Versions:           ----5.11.0---5.11.3---5.12.0---5.12.1---5.12.2---5.13.0----5.14.2----
    // Min required db version:---------null---------5.12.0------------5.12.2-------------5.14.2----
    CDHVersion minRequiredSchemaVersion = null;
    Collection<CDHVersion> cdhVersionsWithSchemaChanges;
    try {
      cdhVersionsWithSchemaChanges = getCDHVersionsWithSchemaChanges();
    } catch (HiveMetaException e) {
      LOG.error("Unable to load the cdh versions with schema changes ", e);
      throw new RuntimeException(e);
    }
    for (CDHVersion currentCheckPoint : cdhVersionsWithSchemaChanges) {
      if (currentCheckPoint.compareTo(cdhVersion) <= 0) {
        minRequiredSchemaVersion = currentCheckPoint;
      }
    }
    if (minRequiredSchemaVersion != null) {
      return dbVersion.compareTo(minRequiredSchemaVersion) >= 0 ? true : false;
    }
    // there is no minRequiredSchemaVersion so assume compatible
    return true;
  }

  @Override
  public String getMetaStoreSchemaVersion(MetaStoreConnectionInfo connectionInfo)
    throws HiveMetaException {
    String versionQuery;
    boolean needsQuotedIdentifier =
      HiveSchemaHelper.getDbCommandParser(connectionInfo.getDbType()).needsQuotedIdentifier();
    if (needsQuotedIdentifier) {
      versionQuery = "select * from \"VERSION\" t";
    } else {
      versionQuery = "select * from VERSION t";
    }

    try (Connection metastoreDbConnection =
      HiveSchemaHelper.getConnectionToMetastore(connectionInfo)) {
      Statement stmt = metastoreDbConnection.createStatement();
      ResultSet res = stmt.executeQuery(versionQuery);
      if (!res.next()) {
        throw new HiveMetaException("Could not find version info in metastore VERSION table.");
      }
      // get schema_version_v2 if available else fall-back to schema_version
      String version = getSchemaVersion(res);
      if (res.next()) {
        throw new HiveMetaException("Multiple versions were found in metastore.");
      }
      return version;
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to get schema version, Cause:" + e.getMessage());
    }
  }

  private String getSchemaVersion(ResultSet res) throws SQLException {
    String version = getColumnValue(res, "SCHEMA_VERSION_V2");
    if (version == null) {
      version = getColumnValue(res, "SCHEMA_VERSION");
    }
    return version;
  }

  private String getColumnValue(ResultSet res, String columnName) throws SQLException {
    if (res.getMetaData() == null) {
      throw new IllegalArgumentException("ResultSet metadata cannot be null");
    }
    int numCols = res.getMetaData().getColumnCount();
    for (int i = 1; i <= numCols; i++) {
      if (columnName.equalsIgnoreCase(res.getMetaData().getColumnName(i))) {
        return res.getString(i);
      }
    }
    return null;
  }

  /**
   * Comparable class for CDH Versions
   */
  @VisibleForTesting
  static class CDHVersion implements Comparable<CDHVersion> {
    private final String version;
    private final boolean skipMaintainenceRelease;

    /**
     * Given a full version string like 1.1.0-cdh5.12.0 returns the CDH version
     *
     * @param versionString full version string including the hive version
     * @return cdh version string
     */
    private String getCdhVersionString() {
      // versionString is of the format <hiveversion>-<cdhversion> eg: 1.1.0-cdh5.12.0
      String[] parts = version.split("-");
      if (parts.length > 1) {
        return parts[1].replaceAll("cdh", "");
      }
      throw new IllegalArgumentException("Invalid format of cdh version string " + version);
    }

    public CDHVersion(String versionStr) {
      this(versionStr, false);
    }

    public String toString() {
      return version;
    }

    public CDHVersion(String versionStr, boolean skipMaintainenceRelease) {
      if(versionStr == null) {
        throw new IllegalArgumentException("Version cannot be null");
      }
      this.version = versionStr.toLowerCase();
      this.skipMaintainenceRelease = skipMaintainenceRelease;
    }

    /**
     * Compares if this with the given CDHVersion and return -1, 0 or 1 based on whether this is
     * lesser, equals or greater than the give CDHVersion respectively. If skipMaintainenceRelease
     * is true comparison skips the Maintenance release versions for comparison. Maintenance release
     * versions are the last numeric values in the dot separated version string. Eg. 5.12.2, 5.12.0,
     * 5.12.4 will all be equivalent versions if this parameter is set to true
     *
     * @param other CDHVersion to be used for comparison with this
     * @return -1 if this is strictly less than given version, 0 if this is equal and 1 if this is
     *         strictly greater than given version.
     */
    @Override
    public int compareTo(CDHVersion other) {
      LOG.debug("Comparing " + this + " with " + other);
      String cdhVersion1 = getCdhVersionString();
      String cdhVersion2 = other.getCdhVersionString();

      String[] aVersionParts = cdhVersion1.split("\\.");
      String[] bVersionParts = cdhVersion2.split("\\.");

      if (aVersionParts.length != bVersionParts.length) {
        // cannot compare if both the version strings are of different format
        throw new IllegalArgumentException("Cannot compare Version strings " + cdhVersion1 + " and "
          + cdhVersion2 + " since follow different format");
      }
      for (int i = 0; i < aVersionParts.length; i++) {
        Integer aVersionPart = Integer.valueOf(aVersionParts[i]);
        Integer bVersionPart = Integer.valueOf(bVersionParts[i]);
        if (this.skipMaintainenceRelease && i == aVersionParts.length - 1) {
          continue;
        }
        if (aVersionPart > bVersionPart) {
          LOG.debug("Version " + aVersionPart + " is higher than " + bVersionPart);
          return 1;
        } else if (aVersionPart < bVersionPart) {
          LOG.debug("Version " + aVersionPart + " is lower than " + bVersionPart);
          return -1;
        }
      }
      return 0; // versions are equal
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((version == null) ? 0 : version.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      CDHVersion other = (CDHVersion) obj;
      if (version == null) {
        if (other.version != null)
          return false;
      } else if (!version.equals(other.version))
        return false;
      return true;
    }
  }
}
