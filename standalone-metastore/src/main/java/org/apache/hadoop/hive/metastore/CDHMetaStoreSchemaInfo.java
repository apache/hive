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
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper.MetaStoreConnectionInfo;
import org.apache.hadoop.hive.metastore.utils.MetastoreVersionInfo;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class defines the Cloudera specific implementation of IMetaStoreSchemaInfo It overrides the
 * default implementation MetaStoreSchemaInfo which is available upstream such that it exposes the
 * CDH specific initialization and upgrade scripts for the the metastore schema. Starting 3.1.3000
 * this class is used by HiveSchemaTool to initialize and upgrade metastore schema which can include
 * CDH specific metastore schema patches. The CDH version is recorded in the metastore db in the
 * CDH_VERSION table and the hive schema version is fetched from the Hive libraries. Based on these and
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
    // If current Metastore schema version (dbVersion) is 3.0.0
    // and current HMS server version is 3.1.2000.7.1.0.0
    // upgradeScriptList will contain the sqls needed for upgrading from 3.0.0 to 3.1.2000
    // and then add cdhScriptList which contain scripts required from 3.1.2000 to 3.1.2000.7.1.0.0
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
      removeRedundantScripts(fromVersion, upgradeScriptList, cdhScriptList);
      upgradeScriptList.addAll(cdhScriptList);
    }
    return upgradeScriptList;
  }

  /**
   * Removes the upstream upgrade script files which are already included in the CDH
   * upgrade path
   * @param upgradeScriptList - Current upstream upgrade path scripts
   * @param cdhScriptList - the CDH upgrade path list
   */
  private void removeRedundantScripts(String fromVersion, List<String> upgradeScriptList,
                                      List<String> cdhScriptList) {
    if (cdhScriptList == null || cdhScriptList.isEmpty()) {
      return;
    }
    // given a cdh hive upgrade path, find the starting and ending hive versions
    // of the CDH upgrade script.
    String first = getUpgradePathFromUpgradeFileName(cdhScriptList.get(0));
    String last = getUpgradePathFromUpgradeFileName(cdhScriptList.get(cdhScriptList.size() - 1));

    String firstHiveVersion = getFirstThreeHiveVersion(first.split("-to-")[0]);
    String lastHiveVersion = getFirstThreeHiveVersion(last.split("-to-")[1]);

    // remove all the upgrade paths from upgradeScriptList which
    // are in between firstHiveVersion and lastHiveVersion because they
    // are already taken care of using CDH upgrade scripts
    Iterator<String> it = upgradeScriptList.iterator();
    while(it.hasNext()) {
      String upgradePathFromScript = getUpgradePathFromUpgradeFileName(it.next());
      String fromHiveVersion = upgradePathFromScript.split("-to-")[0];
      String toHiveVersion = upgradePathFromScript.split("-to-")[1];

      if(CDHVersion.compareVersionStrings(fromHiveVersion, firstHiveVersion) >= 0
              && CDHVersion.compareVersionStrings(toHiveVersion, lastHiveVersion) <= 0) {
        it.remove();
      }
    }
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

  /*
   * given a upgrade file name like upgrade-x-to-y.dbtype.sql return x-to-y
   */
  private String getUpgradePathFromUpgradeFileName(String upgradeFileName) {
    //replace the beginning "upgrade-" part
    upgradeFileName = upgradeFileName.replaceAll(UPGRADE_FILE_PREFIX, "");
    //remove the last .sql part
    upgradeFileName = upgradeFileName.substring(0, upgradeFileName.lastIndexOf('.'));
    //remove the last .dbType part
    upgradeFileName = upgradeFileName.substring(0, upgradeFileName.lastIndexOf('.'));
    return upgradeFileName;
  }

  private String getMajorVersion(String fullVersion) {
    // return the Hive version part
    // ex: fullVersion = 3.1.2000 or 3.1.2000.7.1.0.0
    // return 3.1.2000
    return getFirstThreeHiveVersion(fullVersion);
  }

  /**
   * returns the CDH version from the HiveVersionAnnotation.java This annotation is created during
   * the build time. Check saveVersion.sh and common/pom.xml for more details
   *
   * @return CDH version string excluding the SNAPSHOT
   */
  @Override
  public String getHiveSchemaVersion() {
    return MetastoreVersionInfo.getVersion().replaceAll("-SNAPSHOT", "");
  }

  private boolean validateVersion(String version) {
    String v = getFirstThreeHiveVersion(version);
    String schemaFileName =
            getMetaStoreScriptDir() + File.separator + "hive-schema-" + v + "." + dbType + ".sql";
    File schemaFile = new File(schemaFileName);
    if (schemaFile.exists() && schemaFile.isFile()) {
      return true;
    } else {
      return false;
    }
  }

  private List<String> getCDHSchemaUpgrades(String from)
          throws HiveMetaException {
    if (!validateVersion(from)) {
      throw new HiveMetaException("Unknown schema version " + from + " specified, failing upgrade");
    }

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
    if (StringUtils.countMatches(from, ".") >= 6) {
      // If a version contains CDH version, the version will contains 7 numbers and be separated by "."
      // ex: 3.1.2000.7.1.0.0
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
    return isCDHVersionCompatible(new CDHVersion(cdhHiveVersion), new CDHVersion(dbVersion));
  }

  private boolean isCDHVersionCompatible(CDHVersion cdhVersion, CDHVersion dbVersion) {
    // happy path both the versions are same
    if (cdhVersion.equals(dbVersion)) {
      return true;
    }
    // in order to determine if the current CDH version will work with
    // the schema version stored in DB the DB version should be at least
    // equal to the highest schema change version which is less than current cdh version
    // In this context incompatibility means that HMS cannot be booted unless the db version is
    // upgraded to certain version using schemaTool upgradeSchema
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
    boolean needsQuotedIdentifier = HiveSchemaHelper.getDbCommandParser(connectionInfo.getDbType(),
            connectionInfo.getMetaDbType(), false).needsQuotedIdentifier();
    Connection metastoreDbConnection = null;

    try {
      String schema = ( HiveSchemaHelper.DB_HIVE.equals(connectionInfo.getDbType()) ? "SYS" : null );
      metastoreDbConnection = HiveSchemaHelper.getConnectionToMetastore(connectionInfo, schema);
      return getSchemaVersionInternal(metastoreDbConnection, "CDH_VERSION", needsQuotedIdentifier);
    } catch (SQLException ex) {
      try {
        return getSchemaVersionInternal(metastoreDbConnection, "VERSION", needsQuotedIdentifier);
      } catch (SQLException e) {
        throw new HiveMetaException("Failed to get schema version, Cause:" + e.getMessage());
      }
    }
  }

  private String getSchemaVersionInternal(Connection metastoreDbConnection, String table, boolean quoted)
          throws SQLException, HiveMetaException {
    String versionQuery;
    if (quoted) {
      versionQuery = "select * from \"" + table + "\" t";
    } else {
      versionQuery = "select * from " + table + " t";
    }

    try {
      Statement stmt = metastoreDbConnection.createStatement();
      ResultSet res = stmt.executeQuery(versionQuery);
      if (!res.next()) {
        throw new SQLException("Could not find version info in metastore " + table + " table.");
      }
      String version = getSchemaVersion(res);
      if (res.next()) {
        // Multiple version were found is a case shouldn't appear in both CDH_VERSION or VERSION table
        // Need to throw exception other than SQLException
        throw new HiveMetaException("Multiple versions were found in metastore.");
      }
      return version;
    } catch (SQLException e) {
      throw e;
    }
  }

  private String getSchemaVersion(ResultSet res) throws SQLException {
    return getColumnValue(res, "SCHEMA_VERSION");
  }

  protected String getColumnValue(ResultSet res, String columnName) throws SQLException {
    if (res.getMetaData() == null) {
      throw new IllegalArgumentException("ResultSet metadata cannot be null");
    }
    int numCols = res.getMetaData().getColumnCount();
    for (int i = 1; i <= numCols; i++) {
      // When query Hive DB, it will return column name in format: table_name + "." + column_name
      // We need to handle both "cdh_version.ver_id" and "ver_id" cases
      String[] colNameArr = res.getMetaData().getColumnName(i).split("\\.");
      String db_col_name = (colNameArr.length == 2) ? colNameArr[1] : colNameArr[0];
      if (columnName.equalsIgnoreCase(db_col_name)) {
        return res.getString(i);
      }
    }
    return null;
  }

  protected String getFirstThreeHiveVersion(String version) {
    String[] strArray = version.split("\\.", 4);
    StringBuilder sb = new StringBuilder();
    String seperator = "";
    // The version at least consists of 3 numbers
    // ex1: 3.0.1000 (only Hive version)
    // ex2: 3.1.3000.7.1.0.0 (with CDH version)
    try {
      for (int i = 0; i < 3; i++) {
        sb.append(seperator+strArray[i]);
        seperator = ".";
      }
    } catch (RuntimeException e) {
      String error_string = version + " should at least consists of 3 numbers";
      LOG.error(error_string);
      throw new RuntimeException(e + error_string);
    }
    return sb.toString();
  }

  /**
   * Comparable class for CDH Versions
   */
  @VisibleForTesting
  static class CDHVersion implements Comparable<CDHVersion> {
    private final String version;

    /**
     * CDH7 onwards we don't use cdh as prefix nor SNAPSHOT/.x as suffix in version numbers.
     * Instead CDH7 has version 7.0.0.0-<component-suffix-build-number>.
     * For example a full version string look like "3.1.3000.7.1.0.0-585"
     * the method will return the CDH version "7.1.0.0"
     *
     * @return cdh version string
     */
    private String getCdhVersionString() {
      if (StringUtils.countMatches(version, ".") < 6) {
        throw new IllegalArgumentException("Invalid format of cdh version string " + version);
      }
      String[] array = version.split("-")[0].split("\\.", 4);
      return array[array.length - 1];
    }

    public String toString() {
      return version;
    }

    public CDHVersion(String versionStr) {
      if (versionStr == null) {
        throw new IllegalArgumentException("Version cannot be null");
      }
      this.version = versionStr.toLowerCase();
    }

    /**
     * Compares if this with the given CDHVersion and return -1, 0 or 1 based on whether this is
     * lesser, equals or greater than the give CDHVersion respectively.
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

      return compareVersionStrings(aVersionParts, bVersionParts);
    }

    private static int compareVersionStrings(String aVersion, String bVersion) {
      return compareVersionStrings(aVersion.split("\\."), bVersion.split("\\."));
    }

    private static int compareVersionStrings(String[] aVersionParts, String[] bVersionParts) {
      if (aVersionParts.length != bVersionParts.length) {
        // cannot compare if both the version strings are of different format
        throw new IllegalArgumentException(
                "Cannot compare Version strings " + Arrays.toString(aVersionParts) + " and "
                        + Arrays.toString(bVersionParts) + " since follow different format");
      }
      for (int i = 0; i < aVersionParts.length; i++) {
        Integer aVersionPart = Integer.valueOf(aVersionParts[i]);
        Integer bVersionPart = Integer.valueOf(bVersionParts[i]);

        if (aVersionPart > bVersionPart) {
          LOG.debug("Version " + aVersionPart + " is higher than " + bVersionPart);
          return 1;
        } else if (aVersionPart < bVersionPart) {
          LOG.debug("Version " + aVersionPart + " is lower than " + bVersionPart);
          return -1;
        }
      }
      return 0;
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
