package org.apache.hadoop.hive.metastore.dbinstall.rules;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Mariadb extends DatabaseRule {

  @Override
  public String getDockerImageName() {
    return "mariadb:10.2";
  }

  @Override
  public String[] getDockerAdditionalArgs() {
    return buildArray("-p", "3306:3306", "-e", "MYSQL_ROOT_PASSWORD=" + getDbRootPassword(), "-d");
  }

  @Override
  public String getDbType() {
    return "mysql";
  }

  @Override
  public String getDbRootUser() {
    return "root";
  }

  @Override
  public String getDbRootPassword() {
    return "its-a-secret";
  }

  @Override
  public String getJdbcDriver() {
    return "org.mariadb.jdbc.Driver";
  }

  @Override
  public String getJdbcUrl(String hostAddress) {
    return "jdbc:mariadb://" + hostAddress + ":3306/" + HIVE_DB;
  }

  @Override
  public String getInitialJdbcUrl(String hostAddress) {
    return "jdbc:mariadb://" + hostAddress + ":3306/?allowPublicKeyRetrieval=true";
  }

  @Override
  public boolean isContainerReady(ProcessResults pr) {
    Pattern pat = Pattern.compile("ready for connections");
    Matcher m = pat.matcher(pr.stderr);
    return m.find() && m.find();
  }

  @Override
  public String getHivePassword() {
    return HIVE_PASSWORD;
  }
}
