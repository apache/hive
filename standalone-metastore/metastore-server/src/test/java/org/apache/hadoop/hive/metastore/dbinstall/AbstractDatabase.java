package org.apache.hadoop.hive.metastore.dbinstall;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.tools.schematool.MetastoreSchemaTool;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sqlline.SqlLine;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class AbstractDatabase extends ExternalResource {

  private final Logger LOG = LoggerFactory.getLogger(AbstractDatabase.class);

  private final int MAX_STARTUP_WAIT = 5 * 60 * 1000;
  private final String HIVE_USER = "hiveuser";
  private final String HIVE_PASSWORD = "hivepassword";

  private String db = "hivedb";
  private boolean verbose = System.getProperty("verbose.schematool") != null;
  protected boolean useDockerDatabaseArg = false;

  public AbstractDatabase setVerbose(boolean verbose) {
    this.verbose = verbose;
    return this;
  }

  public AbstractDatabase setUseDockerDatabaseArg(boolean useDockerDatabaseArg) {
    this.useDockerDatabaseArg = useDockerDatabaseArg;
    return this;
  }

  public AbstractDatabase setDb(String db) {
    this.db = db;
    return this;
  }

  protected abstract String getDockerImageName();
  protected abstract List<String> getDockerBaseArgs();
  protected abstract String getDbType();
  protected abstract String getDbRootUser();
  protected abstract String getDbRootPassword();
  public abstract String getJdbcDriver();
  protected abstract String getJdbcUrl(String hostAddress);
  protected abstract boolean isContainerReady(ProcessResults pr);

  public String getJdbcUrl() {
    return getJdbcUrl(getContainerHostAddress());
  }

  protected String getDockerDatabaseArg() {
    return null;
  }

  /**
   * URL to use when connecting as root rather than Hive
   *
   * @return URL
   */
  public abstract String getInitialJdbcUrl(String hostAddress);

  public final String getInitialJdbcUrl() {
    return getInitialJdbcUrl(getContainerHostAddress());
  }

  public String getHiveUser(){
    return HIVE_USER;
  }

  public String getHivePassword(){
    return HIVE_PASSWORD;
  }

  protected String getDb() {
    return db;
  }

  private String getDockerContainerName() {
    return String.format("testDb-%s", getClass().getSimpleName());
  }

  private List<String> getDockerAdditionalArgs() {
    List<String> dockerArgs = getDockerBaseArgs();
    if (useDockerDatabaseArg && StringUtils.isNotEmpty(getDockerDatabaseArg())) {
      dockerArgs.addAll(Arrays.asList("-e", getDockerDatabaseArg()));
    }
    return dockerArgs;
  }

  private String[] buildRunCmd() {
    List<String> cmd =  new ArrayList(
        Arrays.asList("docker", "run", "--rm", "--name"));
    cmd.add(getDockerContainerName());
    cmd.addAll(getDockerAdditionalArgs());
    cmd.add(getDockerImageName());
    return cmd.toArray(new String[cmd.size()]);
  }

  private String getContainerHostAddress() {
    String hostAddress = System.getenv("HIVE_TEST_DOCKER_HOST");
    return hostAddress != null ? hostAddress : "localhost";
  }

  private String[] buildRmCmd() {
    return new String[] { "docker", "rm", "-f", "-v", getDockerContainerName() };
  }

  private String[] buildLogCmd() {
    return new String[] { "docker", "logs", getDockerContainerName() };
  }

  private int runCmdAndPrintStreams(String[] cmd, long secondsToWait)
      throws InterruptedException, IOException {
    ProcessResults results = runCmd(cmd, secondsToWait);
    LOG.info("Stdout from proc: " + results.stdout);
    LOG.info("Stderr from proc: " + results.stderr);
    return results.rc;
  }

  private ProcessResults runCmd(String[] cmd, long secondsToWait)
      throws IOException, InterruptedException {
    LOG.info("Going to run: " + StringUtils.join(cmd, " "));
    Process proc = Runtime.getRuntime().exec(cmd);
    if (!proc.waitFor(secondsToWait, TimeUnit.SECONDS)) {
      throw new RuntimeException(
          "Process " + cmd[0] + " failed to run in " + secondsToWait + " seconds");
    }
    BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
    final StringBuilder lines = new StringBuilder();
    reader.lines().forEach(s -> lines.append(s).append('\n'));

    reader = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
    final StringBuilder errLines = new StringBuilder();
    reader.lines().forEach(s -> errLines.append(s).append('\n'));
    LOG.info("Result size: " + lines.length() + ";" + errLines.length());
    return new ProcessResults(lines.toString(), errLines.toString(), proc.exitValue());
  }

  public void launchDockerContainer() throws Exception {
    runCmdAndPrintStreams(buildRmCmd(), 600);
    if (runCmdAndPrintStreams(buildRunCmd(), 600) != 0) {
      throw new RuntimeException("Unable to start docker container");
    }
    long startTime = System.currentTimeMillis();
    ProcessResults pr;
    do {
      Thread.sleep(1000);
      pr = runCmd(buildLogCmd(), 30);
      if (pr.rc != 0) {
        throw new RuntimeException("Failed to get docker logs");
      }
    } while (startTime + MAX_STARTUP_WAIT >= System.currentTimeMillis() && !isContainerReady(pr));
    if (startTime + MAX_STARTUP_WAIT < System.currentTimeMillis()) {
      throw new RuntimeException("Container failed to be ready in " + MAX_STARTUP_WAIT/1000 +
          " seconds");
    }
  }

  public void cleanupDockerContainer() throws IOException, InterruptedException {
    if (runCmdAndPrintStreams(buildRmCmd(), 600) != 0) {
      throw new RuntimeException("Unable to remove docker container");
    }
  }

  @Override
  public void before() throws Exception {
    launchDockerContainer();
    MetastoreSchemaTool.setHomeDirForTesting();
  }

  @Override
  public void after() {
    if ("true".equalsIgnoreCase(System.getProperty("metastore.itest.no.stop.container"))) {
      LOG.warn("Not stopping container " + getDockerContainerName() + " at user request, please "
          + "be sure to shut it down before rerunning the test.");
      return;
    }
    try {
      cleanupDockerContainer();
    } catch (InterruptedException | IOException e) {
      e.printStackTrace();
    }
  }

  public void install() {
    createUser();
    installLatest();
  }

  public int createUser() {
    return new MetastoreSchemaTool().setVerbose(verbose).run(new String[] {
        "-createUser",
        "-dbType", getDbType(),
        "-userName", getDbRootUser(),
        "-passWord", getDbRootPassword(),
        "-hiveUser", getHiveUser(),
        "-hivePassword", getHivePassword(),
        "-hiveDb", getDb(),
        "-url", getInitialJdbcUrl(),
        "-driver", getJdbcDriver()
    });
  }

  public int installLatest() {
    return new MetastoreSchemaTool().setVerbose(verbose).run(new String[] {
        "-initSchema",
        "-dbType", getDbType(),
        "-userName", getHiveUser(),
        "-passWord", getHivePassword(),
        "-url", getJdbcUrl(),
        "-driver", getJdbcDriver(),
        "-verbose"
    });
  }

  public int installAVersion(String version) {
    return new MetastoreSchemaTool().setVerbose(verbose).run(new String[] {
        "-initSchemaTo", version,
        "-dbType", getDbType(),
        "-userName", getHiveUser(),
        "-passWord", getHivePassword(),
        "-url", getJdbcUrl(),
        "-driver", getJdbcDriver()
    });
  }

  public int upgradeToLatest() {
    return new MetastoreSchemaTool().setVerbose(verbose).run(new String[] {
        "-upgradeSchema",
        "-dbType", getDbType(),
        "-userName", getHiveUser(),
        "-passWord", getHivePassword(),
        "-url", getJdbcUrl(),
        "-driver", getJdbcDriver()
    });
  }

  public int validateSchema() {
    return new MetastoreSchemaTool().setVerbose(verbose).run(new String[] {
        "-validate",
        "-dbType", getDbType(),
        "-userName", getHiveUser(),
        "-passWord", getHivePassword(),
        "-url", getJdbcUrl(),
        "-driver", getJdbcDriver()
    });
  }

  private String[] SQLLineCmdBuild(String sqlScriptFile) {
    return new String[] {"-u", getJdbcUrl(),
        "-d", getJdbcDriver(),
        "-n", getDbRootUser(),
        "-p", getDbRootPassword(),
        "--isolation=TRANSACTION_READ_COMMITTED",
        "-f", sqlScriptFile};
  }

  public void execute(String script) throws IOException, SQLException, ClassNotFoundException {
    // Test we can connect to database
    Class.forName(getJdbcDriver());
    try (Connection ignored = DriverManager.getConnection(getJdbcUrl(), getDbRootUser(), getDbRootPassword())) {
      LOG.info("Successfully connected to {} with user {} and password {}", getJdbcUrl(), getDbRootUser(), getDbRootPassword());
    }
    LOG.info("Starting {} initialization", getClass().getSimpleName());
    SqlLine sqlLine = new SqlLine();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    sqlLine.setOutputStream(new PrintStream(out));
    sqlLine.setErrorStream(new PrintStream(out));
    System.setProperty("sqlline.silent", "true");
    SqlLine.Status status = sqlLine.begin(SQLLineCmdBuild(script), null, false);
    LOG.debug("Printing output from SQLLine:");
    LOG.debug(out.toString());
    if (status != SqlLine.Status.OK) {
      throw new RuntimeException("Database script " + script + " failed with status " + status);
    }
    LOG.info("Completed {} initialization", getClass().getSimpleName());
  }

  protected class ProcessResults {
    final public String stdout;
    final public String stderr;
    final public int rc;

    public ProcessResults(String stdout, String stderr, int rc) {
      this.stdout = stdout;
      this.stderr = stderr;
      this.rc = rc;
    }
  }
}
