package org.apache.hadoop.hive.cli;

import org.apache.hadoop.hive.cli.control.CliAdapter;
import org.apache.hadoop.hive.cli.control.CliConfigs;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.List;

/**
 * This is used only for dev debugging if needed
 */
@RunWith(Parameterized.class)
public class MiniDruidLlapLocalCliDriver {
  static CliAdapter adapter = new CliConfigs.MiniDruidLlapLocalCliConfig().getCliAdapter();

  @Parameterized.Parameters(name = "{0}")
  public static List<Object[]> getParameters() throws Exception {
    return adapter.getParameters();
  }

  @ClassRule
  public static TestRule cliClassRule = adapter.buildClassRule();

  @Rule
  public TestRule cliTestRule = adapter.buildTestRule();

  private String name;
  private File qfile;

  public MiniDruidLlapLocalCliDriver(String name, File qfile) {
    this.name = name;
    this.qfile = qfile;
  }

  @Test
  public void testCliDriver() throws Exception {
    adapter.runTest(name, qfile);
  }

}