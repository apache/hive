package org.apache.hadoop.hive.ql.dataset;

import org.apache.hadoop.hive.cli.CliDriver;

public interface QTestFeatHandler {

  void processArguments(String arguments);

  // FIXME does it need the argument?
  void beforeTest(CliDriver cliDriver) throws Exception;

}
