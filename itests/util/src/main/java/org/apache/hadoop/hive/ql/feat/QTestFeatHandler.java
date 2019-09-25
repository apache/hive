package org.apache.hadoop.hive.ql.feat;

import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.ql.QTestUtil;

public interface QTestFeatHandler {

  void processArguments(String arguments);

  void beforeTest(QTestUtil qt) throws Exception;

}
