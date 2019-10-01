package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.ql.qoption.QTestOptionHandler;

/**
 * On cdpd-master we don't have dataset support; but we do have the qoption parser
 * so to silently ignore backported qtests which do use datasets this fake handler is used.
 */
public class FakeDatasetHandler implements QTestOptionHandler {

  @Override
  public void processArguments(String arguments) {
  }

  @Override
  public void beforeTest(QTestUtil qt) throws Exception {
  }

  @Override
  public void afterTest(QTestUtil qt) throws Exception {
  }

}
