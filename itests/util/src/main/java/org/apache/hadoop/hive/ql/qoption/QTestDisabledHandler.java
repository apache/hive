package org.apache.hadoop.hive.ql.qoption;

import org.apache.hadoop.hive.ql.QTestUtil;
import org.junit.Assume;

public class QTestDisabledHandler implements QTestOptionHandler {

  private String message;

  @Override
  public void processArguments(String arguments) {
    message = arguments;
  }

  @Override
  public void beforeTest(QTestUtil qt) throws Exception {
    Assume.assumeTrue(message, (message == null));
  }

  @Override
  public void afterTest(QTestUtil qt) throws Exception {
    message = null;
  }

}
