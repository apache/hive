package org.apache.hadoop.hive.ql.qoption;

import org.apache.hadoop.hive.ql.QTestUtil;
import org.junit.Assume;

import com.google.common.base.Strings;

public class QTestDisabledHandler implements QTestOptionHandler {

  private String message;

  @Override
  public void processArguments(String arguments) {
    message = arguments;
    if (Strings.isNullOrEmpty(message)) {
      throw new RuntimeException("you have to give a reason why it was ignored");
    }
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
