package org.apache.hadoop.hive.hwi;

/**
 * HWIException.
 *
 */
public class HWIException extends Exception {

  private static final long serialVersionUID = 1L;

  public HWIException() {
    super();
  }

  /** Specify an error String with the Exception. */
  public HWIException(String arg0) {
    super(arg0);
  }

  /** Wrap an Exception in HWIException. */
  public HWIException(Throwable arg0) {
    super(arg0);
  }

  /** Specify an error String and wrap an Exception in HWIException. */
  public HWIException(String arg0, Throwable arg1) {
    super(arg0, arg1);
  }

}
