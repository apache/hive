package org.apache.hive.druid;

import java.io.Closeable;
import java.io.IOException;

public abstract class DruidNode implements Closeable{

  private final String nodeType;

  public DruidNode(String nodeId) {this.nodeType = nodeId;}

  final public String getNodeType() {
    return nodeType;
  }

  /**
   * starts the druid node
   */
  public abstract void start() throws IOException;

  /**
   * @return true if the process is working
   */
  public abstract boolean isAlive();

}
