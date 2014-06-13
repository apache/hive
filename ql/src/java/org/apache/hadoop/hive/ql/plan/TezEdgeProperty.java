package org.apache.hadoop.hive.ql.plan;

import org.apache.hadoop.hive.conf.HiveConf;

public class TezEdgeProperty {

  public enum EdgeType {
    SIMPLE_EDGE,
    BROADCAST_EDGE,
    CONTAINS,
    CUSTOM_EDGE,
    CUSTOM_SIMPLE_EDGE,
  }

  private HiveConf hiveConf;
  private EdgeType edgeType;
  private int numBuckets;

  private boolean isAutoReduce;
  private int minReducer;
  private int maxReducer;
  private long inputSizePerReducer;

  public TezEdgeProperty(HiveConf hiveConf, EdgeType edgeType,
      int buckets) {
    this.hiveConf = hiveConf;
    this.edgeType = edgeType;
    this.numBuckets = buckets;
  }

  public TezEdgeProperty(HiveConf hiveConf, EdgeType edgeType, boolean isAutoReduce,
      int minReducer, int maxReducer, long bytesPerReducer) {
    this(hiveConf, edgeType, -1);
    this.minReducer = minReducer;
    this.maxReducer = maxReducer;
    this.isAutoReduce = isAutoReduce;
    this.inputSizePerReducer = bytesPerReducer;
  }

  public TezEdgeProperty(EdgeType edgeType) {
    this(null, edgeType, -1);
  }

  public EdgeType getEdgeType() {
    return edgeType;
  }

  public HiveConf getHiveConf () {
    return hiveConf;
  }

  public int getNumBuckets() {
    return numBuckets;
  }

  public boolean isAutoReduce() {
    return isAutoReduce;
  }

  public int getMinReducer() {
    return minReducer;
  }

  public int getMaxReducer() {
    return maxReducer;
  }

  public long getInputSizePerReducer() {
    return inputSizePerReducer;
  }
}
