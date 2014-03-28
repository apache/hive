package org.apache.hadoop.hive.ql.plan;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty.EdgeType;

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

  public TezEdgeProperty(HiveConf hiveConf, EdgeType edgeType, 
      int buckets) {
    this.hiveConf = hiveConf;
    this.edgeType = edgeType;
    this.numBuckets = buckets;
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
}
