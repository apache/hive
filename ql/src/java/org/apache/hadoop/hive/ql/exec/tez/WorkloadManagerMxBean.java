package org.apache.hadoop.hive.ql.exec.tez;

import java.util.List;

import javax.management.MXBean;

/**
 * MXbean to expose cache allocator related information through JMX.
 */
@MXBean
public interface WorkloadManagerMxBean {
  /**
   * @return The text-based description of current WM state.
   */
  public List<String> getWmStateDescription();
}

