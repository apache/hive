package org.apache.hadoop.hive.ql.exec.tez.monitoring;

import org.apache.hadoop.hive.common.log.InPlaceUpdate;

public interface Constants {
  String SEPARATOR = new String(new char[InPlaceUpdate.MIN_TERMINAL_WIDTH]).replace("\0", "-");
}