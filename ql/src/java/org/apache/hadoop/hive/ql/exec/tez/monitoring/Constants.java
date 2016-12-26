package org.apache.hadoop.hive.ql.exec.tez.monitoring;

import org.apache.hadoop.hive.ql.exec.InPlaceUpdates;

public interface Constants {
  String SEPARATOR = new String(new char[InPlaceUpdates.MIN_TERMINAL_WIDTH]).replace("\0", "-");
}