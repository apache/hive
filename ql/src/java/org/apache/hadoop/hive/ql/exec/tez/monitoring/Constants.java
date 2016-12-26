package org.apache.hadoop.hive.ql.exec.tez.monitoring;

import org.apache.hadoop.hive.ql.exec.InPlaceUpdates;

public interface Constants {
  int SEPARATOR_WIDTH = InPlaceUpdates.MIN_TERMINAL_WIDTH;
  String SEPARATOR = new String(new char[SEPARATOR_WIDTH]).replace("\0", "-");
}