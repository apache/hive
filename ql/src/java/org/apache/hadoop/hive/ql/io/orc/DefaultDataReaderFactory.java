package org.apache.hadoop.hive.ql.io.orc;

import org.apache.orc.DataReader;
import org.apache.orc.DataReaderFactory;
import org.apache.orc.impl.DataReaderProperties;

public final class DefaultDataReaderFactory implements DataReaderFactory {

  @Override
  public DataReader create(DataReaderProperties properties) {
    return RecordReaderUtils.createDefaultDataReader(properties);
  }

}
