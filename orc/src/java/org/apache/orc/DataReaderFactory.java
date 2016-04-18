package org.apache.orc;

import org.apache.orc.impl.DataReaderProperties;

public interface DataReaderFactory {

  DataReader create(DataReaderProperties properties);

}
