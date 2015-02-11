package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.llap.Consumer;
import org.apache.hadoop.hive.llap.io.api.EncodedColumnBatch;
import org.apache.hadoop.hive.llap.io.api.cache.LowLevelCache;
import org.apache.hadoop.hive.llap.io.api.orc.OrcBatchKey;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.RowIndex;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream;

public interface EncodedReader {
  void readEncodedColumns(int stripeIx, StripeInformation stripe,
      RowIndex[] index, List<ColumnEncoding> encodings, List<Stream> streams,
      boolean[] included, boolean[][] colRgs) throws IOException;

  void close() throws IOException;
}