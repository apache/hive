package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.ql.io.orc.EncodedReaderImpl.OrcEncodedColumnBatch;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.RowIndex;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream;
import org.apache.hadoop.hive.ql.io.orc.llap.Consumer;

public interface EncodedReader {
  // TODO: document
  void readEncodedColumns(int stripeIx, StripeInformation stripe,
      RowIndex[] index, List<ColumnEncoding> encodings, List<Stream> streams,
      boolean[] included, boolean[][] colRgs,
      Consumer<OrcEncodedColumnBatch> consumer) throws IOException;

  void close() throws IOException;

  void setDebugTracing(boolean isEnabled);
}