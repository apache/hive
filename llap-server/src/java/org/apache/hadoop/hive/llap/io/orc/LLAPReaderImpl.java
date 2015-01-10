/**
 *   Copyright 2014 Prasanth Jayachandran
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap.io.orc;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.llap.io.metadata.CompressionBuffer;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcProto;
import org.apache.hadoop.hive.ql.io.orc.ReaderImpl;

/**
 *
 */
public class LLAPReaderImpl extends ReaderImpl implements Reader {

  public LLAPReaderImpl(Path path, OrcFile.ReaderOptions options) throws IOException {
    super(path, options);
  }

  @Override
  public RecordReader rows() throws IOException {
    Reader.Options options = new Options();
    boolean[] include = options.getInclude();
    // if included columns is null, then include all columns
    if (include == null) {
      include = new boolean[footer.getTypesCount()];
      Arrays.fill(include, true);
      options.include(include);
    }
    return new LLAPRecordReaderImpl(this.getStripes(), fileSystem, path,
        options, footer.getTypesList(), codec, bufferSize,
        footer.getRowIndexStride(), conf);
  }

  @Override
  public RecordReader rows(CompressionBuffer buffer, CompressionKind kind,
      OrcProto.ColumnEncoding encoding) throws IOException {
    return null;
  }
}
