/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.parquet;

import java.io.IOException;
import org.apache.hadoop.hive.common.io.encoded.MemoryBufferOrBuffers;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.encryption.NativeEncryptionInputFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

public class HiveParquetUtil {

  private HiveParquetUtil() {
  }

  public static ParquetMetadata readFooter(DataFile dataFile, FileIO io, JobConf job, MemoryBufferOrBuffers footerData)
      throws IOException {

    InputFile icebergInputFile = io.newInputFile(dataFile);
    InputFile rawFileToRead = icebergInputFile;

    ParquetReadOptions.Builder optionsBuilder =
        HadoopReadOptions.builder(job).withMetadataFilter(ParquetMetadataConverter.NO_FILTER);

    if (icebergInputFile instanceof NativeEncryptionInputFile nativeEncFile) {
      rawFileToRead = nativeEncFile.encryptedInputFile();

      byte[] footerKey = ByteBuffers.toByteArray(nativeEncFile.keyMetadata().encryptionKey());
      byte[] aadPrefix = ByteBuffers.toByteArray(nativeEncFile.keyMetadata().aadPrefix());

      FileDecryptionProperties decryptProps =
          FileDecryptionProperties.builder().withFooterKey(footerKey).withAADPrefix(aadPrefix).build();

      optionsBuilder.withDecryption(decryptProps);
    }

    org.apache.parquet.io.InputFile parquetInputFile;
    if (footerData != null) {
      byte[] magic = (icebergInputFile instanceof NativeEncryptionInputFile) ?
          ParquetFileWriter.EFMAGIC : ParquetFileWriter.MAGIC;
      parquetInputFile = new ParquetFooterInputFromCache(footerData, magic);
    } else {
      parquetInputFile = ParquetIO.file(rawFileToRead);
    }

    try (ParquetFileReader reader = ParquetFileReader.open(parquetInputFile, optionsBuilder.build())) {
      return reader.getFooter();
    }
  }
}
