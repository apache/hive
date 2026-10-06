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

package org.apache.iceberg.data;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.Pair;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

public class FileHelpers {
  private FileHelpers() {
  }

  public static Pair<DeleteFile, CharSequenceSet> writeDeleteFile(Table table, OutputFile out,
                                                                    List<Pair<CharSequence, Long>> deletes)
      throws IOException {
    return writeDeleteFile(table, out, null, deletes);
  }

  public static Pair<DeleteFile, CharSequenceSet> writeDeleteFile(Table table, OutputFile out, StructLike partition,
                                                                    List<Pair<CharSequence, Long>> deletes)
      throws IOException {
    FileFormat format = defaultFormat(table.properties());
    FileWriterFactory<Record> factory = new GenericFileWriterFactory.Builder(table).dataFileFormat(format).build();

    PositionDeleteWriter<Record> writer =
        factory.newPositionDeleteWriter(encrypt(out), table.spec(), partition);
    PositionDelete<Record> posDelete = PositionDelete.create();
    try (writer) {
      for (Pair<CharSequence, Long> delete : deletes) {
        posDelete.set(delete.first(), delete.second());
        writer.write(posDelete);
      }
    }

    return Pair.of(writer.toDeleteFile(), writer.referencedDataFiles());
  }

  public static DeleteFile writeDeleteFile(Table table, OutputFile out, List<Record> deletes, Schema deleteRowSchema)
      throws IOException {
    return writeDeleteFile(table, out, null, deletes, deleteRowSchema);
  }

  public static DeleteFile writeDeleteFile(Table table, OutputFile out, StructLike partition,
                                           List<Record> deletes, Schema deleteRowSchema)
      throws IOException {
    FileFormat format = defaultFormat(table.properties());
    int[] equalityFieldIds = deleteRowSchema.columns().stream().mapToInt(Types.NestedField::fieldId).toArray();
    FileWriterFactory<Record> factory = new GenericFileWriterFactory.Builder(table).equalityFieldIds(equalityFieldIds)
        .deleteFileFormat(format).equalityDeleteRowSchema(deleteRowSchema)
        .build();

    EqualityDeleteWriter<Record> writer = factory.newEqualityDeleteWriter(encrypt(out), table.spec(), partition);
    try (writer) {
      writer.write(deletes);
    }

    return writer.toDeleteFile();
  }

  public static Pair<DeleteFile, CharSequenceSet> writeDeleteFile(
          Table table,
          OutputFile out,
          StructLike partition,
          List<Pair<CharSequence, Long>> deletes,
          int formatVersion)
          throws IOException {
    if (formatVersion >= 3) {
      OutputFileFactory fileFactory =
              OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();
      DVFileWriter writer = new BaseDVFileWriter(fileFactory, p -> null);
      try (writer) {
        for (Pair<CharSequence, Long> delete : deletes) {
          writer.delete(
                  delete.first().toString(), delete.second(), table.spec(), partition);
        }
      }

      return Pair.of(
              Iterables.getOnlyElement(writer.result().deleteFiles()),
              writer.result().referencedDataFiles());
    } else {
      FileFormat format = defaultFormat(table.properties());
      FileWriterFactory<Record> factory = new GenericFileWriterFactory.Builder(table).deleteFileFormat(format).build();

      PositionDeleteWriter<Record> writer =
              factory.newPositionDeleteWriter(encrypt(out), table.spec(), partition);
      PositionDelete<Record> posDelete = PositionDelete.create();
      try (writer) {
        for (Pair<CharSequence, Long> delete : deletes) {
          posDelete.set(delete.first(), delete.second());
          writer.write(posDelete);
        }
      }

      return Pair.of(writer.toDeleteFile(), writer.referencedDataFiles());
    }
  }

  public static DataFile writeDataFile(Table table, OutputFile out, List<Record> rows) throws IOException {
    FileFormat format = defaultFormat(table.properties());
    GenericFileWriterFactory factory = new GenericFileWriterFactory.Builder(table).dataFileFormat(format).build();

    DataWriter<Record> writer = factory.newDataWriter(encrypt(out), table.spec(), null);
    try (writer) {
      for (Record row : rows) {
        writer.write(row);
      }
    }

    return writer.toDataFile();

  }

  public static DataFile writeDataFile(Table table, OutputFile out, StructLike partition, List<Record> rows)
      throws IOException {
    FileFormat format = defaultFormat(table.properties());
    GenericFileWriterFactory factory = new GenericFileWriterFactory.Builder(table).dataFileFormat(format).build();

    DataWriter<Record> writer = factory.newDataWriter(encrypt(out), table.spec(), partition);
    try (writer) {
      for (Record row : rows) {
        writer.write(row);
      }
    }

    return writer.toDataFile();

  }

  public static DeleteFile writePosDeleteFile(
          Table table, OutputFile out, StructLike partition, List<PositionDelete<?>> deletes)
          throws IOException {
    return writePosDeleteFile(table, out, partition, deletes, 2);
  }

  public static DeleteFile writePosDeleteFile(
          Table table,
          OutputFile out,
          StructLike partition,
          List<PositionDelete<?>> deletes,
          int formatVersion)
          throws IOException {
    if (formatVersion >= 3) {
      OutputFileFactory fileFactory =
              OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();
      DVFileWriter writer = new BaseDVFileWriter(fileFactory, p -> null);
      try (writer) {
        for (PositionDelete<?> delete : deletes) {
          writer.delete(delete.path().toString(), delete.pos(), table.spec(), partition);
        }
      }

      return Iterables.getOnlyElement(writer.result().deleteFiles());
    } else {
      FileWriterFactory<Record> factory =
              new GenericFileWriterFactory.Builder(table)
                      .build();

      PositionDeleteWriter<?> writer =
              factory.newPositionDeleteWriter(encrypt(out), table.spec(), partition);
      try (writer) {
        for (PositionDelete delete : deletes) {
          writer.write(delete);
        }
      }

      return writer.toDeleteFile();
    }
  }

  private static EncryptedOutputFile encrypt(OutputFile out) {
    return EncryptedFiles.encryptedOutput(out, EncryptionKeyMetadata.EMPTY);
  }

  private static FileFormat defaultFormat(Map<String, String> properties) {
    String formatString = properties.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
    return FileFormat.fromString(formatString);
  }
}
