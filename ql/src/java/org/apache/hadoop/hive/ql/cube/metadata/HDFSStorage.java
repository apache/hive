package org.apache.hadoop.hive.ql.cube.metadata;

import static org.apache.hadoop.hive.serde.serdeConstants.COLLECTION_DELIM;
import static org.apache.hadoop.hive.serde.serdeConstants.ESCAPE_CHAR;
import static org.apache.hadoop.hive.serde.serdeConstants.FIELD_DELIM;
import static org.apache.hadoop.hive.serde.serdeConstants.LINE_DELIM;
import static org.apache.hadoop.hive.serde.serdeConstants.MAPKEY_DELIM;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

public class HDFSStorage extends Storage {

  private Path tableLocation;
  private Path partLocation;

  private String inputFormat;
  private String outputFormat;
  private boolean isCompressed = true;

  // Delimited row format
  private String fieldDelimiter;
  private String escapeChar;
  private String collectionDelimiter;
  private String lineDelimiter;
  private String mapKeyDelimiter;

  // serde row format
  private String serdeClassName;

  public HDFSStorage(String name, String inputFormat, String outputFormat) {
    this(name, inputFormat, outputFormat, null);
  }

  public HDFSStorage(String name, String inputFormat, String outputFormat,
      Path tableLocation) {
    this(name, inputFormat, outputFormat, null, null, tableLocation, true);
  }

  public HDFSStorage(String name, String inputFormat, String outputFormat,
      String fieldDelimiter, String lineDelimiter, Path tableLocation) {
    this(name, inputFormat, outputFormat, fieldDelimiter, lineDelimiter, null,
        null, null, true, null, null, tableLocation);
  }

  public HDFSStorage(String name, String inputFormat, String outputFormat,
      boolean isCompressed) {
    this(name, inputFormat, outputFormat, null, isCompressed);
  }

  public HDFSStorage(String name, String inputFormat, String outputFormat,
      Path tableLocation, boolean isCompressed) {
    this(name, inputFormat, outputFormat, null, null, tableLocation, isCompressed);
  }

  public HDFSStorage(String name, String inputFormat, String outputFormat,
      String fieldDelimiter, String lineDelimiter, Path tableLocation,
      boolean isCompressed) {
    this(name, inputFormat, outputFormat, fieldDelimiter, lineDelimiter, null, null,
        null, isCompressed, null, null, tableLocation);
  }

  public HDFSStorage(String name, String inputFormat, String outputFormat,
      String fieldDelimiter, String lineDelimiter, String escapeChar,
      String collectionDelimiter, String mapKeyDelimiter, boolean isCompressed,
      Map<String, String> tableParameters, Map<String, String> serdeParameters,
      Path tableLocation) {
    this(name, inputFormat, outputFormat, isCompressed, tableParameters,
        serdeParameters, tableLocation);
    this.fieldDelimiter = fieldDelimiter;
    this.escapeChar = escapeChar;
    this.lineDelimiter = lineDelimiter;
    this.collectionDelimiter = collectionDelimiter;
    this.mapKeyDelimiter = mapKeyDelimiter;
  }

  public HDFSStorage(String name, String inputFormat, String outputFormat,
      String serdeClassName, boolean isCompressed,
      Map<String, String> tableParameters, Map<String, String> serdeParameters,
      Path tableLocation) {
    this(name, inputFormat, outputFormat, isCompressed, tableParameters,
        serdeParameters, tableLocation);
    this.serdeClassName = serdeClassName;
  }

  public HDFSStorage(Table table) {
    super("HDFS", TableType.EXTERNAL_TABLE);
    // TODO
  }

  private HDFSStorage(String name, String inputFormat, String outputFormat,
      boolean isCompressed,
      Map<String, String> tableParameters, Map<String, String> serdeParameters,
      Path tableLocation) {
    super(name, TableType.EXTERNAL_TABLE);
    this.inputFormat = inputFormat;
    this.outputFormat = outputFormat;
    this.isCompressed = isCompressed;
    if (tableParameters != null) {
      addToTableParameters(tableParameters);
    }
    if (serdeParameters != null) {
      this.serdeParameters.putAll(serdeParameters);
    }
    this.tableLocation = tableLocation;
  }

  @Override
  public void setSD(StorageDescriptor sd) {
    if (fieldDelimiter != null) {
      serdeParameters.put(FIELD_DELIM, fieldDelimiter);
      serdeParameters.put(SERIALIZATION_FORMAT, fieldDelimiter);
    }
    if (escapeChar != null) {
      serdeParameters.put(ESCAPE_CHAR, escapeChar);
    }
    if (collectionDelimiter != null) {
      serdeParameters.put(COLLECTION_DELIM, collectionDelimiter);
    }
    if (mapKeyDelimiter != null) {
      serdeParameters.put(MAPKEY_DELIM, mapKeyDelimiter);
    }

    if (serdeParameters != null) {
      serdeParameters.put(LINE_DELIM, lineDelimiter);
    }

    sd.getSerdeInfo().getParameters().putAll(serdeParameters);

    if (outputFormat != null) {
      sd.setOutputFormat(outputFormat);
    }
    if (inputFormat != null) {
      sd.setInputFormat(inputFormat);
    }
    sd.setCompressed(isCompressed);
    if (serdeClassName != null) {
      sd.getSerdeInfo().setSerializationLib(serdeClassName);
    } else {
      sd.getSerdeInfo().setSerializationLib(
          org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
    }
    if (tableLocation != null) {
      sd.setLocation(tableLocation.toString());
    }
  }

  public Path getPartLocation() {
    return partLocation;
  }

  public void setPartLocation(Path partLocation) {
    this.partLocation = partLocation;
  }

  @Override
  public void addPartition(String storageTableName,
      Map<String, String> partSpec, HiveConf conf,
      boolean makeLatest) throws HiveException {
    Hive client = Hive.get(conf);
    Table storageTbl = client.getTable(storageTableName);
    Path location = null;
    if (partLocation != null) {
      if (partLocation.isAbsolute()) {
        location = partLocation;
      } else {
        location = new Path(storageTbl.getPath(), partLocation);
      }
    }
    client.createPartition(storageTbl, partSpec,
        location, getTableParameters(), inputFormat, outputFormat, -1,
        storageTbl.getCols(), serdeClassName, serdeParameters, null, null);
    if (makeLatest) {
      // symlink this partition to latest
      client.createPartition(storageTbl, getLatestPartSpec(),
          location, getTableParameters(), inputFormat, outputFormat, -1,
          storageTbl.getCols(), serdeClassName, serdeParameters, null, null);
    }
  }

  @Override
  public void dropPartition(String storageTableName,
      List<String> partVals, HiveConf conf) throws HiveException {
    Hive.get(conf).dropPartition(storageTableName, partVals, false);
  }

}
