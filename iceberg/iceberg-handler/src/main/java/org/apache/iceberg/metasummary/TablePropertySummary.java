/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.metasummary;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.metasummary.MetadataTableSummary;
import org.apache.hadoop.hive.metastore.metasummary.SummaryMapBuilder;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Collect the summary based on the table's properties.
 * It could be the version, write mode or compression, etc.
 */
public class TablePropertySummary extends IcebergSummaryRetriever {
  private static final String VERSION = "version";
  private static final String WRITE_WAP_PROPERTY = "write.wap.enabled";
  private List<PropertyRetriever> retrievers;

  @Override
  public void initialize(Configuration conf, boolean formatJson) {
    super.initialize(conf, formatJson);
    this.retrievers = Arrays.asList(
        new WriteFormatSummary(),
        new DistributionModeSummary(),
        new UpdateModeSummary());
  }

  @Override
  public List<String> getFieldNames() {
    List<String> fields = Lists.newArrayList();
    fields.add(VERSION);
    retrievers.forEach(propertyRetriever -> fields.addAll(propertyRetriever.getFieldNames()));
    fields.add(WRITE_WAP_PROPERTY);
    return fields;
  }

  @Override
  public void getMetaSummary(Table table, MetadataTableSummary summary) {
    Map<String, String> properties = table.properties();
    retrievers.forEach(propertyRetriever ->
        propertyRetriever.getMetaSummary(properties, summary));
    TableMetadata metadata = ((BaseTable) table).operations().current();
    assert metadata != null;
    int version = metadata.formatVersion();
    String wapEnabled = properties.getOrDefault(WRITE_WAP_PROPERTY, "false");
    summary.addExtra(new SummaryMapBuilder().add(VERSION, version).add(WRITE_WAP_PROPERTY, wapEnabled));
  }

  public class UpdateModeSummary implements PropertyRetriever {
    private static final String WRITE_MERGE_MODE = "write.merge.mode";
    private static final String WRITE_DELETE_MODE = "write.delete.mode";
    private static final String WRITE_UPDATE_MODE = "write.update.mode";
    private static final String COPY_ON_WRITE = "copy-on-write";

    @Override
    public List<String> getFieldNames() {
      if (formatJson) {
        return Collections.singletonList("CoW/MoR");
      }
      return Arrays.asList(WRITE_MERGE_MODE, WRITE_DELETE_MODE, WRITE_UPDATE_MODE);
    }

    @Override
    public void getMetaSummary(Map<String, String> properties, MetadataTableSummary summary) {
      SummaryMapBuilder builder = new SummaryMapBuilder()
          .add(WRITE_MERGE_MODE, properties.getOrDefault(WRITE_MERGE_MODE, COPY_ON_WRITE))
          .add(WRITE_DELETE_MODE, properties.getOrDefault(WRITE_DELETE_MODE, COPY_ON_WRITE))
          .add(WRITE_UPDATE_MODE, properties.getOrDefault(WRITE_UPDATE_MODE, COPY_ON_WRITE));
      if (formatJson) {
        summary.addExtra("CoW/MoR", builder);
      } else {
        summary.addExtra(builder);
      }
    }
  }

  public class WriteFormatSummary implements PropertyRetriever {
    private static final String WRITE_FORMAT_DEFAULT = "write.format.default";
    private static final String WRITE_DELETE_FORMAT_DEFAULT = "write.delete.format.default";
    private static final String WRITE_COMPRESSION_CODEC = "write.compression-codec";
    private static final String WRITE_FORMAT = "writeFormat";
    private static final String PARQUET = "parquet";
    private final Map<String, String> defaultCompressionCodec;

    public WriteFormatSummary() {
      defaultCompressionCodec = Maps.newHashMap();
      defaultCompressionCodec.put(PARQUET, "zstd");
      defaultCompressionCodec.put("orc", "zlib");
      defaultCompressionCodec.put("avro", "gzip");
    }

    @Override
    public List<String> getFieldNames() {
      if (formatJson) {
        return Collections.singletonList(WRITE_FORMAT);
      }
      return Arrays.asList(WRITE_FORMAT_DEFAULT, WRITE_DELETE_FORMAT_DEFAULT, WRITE_COMPRESSION_CODEC);
    }

    @Override
    public void getMetaSummary(Map<String, String> properties, MetadataTableSummary summary) {
      SummaryMapBuilder builder = new SummaryMapBuilder()
          .add(WRITE_FORMAT_DEFAULT, properties.getOrDefault(WRITE_FORMAT_DEFAULT, PARQUET))
          .add(WRITE_DELETE_FORMAT_DEFAULT, properties.getOrDefault(WRITE_DELETE_FORMAT_DEFAULT, PARQUET));
      String fileFormat = builder.get(WRITE_FORMAT_DEFAULT, String.class);
      String compression = "write." + fileFormat + ".compression-codec";
      builder.add(WRITE_COMPRESSION_CODEC,
          properties.getOrDefault(compression, defaultCompressionCodec.get(fileFormat)));
      if (formatJson) {
        summary.addExtra(WRITE_FORMAT, builder);
      } else {
        summary.addExtra(builder);
      }
      summary.setCompressionType(builder.get(WRITE_COMPRESSION_CODEC, String.class));
      summary.setFileFormat(fileFormat);
    }
  }

  public class DistributionModeSummary implements PropertyRetriever {
    private static final String WRITE_DISTRIBUTION_MODE = "write.distribution-mode";
    private static final String WRITE_UPDATE_DISTRIBUTION_MODE = "write.update.distribution-mode";
    private static final String WRITE_DELETE_DISTRIBUTION_MODE = "write.delete.distribution-mode";
    private static final String WRITE_MERGE_DISTRIBUTION_MODE = "write.merge.distribution-mode";
    private static final String HASH_MODE = "hash";
    private static final String NONE = "none";

    @Override
    public List<String> getFieldNames() {
      if (formatJson) {
        return Collections.singletonList("distribution-mode");
      }
      return Arrays.asList(WRITE_DISTRIBUTION_MODE, WRITE_UPDATE_DISTRIBUTION_MODE,
          WRITE_DELETE_DISTRIBUTION_MODE, WRITE_MERGE_DISTRIBUTION_MODE);
    }

    @Override
    public void getMetaSummary(Map<String, String> properties, MetadataTableSummary summary) {
      SummaryMapBuilder builder = new SummaryMapBuilder()
          .add(WRITE_DISTRIBUTION_MODE, properties.getOrDefault(WRITE_DISTRIBUTION_MODE, NONE))
          .add(WRITE_UPDATE_DISTRIBUTION_MODE, properties.getOrDefault(WRITE_UPDATE_DISTRIBUTION_MODE, HASH_MODE))
          .add(WRITE_DELETE_DISTRIBUTION_MODE, properties.getOrDefault(WRITE_DELETE_DISTRIBUTION_MODE, HASH_MODE))
          .add(WRITE_MERGE_DISTRIBUTION_MODE, properties.getOrDefault(WRITE_MERGE_DISTRIBUTION_MODE, NONE));
      if (formatJson) {
        summary.addExtra("distribution-mode", builder);
      } else {
        summary.addExtra(builder);
      }
    }
  }

  public interface PropertyRetriever {
    default List<String> getFieldNames() {
      return Collections.emptyList();
    }
    void getMetaSummary(Map<String, String> properties, MetadataTableSummary summary);
  }
}
