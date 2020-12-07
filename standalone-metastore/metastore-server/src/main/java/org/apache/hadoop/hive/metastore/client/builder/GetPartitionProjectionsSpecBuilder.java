package org.apache.hadoop.hive.metastore.client.builder;

import org.apache.hadoop.hive.metastore.api.GetProjectionsSpec;

import java.util.List;

/**
 * Builder for the GetProjectionsSpec. This is a projection specification for partitions returned from the HMS.
 */
public class GetPartitionProjectionsSpecBuilder {

    private List<String> partitionList = null;
    private String includePartitionPattern = null;
    private String excludePartitionPattern = null;

    public GetPartitionProjectionsSpecBuilder(List<String> partitionList, String includePartitionPattern,
                                              String excludePartitionPattern) {
        this.partitionList = partitionList;
        this.includePartitionPattern = includePartitionPattern;
        this.excludePartitionPattern = excludePartitionPattern;
    }

    public GetPartitionProjectionsSpecBuilder setPartitionList(List<String> partitionList) {
        this.partitionList = partitionList;
        return this;
    }

    public GetPartitionProjectionsSpecBuilder setIncludePartitionPattern(String includePartitionPattern) {
        this.includePartitionPattern = includePartitionPattern;
        return this;
    }

    public GetPartitionProjectionsSpecBuilder setExcludePartitionPattern(String excludePartitionPattern) {
        this.excludePartitionPattern = excludePartitionPattern;
        return this;
    }

    public GetProjectionsSpec build() {
        return new GetProjectionsSpec(partitionList, includePartitionPattern, excludePartitionPattern);
    }
}