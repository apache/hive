package org.apache.hadoop.hive.metastore.client.builder;

import org.apache.hadoop.hive.metastore.api.ClientCapabilities;
import org.apache.hadoop.hive.metastore.api.GetProjectionsSpec;
import org.apache.hadoop.hive.metastore.api.GetTablesExtRequestFields;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;

import java.util.List;

/**
 * Builder for requiredFields bitmask to be sent via GetTablesExtRequest
 */
public class GetTablesRequestBuilder {
    private String dbName = null;
    private List<String> tblNames = null;
    private ClientCapabilities capabilities = null;
    private String catName = null;
    private List<String> processorCapabilities = null;
    private String processorIdentifier = null;
    private GetProjectionsSpec projectionsSpec = null;
    private int requestedFields = 0x0;
    final static GetTablesExtRequestFields defValue = GetTablesExtRequestFields.ALL;

    public GetTablesRequestBuilder() {
    }

    public GetTablesRequestBuilder(String dbName, List<String> tblNames, ClientCapabilities capabilities,
                                   String catName, List<String> processorCapabilities, String processorIdentifier,
                                   GetProjectionsSpec projectionsSpec, int requestedFields) {
        this.dbName = dbName;
        this.tblNames = tblNames;
        this.capabilities = capabilities;
        this.catName = catName;
        this.processorCapabilities = processorCapabilities;
        this.processorIdentifier = processorIdentifier;
        this.projectionsSpec = projectionsSpec;
        this.requestedFields = requestedFields;
    }

    public GetTablesRequestBuilder setDbName(String dbName) {
        this.dbName = dbName;
        return this;
    }

    public GetTablesRequestBuilder setTblNames(List<String> tblNames) {
        this.tblNames = tblNames;
        return this;
    }

    public GetTablesRequestBuilder setCapabilities(ClientCapabilities capabilities) {
        this.capabilities = capabilities;
        return this;
    }

    public GetTablesRequestBuilder setCatName(String catName) {
        this.catName = catName;
        return this;
    }

    public GetTablesRequestBuilder setProcessorCapabilities(List<String> processorCapabilities) {
        this.processorCapabilities = processorCapabilities;
        return this;
    }

    public GetTablesRequestBuilder setProcessorIdentifier(String processorIdentifier) {
        this.processorIdentifier = processorIdentifier;
        return this;
    }

    public GetTablesRequestBuilder setGetProjectionsSpec(GetProjectionsSpec projectionsSpec) {
        this.projectionsSpec = projectionsSpec;
        return this;
    }

    public GetTablesRequestBuilder with(GetTablesExtRequestFields type) {
        switch (type) {
            case ALL:
                this.requestedFields |= Integer.MAX_VALUE;
                break;
            case PROCESSOR_CAPABILITIES:
                this.requestedFields |= 0x2;
                break;
            case ACCESS_TYPE:
                this.requestedFields |= 0x1;
                break;
            default:
                this.requestedFields |= Integer.MAX_VALUE;
                break;
        }
        return this;
    }

    public int bitValue() {
        return this.requestedFields;
    }

    public static int defaultValue() {
        return new GetTablesRequestBuilder().with(defValue).bitValue();
    }

    public GetTablesRequest build() {
        GetTablesRequest tablesRequest = new GetTablesRequest();
        tablesRequest.setDbName(dbName);
        tablesRequest.setTblNames(tblNames);
        tablesRequest.setCapabilities(capabilities);
        tablesRequest.setCatName(catName);
        tablesRequest.setProcessorCapabilities(processorCapabilities);
        tablesRequest.setProcessorIdentifier(processorIdentifier);
        tablesRequest.setProjectionSpec(projectionsSpec);
        return tablesRequest;
    }
}