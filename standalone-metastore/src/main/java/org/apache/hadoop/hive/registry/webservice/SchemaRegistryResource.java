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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.registry.webservice;

import com.codahale.metrics.annotation.Timed;
import org.apache.hadoop.hive.registry.common.catalog.CatalogResponse;
import org.apache.hadoop.hive.registry.common.transaction.UnitOfWork;
import org.apache.hadoop.hive.registry.common.util.WSUtils;
import org.apache.hadoop.hive.registry.AggregatedSchemaMetadataInfo;
import org.apache.hadoop.hive.registry.CompatibilityResult;
import org.apache.hadoop.hive.registry.ISchemaRegistry;
import org.apache.hadoop.hive.registry.SchemaVersionMergeResult;
import org.apache.hadoop.hive.registry.SchemaBranch;
import org.apache.hadoop.hive.registry.SchemaFieldInfo;
import org.apache.hadoop.hive.registry.SchemaFieldQuery;
import org.apache.hadoop.hive.registry.SchemaIdVersion;
import org.apache.hadoop.hive.registry.SchemaMetadata;
import org.apache.hadoop.hive.registry.SchemaMetadataInfo;
import org.apache.hadoop.hive.registry.SchemaMetadataStorable;
import org.apache.hadoop.hive.registry.SchemaProviderInfo;
import org.apache.hadoop.hive.registry.SchemaVersion;
import org.apache.hadoop.hive.registry.SchemaVersionInfo;
import org.apache.hadoop.hive.registry.SchemaVersionKey;
import org.apache.hadoop.hive.registry.SerDesInfo;
import org.apache.hadoop.hive.registry.SerDesPair;
import org.apache.hadoop.hive.registry.errors.IncompatibleSchemaException;
import org.apache.hadoop.hive.registry.errors.InvalidSchemaBranchDeletionException;
import org.apache.hadoop.hive.registry.errors.InvalidSchemaException;
import org.apache.hadoop.hive.registry.errors.SchemaBranchAlreadyExistsException;
import org.apache.hadoop.hive.registry.errors.SchemaBranchNotFoundException;
import org.apache.hadoop.hive.registry.errors.SchemaNotFoundException;
import org.apache.hadoop.hive.registry.errors.UnsupportedSchemaTypeException;
import org.apache.hadoop.hive.registry.state.SchemaLifecycleException;
import org.apache.hadoop.hive.registry.state.SchemaVersionLifecycleStateMachineInfo;
import org.apache.hadoop.hive.registry.storage.core.search.OrderBy;
import org.apache.hadoop.hive.registry.storage.core.search.WhereClause;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.registry.DefaultSchemaRegistry.ORDER_BY_FIELDS_PARAM_NAME;
import static org.apache.hadoop.hive.registry.SchemaBranch.MASTER_BRANCH;

/**
 * Schema Registry resource that provides schema registry REST service.
 */
@Path("/api/v1/schemaregistry")
@Api(value = "/api/v1/schemaregistry", description = "Endpoint for Schema Registry service")
@Produces(MediaType.APPLICATION_JSON)
public class SchemaRegistryResource extends BaseRegistryResource {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryResource.class);
    public static final String THROW_ERROR_IF_EXISTS = "_throwErrorIfExists";
    public static final String THROW_ERROR_IF_EXISTS_LOWER_CASE = THROW_ERROR_IF_EXISTS.toLowerCase();

    // reserved as schema related paths use these strings
    private static final String[] reservedNames = {"aggregate", "versions", "compatibility"};

    public SchemaRegistryResource(ISchemaRegistry schemaRegistry) {
        super(schemaRegistry);
    }

    @GET
    @Path("/schemaproviders")
    @ApiOperation(value = "Get list of registered Schema Providers",
            notes = "The Schema Registry supports different types of schemas, such as Avro, JSON etc. " + "" +
                    "A Schema Provider is needed for each type of schema supported by the Schema Registry. " +
                    "Schema Provider supports defining schema, serializing and deserializing data using the schema, " +
                    " and checking compatibility between different versions of the schema.",
            response = SchemaProviderInfo.class, responseContainer = "List",
            tags = OPERATION_GROUP_OTHER)
    @Timed
    public Response getRegisteredSchemaProviderInfos(@Context UriInfo uriInfo) {
        try {
            Collection<SchemaProviderInfo> schemaProviderInfos = schemaRegistry.getSupportedSchemaProviders();
            return WSUtils.respondEntities(schemaProviderInfos, Response.Status.OK);
        } catch (Exception ex) {
            LOG.error("Encountered error while listing schemas", ex);
            return WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }
    }

    //TODO : Get all the versions across all the branches

    @GET
    @Path("/schemas/aggregated")
    @ApiOperation(value = "Get list of schemas by filtering with the given query parameters",
            response = AggregatedSchemaMetadataInfo.class, responseContainer = "List", tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response listAggregatedSchemas(@Context UriInfo uriInfo) {
        try {
            MultivaluedMap<String, String> queryParameters = uriInfo.getQueryParameters();
            Map<String, String> filters = new HashMap<>();
            for (Map.Entry<String, List<String>> entry : queryParameters.entrySet()) {
                List<String> value = entry.getValue();
                filters.put(entry.getKey(), value != null && !value.isEmpty() ? value.get(0) : null);
            }
            Collection<AggregatedSchemaMetadataInfo> schemaMetadatas = schemaRegistry.findAggregatedSchemaMetadata(filters);

            return WSUtils.respondEntities(schemaMetadatas, Response.Status.OK);
        } catch (SchemaBranchNotFoundException e) {
            return WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND,  e.getMessage());
        } catch (Exception ex) {
            LOG.error("Encountered error while listing schemas", ex);
            return WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }
    }

    @GET
    @Path("/schemas/{name}/aggregated")
    @ApiOperation(value = "Get aggregated schema information for the given schema name",
            response = SchemaMetadataInfo.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response getAggregatedSchemaInfo(@ApiParam(value = "Schema name", required = true) @PathParam("name") String schemaName) {
        Response response;
        try {
            AggregatedSchemaMetadataInfo schemaMetadataInfo = schemaRegistry.getAggregatedSchemaMetadataInfo(schemaName);
            if (schemaMetadataInfo != null) {
                response = WSUtils.respondEntity(schemaMetadataInfo, Response.Status.OK);
            } else {
                response = WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, schemaName);
            }
        } catch (SchemaBranchNotFoundException e) {
            return WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND,  e.getMessage());
        } catch (Exception ex) {
            LOG.error("Encountered error while retrieving SchemaInfo with name: [{}]", schemaName, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @GET
    @Path("/schemas")
    @ApiOperation(value = "Get list of schemas by filtering with the given query parameters",
            response = SchemaMetadataInfo.class, responseContainer = "List", tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response listSchemas(@Context UriInfo uriInfo) {
        try {
            MultivaluedMap<String, String> queryParameters = uriInfo.getQueryParameters();
            Map<String, String> filters = new HashMap<>();
            for (Map.Entry<String, List<String>> entry : queryParameters.entrySet()) {
                List<String> value = entry.getValue();
                filters.put(entry.getKey(), value != null && !value.isEmpty() ? value.get(0) : null);
            }

            Collection<SchemaMetadataInfo> schemaMetadatas = schemaRegistry.findSchemaMetadata(filters);

            return WSUtils.respondEntities(schemaMetadatas, Response.Status.OK);
        } catch (Exception ex) {
            LOG.error("Encountered error while listing schemas", ex);
            return WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }
    }

    @GET
    @Path("/search/schemas")
    @ApiOperation(value = "Search for schemas containing the given name and description",
            notes = "Search the schemas for given name and description, return a list of schemas that contain the field.",
            response = SchemaMetadataInfo.class, responseContainer = "Collection", tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response findSchemas(@Context UriInfo uriInfo) {
        MultivaluedMap<String, String> queryParameters = uriInfo.getQueryParameters();
        try {
            Collection<SchemaMetadataInfo> schemaMetadataInfos = findSchemaMetadataInfos(queryParameters);
            return WSUtils.respondEntities(schemaMetadataInfos, Response.Status.OK);
        } catch (Exception ex) {
            LOG.error("Encountered error while finding schemas for given fields [{}]", queryParameters, ex);
            return WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }
    }

    private Collection<SchemaMetadataInfo> findSchemaMetadataInfos(MultivaluedMap<String, String> queryParameters) {
        Collection<SchemaMetadataInfo> schemaMetadataInfos;
        // name and description for now, complex queries are supported by backend and front end can send the json
        // query for those complex queries.
        if (queryParameters.containsKey(SchemaMetadataStorable.NAME)
                || queryParameters.containsKey(SchemaMetadataStorable.DESCRIPTION)) {
            String name = queryParameters.getFirst(SchemaMetadataStorable.NAME);
            String description = queryParameters.getFirst(SchemaMetadataStorable.DESCRIPTION);
            WhereClause whereClause = WhereClause.begin()
                                                 .contains(SchemaMetadataStorable.NAME, name)
                                                 .or()
                                                 .contains(SchemaMetadataStorable.DESCRIPTION, description)
                                                 .combine();
            //todo refactor orderby field in DefaultSchemaRegistry#search APIs merge with these APIs
            String orderByFieldStr = queryParameters.getFirst(ORDER_BY_FIELDS_PARAM_NAME);
            schemaMetadataInfos = schemaRegistry.searchSchemas(whereClause, getOrderByFields(orderByFieldStr));
        } else {
            schemaMetadataInfos = Collections.emptyList();
        }
        return schemaMetadataInfos;
    }

    private List<OrderBy> getOrderByFields(String value) {
        List<OrderBy> orderByList = new ArrayList<>();
        // _orderByFields=[<field-name>,<a/d>,]*
        // example can be : _orderByFields=foo,a,bar,d
        // order by foo with ascending then bar with descending
        String[] splitStrings = value.split(",");
        for (int i = 0; i < splitStrings.length; i += 2) {
            String ascStr = splitStrings[i + 1];
            boolean descending;
            if ("a".equals(ascStr)) {
                descending = false;
            } else if ("d".equals(ascStr)) {
                descending = true;
            } else {
                throw new IllegalArgumentException("Ascending or Descending identifier can only be 'a' or 'd' respectively.");
            }

            String fieldName = splitStrings[i];
            orderByList.add(descending ? OrderBy.desc(fieldName) : OrderBy.asc(fieldName));
        }

        return orderByList;
    }

    @GET
    @Path("/search/schemas/aggregated")
    @ApiOperation(value = "Search for schemas containing the given name and description",
            notes = "Search the schemas for given name and description, return a list of schemas that contain the field.",
            response = AggregatedSchemaMetadataInfo.class, responseContainer = "Collection", tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response findAggregatedSchemas(@Context UriInfo uriInfo) {
        MultivaluedMap<String, String> queryParameters = uriInfo.getQueryParameters();
        try {
            Collection<SchemaMetadataInfo> schemaMetadataInfos = findSchemaMetadataInfos(uriInfo.getQueryParameters());
            List<AggregatedSchemaMetadataInfo> aggregatedSchemaMetadataInfos = new ArrayList<>();
            for (SchemaMetadataInfo schemaMetadataInfo : schemaMetadataInfos) {
                SchemaMetadata schemaMetadata = schemaMetadataInfo.getSchemaMetadata();
                List<SerDesInfo> serDesInfos = new ArrayList<>(schemaRegistry.getSerDes(schemaMetadataInfo
                                                                                                .getSchemaMetadata()
                                                                                                .getName()));
                aggregatedSchemaMetadataInfos.add(
                        new AggregatedSchemaMetadataInfo(schemaMetadata,
                                                         schemaMetadataInfo.getId(),
                                                         schemaMetadataInfo.getTimestamp(),
                                                         schemaRegistry.getAggregatedSchemaBranch(schemaMetadata.getName()),
                                                         serDesInfos));
            }

            return WSUtils.respondEntities(aggregatedSchemaMetadataInfos, Response.Status.OK);
        } catch (SchemaBranchNotFoundException e) {
            return WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND,  e.getMessage());
        } catch (Exception ex) {
            LOG.error("Encountered error while finding schemas for given fields [{}]", queryParameters, ex);
            return WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }
    }

    @GET
    @Path("/search/schemas/fields")
    @ApiOperation(value = "Search for schemas containing the given field names",
            notes = "Search the schemas for given field names and return a list of schemas that contain the field.",
            response = SchemaVersionKey.class, responseContainer = "List", tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response findSchemasByFields(@Context UriInfo uriInfo) {
        MultivaluedMap<String, String> queryParameters = uriInfo.getQueryParameters();
        try {
            Collection<SchemaVersionKey> schemaVersionKeys = schemaRegistry.findSchemasByFields(buildSchemaFieldQuery(queryParameters));

            return WSUtils.respondEntities(schemaVersionKeys, Response.Status.OK);
        } catch (Exception ex) {
            LOG.error("Encountered error while finding schemas for given fields [{}]", queryParameters, ex);
            return WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }
    }

    private SchemaFieldQuery buildSchemaFieldQuery(MultivaluedMap<String, String> queryParameters) {
        SchemaFieldQuery.Builder builder = new SchemaFieldQuery.Builder();
        for (Map.Entry<String, List<String>> entry : queryParameters.entrySet()) {
            List<String> entryValue = entry.getValue();
            String value = entryValue != null && !entryValue.isEmpty() ? entryValue.get(0) : null;
            if (value != null) {
                if (SchemaFieldInfo.FIELD_NAMESPACE.equals(entry.getKey())) {
                    builder.namespace(value);
                } else if (SchemaFieldInfo.NAME.equals(entry.getKey())) {
                    builder.name(value);
                } else if (SchemaFieldInfo.TYPE.equals(entry.getKey())) {
                    builder.type(value);
                }
            }
        }

        return builder.build();
    }

    @POST
    @Path("/schemas")
    @ApiOperation(value = "Create a schema if it does not already exist",
            notes = "Creates a schema with the given schema information if it does not already exist." +
                    " A unique schema identifier is returned.",
            response = Long.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response addSchemaInfo(@ApiParam(value = "Schema to be added to the registry", required = true)
                                          SchemaMetadata schemaMetadata,
                                  @Context UriInfo uriInfo,
                                  @Context HttpHeaders httpHeaders) {
        Response response;
        try {
            schemaMetadata.trim();
            checkValueAsNullOrEmpty("Schema name", schemaMetadata.getName());
            checkValueAsNullOrEmpty("Schema type", schemaMetadata.getType());
            checkValidNames(schemaMetadata.getName());

            boolean throwErrorIfExists = isThrowErrorIfExists(httpHeaders);
            Long schemaId = schemaRegistry.addSchemaMetadata(schemaMetadata, throwErrorIfExists);
            response = WSUtils.respondEntity(schemaId, Response.Status.CREATED);
        } catch (IllegalArgumentException ex) {
            LOG.error("Expected parameter is invalid", schemaMetadata, ex);
            response = WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.BAD_REQUEST_PARAM_MISSING, ex.getMessage());
        } catch (UnsupportedSchemaTypeException ex) {
            LOG.error("Unsupported schema type encountered while adding schema metadata [{}]", schemaMetadata, ex);
            response = WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.UNSUPPORTED_SCHEMA_TYPE, ex.getMessage());
        } catch (Exception ex) {
            LOG.error("Error encountered while adding schema info [{}] ", schemaMetadata, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR,
                                       CatalogResponse.ResponseMessage.EXCEPTION,
                                       String.format("Storing the given SchemaMetadata [%s] is failed", schemaMetadata.toString()));
        }

        return response;
    }

    @POST
    @Path("/schemas/{name}")
    @ApiOperation(value = "Updates schema information for the given schema name",
        response = SchemaMetadataInfo.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response updateSchemaInfo(@ApiParam(value = "Schema name", required = true) @PathParam("name") String schemaName, 
                                     @ApiParam(value = "Schema to be added to the registry", required = true)
                                         SchemaMetadata schemaMetadata,
                                     @Context UriInfo uriInfo) {
        Response response;
        try {
            SchemaMetadataInfo schemaMetadataInfo = schemaRegistry.updateSchemaMetadata(schemaName, schemaMetadata);
            if (schemaMetadataInfo != null) {
                response = WSUtils.respondEntity(schemaMetadataInfo, Response.Status.OK);
            } else {
                response = WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, schemaName);
            }
        } catch (IllegalArgumentException ex) {
            LOG.error("Expected parameter is invalid", schemaName, schemaMetadata, ex);
            response = WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.BAD_REQUEST_PARAM_MISSING, ex.getMessage());
        } catch (Exception ex) {
            LOG.error("Encountered error while retrieving SchemaInfo with name: [{}]", schemaName, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }
        return response;
    }

    private void checkValidNames(String name) {
        for (String reservedName : reservedNames) {
            if (reservedName.equalsIgnoreCase(name)) {
                throw new IllegalArgumentException("schema name [" + reservedName + "] is reserved");
            }
        }
    }

    private boolean isThrowErrorIfExists(HttpHeaders httpHeaders) {
        List<String> values = httpHeaders.getRequestHeader(THROW_ERROR_IF_EXISTS);
        if (values != null) {
            values = httpHeaders.getRequestHeader(THROW_ERROR_IF_EXISTS_LOWER_CASE);
        }
        return values != null && !values.isEmpty() && Boolean.parseBoolean(values.get(0));
    }

    @GET
    @Path("/schemas/{name}")
    @ApiOperation(value = "Get schema information for the given schema name",
            response = SchemaMetadataInfo.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response getSchemaInfo(@ApiParam(value = "Schema name", required = true) @PathParam("name") String schemaName) {
        Response response;
        try {
            SchemaMetadataInfo schemaMetadataInfo = schemaRegistry.getSchemaMetadataInfo(schemaName);
            if (schemaMetadataInfo != null) {
                response = WSUtils.respondEntity(schemaMetadataInfo, Response.Status.OK);
            } else {
                response = WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, schemaName);
            }
        } catch (Exception ex) {
            LOG.error("Encountered error while retrieving SchemaInfo with name: [{}]", schemaName, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @GET
    @Path("/schemasById/{schemaId}")
    @ApiOperation(value = "Get schema for a given schema identifier",
            response = SchemaMetadataInfo.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response getSchemaInfo(@ApiParam(value = "Schema identifier", required = true) @PathParam("schemaId") Long schemaId) {
        Response response;
        try {
            SchemaMetadataInfo schemaMetadataInfo = schemaRegistry.getSchemaMetadataInfo(schemaId);
            if (schemaMetadataInfo != null) {
                response = WSUtils.respondEntity(schemaMetadataInfo, Response.Status.OK);
            } else {
                response = WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, schemaId.toString());
            }
        } catch (Exception ex) {
            LOG.error("Encountered error while retrieving SchemaInfo with schemaId: [{}]", schemaId, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @POST
    @Path("/schemas/{name}/versions/upload")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @ApiOperation(value = "Register a new version of the schema by uploading schema version text",
            notes = "Registers the given schema version to schema with name if the given file content is not registered as a version for this schema, " +
                    "and returns respective version number." +
                    "In case of incompatible schema errors, it throws error message like 'Unable to read schema: <> using schema <>' ",
            response = Integer.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response uploadSchemaVersion(@ApiParam(value = "Schema name", required = true) @PathParam("name")
                                                String schemaName,
                                        @QueryParam("branch") @DefaultValue(MASTER_BRANCH) String schemaBranchName,
                                        @ApiParam(value = "Schema version text file to be uploaded", required = true)
                                        @FormDataParam("file") final InputStream inputStream,
                                        @ApiParam(value = "Description about the schema version to be uploaded", required = true)
                                        @FormDataParam("description") final String description,
                                        @Context UriInfo uriInfo) {
        Response response;
        SchemaVersion schemaVersion = null;
        try {
            schemaVersion = new SchemaVersion(IOUtils.toString(inputStream, "UTF-8"),
                                              description);
            response = addSchemaVersion(schemaBranchName, schemaName, schemaVersion, uriInfo);
        } catch (IOException ex) {
            LOG.error("Encountered error while adding schema [{}] with key [{}]", schemaVersion, schemaName, ex, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @POST
    @Path("/schemas/{name}/versions")
    @ApiOperation(value = "Register a new version of the schema",
            notes = "Registers the given schema version to schema with name if the given schemaText is not registered as a version for this schema, " +
                    "and returns respective version number." +
                    "In case of incompatible schema errors, it throws error message like 'Unable to read schema: <> using schema <>' ",
            response = Integer.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response addSchemaVersion(@QueryParam("branch") @DefaultValue(MASTER_BRANCH) String schemaBranchName,
                                     @ApiParam(value = "Schema name", required = true) @PathParam("name")
                                      String schemaName,
                                     @ApiParam(value = "Details about the schema", required = true)
                                      SchemaVersion schemaVersion,

                                     @Context UriInfo uriInfo) {
        Response response;
        try {
            LOG.info("adding schema version for name [{}] with [{}]", schemaName, schemaVersion);
            SchemaIdVersion version = schemaRegistry.addSchemaVersion(schemaBranchName, schemaName, schemaVersion);
            response = WSUtils.respondEntity(version.getVersion(), Response.Status.CREATED);
        } catch (InvalidSchemaException ex) {
            LOG.error("Invalid schema error encountered while adding schema [{}] with key [{}]", schemaVersion, schemaName, ex);
            response = WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.INVALID_SCHEMA, ex.getMessage());
        } catch (IncompatibleSchemaException ex) {
            LOG.error("Incompatible schema error encountered while adding schema [{}] with key [{}]", schemaVersion, schemaName, ex);
            response = WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.INCOMPATIBLE_SCHEMA, ex.getMessage());
        } catch (UnsupportedSchemaTypeException ex) {
            LOG.error("Unsupported schema type encountered while adding schema [{}] with key [{}]", schemaVersion, schemaName, ex);
            response = WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.UNSUPPORTED_SCHEMA_TYPE, ex.getMessage());
        } catch (SchemaBranchNotFoundException e) {
            return WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND,  e.getMessage());
        } catch (Exception ex) {
            LOG.error("Encountered error while adding schema [{}] with key [{}]", schemaVersion, schemaName, ex, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @GET
    @Path("/schemas/{name}/versions/latest")
    @ApiOperation(value = "Get the latest version of the schema for the given schema name",
            response = SchemaVersionInfo.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response getLatestSchemaVersion(@ApiParam(value = "Schema name", required = true) @PathParam("name") String schemaName,
                                           @QueryParam("branch") @DefaultValue(MASTER_BRANCH) String schemaBranchName) {

        Response response;
        try {
            SchemaVersionInfo schemaVersionInfo = schemaRegistry.getLatestSchemaVersionInfo(schemaBranchName, schemaName);
            if (schemaVersionInfo != null) {
                response = WSUtils.respondEntity(schemaVersionInfo, Response.Status.OK);
            } else {
                LOG.info("No schemas found with schemakey: [{}]", schemaName);
                response = WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, schemaName);
            }
        } catch (SchemaBranchNotFoundException e) {
            return WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND,  e.getMessage());
        } catch (Exception ex) {
            LOG.error("Encountered error while getting latest schema version for schemakey [{}]", schemaName, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;

    }

    @GET
    @Path("/schemas/{name}/versions")
    @ApiOperation(value = "Get all the versions of the schema for the given schema name)",
            response = SchemaVersionInfo.class, responseContainer = "List", tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response getAllSchemaVersions(@ApiParam(value = "Schema name", required = true) @PathParam("name") String schemaName,
                                         @QueryParam("branch") @DefaultValue(MASTER_BRANCH) String schemaBranchName,
                                         @QueryParam("states") List<Byte> stateIds) {

        Response response;
        try {
            Collection<SchemaVersionInfo> schemaVersionInfos = schemaRegistry.getAllVersions(schemaBranchName, schemaName, stateIds);
            if (schemaVersionInfos != null) {
                response = WSUtils.respondEntities(schemaVersionInfos, Response.Status.OK);
            } else {
                LOG.info("No schemas found with schemakey: [{}]", schemaName);
                response = WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, schemaName);
            }
        } catch (SchemaBranchNotFoundException e) {
            return WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND,  e.getMessage());
        } catch (Exception ex) {
            LOG.error("Encountered error while getting all schema versions for schemakey [{}]", schemaName, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @GET
    @Path("/schemas/{name}/versions/{version}")
    @ApiOperation(value = "Get a version of the schema identified by the schema name",
            response = SchemaVersionInfo.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response getSchemaVersion(@ApiParam(value = "Schema name", required = true) @PathParam("name") String schemaMetadata,
                                     @ApiParam(value = "version of the schema", required = true) @PathParam("version") Integer versionNumber) {
        SchemaVersionKey schemaVersionKey = new SchemaVersionKey(schemaMetadata, versionNumber);

        Response response;
        try {
            SchemaVersionInfo schemaVersionInfo = schemaRegistry.getSchemaVersionInfo(schemaVersionKey);
            response = WSUtils.respondEntity(schemaVersionInfo, Response.Status.OK);
        } catch (SchemaNotFoundException e) {
            LOG.info("No schemas found with schemaVersionKey: [{}]", schemaVersionKey);
            response = WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, schemaVersionKey.toString());
        } catch (Exception ex) {
            LOG.error("Encountered error while getting all schema versions for schemakey [{}]", schemaMetadata, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @GET
    @Path("/schemas/versionsById/{id}")
    @ApiOperation(value = "Get a version of the schema identified by the given versionid",
            response = SchemaVersionInfo.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response getSchemaVersionById(@ApiParam(value = "version identifier of the schema", required = true) @PathParam("id") Long versionId) {
        SchemaIdVersion schemaIdVersion = new SchemaIdVersion(versionId);

        Response response;
        try {
            SchemaVersionInfo schemaVersionInfo = schemaRegistry.getSchemaVersionInfo(schemaIdVersion);
            response = WSUtils.respondEntity(schemaVersionInfo, Response.Status.OK);
        } catch (SchemaNotFoundException e) {
            LOG.info("No schema version is found with schema version id : [{}]", versionId);
            response = WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, versionId.toString());
        } catch (Exception ex) {
            LOG.error("Encountered error while getting schema version with id [{}]", versionId, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @GET
    @Path("/schemas/versions/statemachine")
    @ApiOperation(value = "Get schema version life cycle states",
            response = SchemaVersionInfo.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    public Response getSchemaVersionLifeCycleStates() {
        Response response;
        try {
            SchemaVersionLifecycleStateMachineInfo states = schemaRegistry.getSchemaVersionLifecycleStateMachineInfo();
            response = WSUtils.respondEntity(states, Response.Status.OK);
        } catch (Exception ex) {
            LOG.error("Encountered error while getting schema version lifecycle states", ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @POST
    @Path("/schemas/versions/{id}/state/enable")
    @ApiOperation(value = "Enables version of the schema identified by the given versionid",
            response = Boolean.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response enableSchema(@ApiParam(value = "version identifier of the schema", required = true) @PathParam("id") Long versionId) {

        Response response;
        try {
            schemaRegistry.enableSchemaVersion(versionId);
            response = WSUtils.respondEntity(true, Response.Status.OK);
        } catch (SchemaNotFoundException e) {
            LOG.info("No schema version is found with schema version id : [{}]", versionId);
            response = WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, versionId.toString());
        } catch(IncompatibleSchemaException e) {
            LOG.error("Encountered error while enabling schema version with id [{}]", versionId, e);
            response = WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.INCOMPATIBLE_SCHEMA, e.getMessage());
        } catch(SchemaLifecycleException e) {
            LOG.error("Encountered error while enabling schema version with id [{}]", versionId, e);
            response = WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.BAD_REQUEST, e.getMessage());
        } catch (Exception ex) {
            LOG.error("Encountered error while getting schema version with id [{}]", versionId, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @POST
    @Path("/schemas/versions/{id}/state/disable")
    @ApiOperation(value = "Disables version of the schema identified by the given version id",
            response = Boolean.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response disableSchema(@ApiParam(value = "version identifier of the schema", required = true) @PathParam("id") Long versionId) {

        Response response;
        try {
            schemaRegistry.disableSchemaVersion(versionId);
            response = WSUtils.respondEntity(true, Response.Status.OK);
        } catch (SchemaNotFoundException e) {
            LOG.info("No schema version is found with schema version id : [{}]", versionId);
            response = WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, versionId.toString());
        } catch(SchemaLifecycleException e) {
            LOG.error("Encountered error while disabling schema version with id [{}]", versionId, e);
            response = WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.BAD_REQUEST, e.getMessage());
        }catch (Exception ex) {
            LOG.error("Encountered error while getting schema version with id [{}]", versionId, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @POST
    @Path("/schemas/versions/{id}/state/archive")
    @ApiOperation(value = "Disables version of the schema identified by the given version id",
            response = Boolean.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response archiveSchema(@ApiParam(value = "version identifier of the schema", required = true) @PathParam("id") Long versionId) {

        Response response;
        try {
            schemaRegistry.archiveSchemaVersion(versionId);
            response = WSUtils.respondEntity(true, Response.Status.OK);
        } catch (SchemaNotFoundException e) {
            LOG.info("No schema version is found with schema version id : [{}]", versionId);
            response = WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, versionId.toString());
        } catch(SchemaLifecycleException e) {
            LOG.error("Encountered error while disabling schema version with id [{}]", versionId, e);
            response = WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.BAD_REQUEST, e.getMessage());
        }catch (Exception ex) {
            LOG.error("Encountered error while getting schema version with id [{}]", versionId, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }


    @POST
    @Path("/schemas/versions/{id}/state/delete")
    @ApiOperation(value = "Disables version of the schema identified by the given version id",
            response = Boolean.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response deleteSchema(@ApiParam(value = "version identifier of the schema", required = true) @PathParam("id") Long versionId) {

        Response response;
        try {
            schemaRegistry.deleteSchemaVersion(versionId);
            response = WSUtils.respondEntity(true, Response.Status.OK);
        } catch (SchemaNotFoundException e) {
            LOG.info("No schema version is found with schema version id : [{}]", versionId);
            response = WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, versionId.toString());
        } catch(SchemaLifecycleException e) {
            LOG.error("Encountered error while disabling schema version with id [{}]", versionId, e);
            response = WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.BAD_REQUEST_WITH_MESSAGE, e.getMessage());
        } catch (Exception ex) {
            LOG.error("Encountered error while getting schema version with id [{}]", versionId, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @POST
    @Path("/schemas/versions/{id}/state/startReview")
    @ApiOperation(value = "Disables version of the schema identified by the given version id",
            response = Boolean.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response startReviewSchema(@ApiParam(value = "version identifier of the schema", required = true) @PathParam("id") Long versionId) {

        Response response;
        try {
            schemaRegistry.startSchemaVersionReview(versionId);
            response = WSUtils.respondEntity(true, Response.Status.OK);
        } catch (SchemaNotFoundException e) {
            LOG.info("No schema version is found with schema version id : [{}]", versionId);
            response = WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, versionId.toString());
        } catch(SchemaLifecycleException e) {
            LOG.error("Encountered error while disabling schema version with id [{}]", versionId, e);
            response = WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.BAD_REQUEST, e.getMessage());
        } catch (Exception ex) {
            LOG.error("Encountered error while getting schema version with id [{}]", versionId, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @POST
    @Path("/schemas/versions/{id}/state/{stateId}")
    @ApiOperation(value = "Runs the state execution for schema version identified by the given version id and executes action associated with target state id",
            response = Boolean.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response executeState(@ApiParam(value = "version identifier of the schema", required = true) @PathParam("id") Long versionId,
                                 @ApiParam(value = "", required = true) @PathParam("stateId") Byte stateId,
                                 byte [] transitionDetails) {

        Response response;
        try {
            schemaRegistry.transitionState(versionId, stateId, transitionDetails);
            response = WSUtils.respondEntity(true, Response.Status.OK);
        } catch (SchemaNotFoundException e) {
            LOG.info("No schema version is found with schema version id : [{}]", versionId);
            response = WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, versionId.toString());
        } catch(SchemaLifecycleException e) {
            LOG.error("Encountered error while disabling schema version with id [{}]", versionId, e);
            CatalogResponse.ResponseMessage badRequestResponse =
                    e.getCause() != null && e.getCause() instanceof IncompatibleSchemaException
                    ? CatalogResponse.ResponseMessage.INCOMPATIBLE_SCHEMA
                    : CatalogResponse.ResponseMessage.BAD_REQUEST;
            response = WSUtils.respond(Response.Status.BAD_REQUEST, badRequestResponse, e.getMessage());
        } catch (Exception ex) {
            LOG.error("Encountered error while getting schema version with id [{}]", versionId, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @POST
    @Path("/schemas/{name}/compatibility")
    @ApiOperation(value = "Checks if the given schema text is compatible with all the versions of the schema identified by the name",
            response = CompatibilityResult.class, tags = OPERATION_GROUP_SCHEMA)
    @Timed
    @UnitOfWork
    public Response checkCompatibilityWithSchema(@QueryParam("branch") @DefaultValue(MASTER_BRANCH) String schemaBranchName,
                                                 @ApiParam(value = "Schema name", required = true) @PathParam("name") String schemaName,
                                                 @ApiParam(value = "schema text", required = true) String schemaText) {
        Response response;
        try {
            CompatibilityResult compatibilityResult = schemaRegistry.checkCompatibility(schemaBranchName, schemaName, schemaText);
            response = WSUtils.respondEntity(compatibilityResult, Response.Status.OK);
        } catch (SchemaNotFoundException e) {
            LOG.error("No schemas found with schemakey: [{}]", schemaName, e);
            response = WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, schemaName);
        } catch (SchemaBranchNotFoundException e) {
            return WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND,  e.getMessage());
        } catch (Exception ex) {
            LOG.error("Encountered error while checking compatibility with versions of schema with [{}] for given schema text [{}]", schemaName, schemaText, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @GET
    @Path("/schemas/{name}/serdes")
    @ApiOperation(value = "Get list of Serializers registered for the given schema name",
            response = SerDesInfo.class, responseContainer = "List", tags = OPERATION_GROUP_SERDE)
    @Timed
    @UnitOfWork
    public Response getSerializers(@ApiParam(value = "Schema name", required = true) @PathParam("name") String schemaName) {
        Response response;
        try {
            SchemaMetadataInfo schemaMetadataInfoStorable = schemaRegistry.getSchemaMetadataInfo(schemaName);
            if (schemaMetadataInfoStorable != null) {
                Collection<SerDesInfo> schemaSerializers = schemaRegistry.getSerDes(schemaMetadataInfoStorable.getSchemaMetadata().getName());
                response = WSUtils.respondEntities(schemaSerializers, Response.Status.OK);
            } else {
                LOG.info("No schemas found with schemakey: [{}]", schemaName);
                response = WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, schemaName);
            }
        } catch (Exception ex) {
            LOG.error("Encountered error while getting serializers for schemaKey [{}]", schemaName, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Path("/files")
    @ApiOperation(value = "Upload the given file and returns respective identifier.", response = String.class, tags = OPERATION_GROUP_OTHER)
    @Timed
    public Response uploadFile(@FormDataParam("file") final InputStream inputStream,
                               @FormDataParam("file") final FormDataContentDisposition contentDispositionHeader) {
        Response response;
        try {
            LOG.info("Received contentDispositionHeader: [{}]", contentDispositionHeader);
            String uploadedFileId = schemaRegistry.uploadFile(inputStream);
            response = WSUtils.respondEntity(uploadedFileId, Response.Status.OK);
        } catch (Exception ex) {
            LOG.error("Encountered error while uploading file", ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @GET
    @Produces({"application/octet-stream", "application/json"})
    @Path("/files/download/{fileId}")
    @ApiOperation(value = "Downloads the respective for the given fileId if it exists", response = StreamingOutput.class, tags = OPERATION_GROUP_OTHER)
    @Timed
    public Response downloadFile(@ApiParam(value = "Identifier of the file to be downloaded", required = true) @PathParam("fileId") String fileId) {
        Response response;
        try {
            StreamingOutput streamOutput = WSUtils.wrapWithStreamingOutput(schemaRegistry.downloadFile(fileId));
            response = Response.ok(streamOutput).build();
            return response;
        } catch (FileNotFoundException e) {
            LOG.error("No file found for fileId [{}]", fileId, e);
            response = WSUtils.respondEntity(fileId, Response.Status.NOT_FOUND);
        } catch (Exception ex) {
            LOG.error("Encountered error while downloading file [{}]", fileId, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @POST
    @Path("/serdes")
    @ApiOperation(value = "Add a Serializer/Deserializer into the Schema Registry", response = Long.class, tags = OPERATION_GROUP_SERDE)
    @Timed
    @UnitOfWork
    public Response addSerDes(@ApiParam(value = "Serializer/Deserializer information to be registered", required = true) SerDesPair serDesPair,
                              @Context UriInfo uriInfo) {
        return _addSerDesInfo(serDesPair);
    }

    @GET
    @Path("/serdes/{id}")
    @ApiOperation(value = "Get a Serializer for the given serializer id", response = SerDesInfo.class, tags = OPERATION_GROUP_SERDE)
    @Timed
    @UnitOfWork
    public Response getSerDes(@ApiParam(value = "Serializer identifier", required = true) @PathParam("id") Long serializerId) {
        return _getSerDesInfo(serializerId);
    }

    private Response _addSerDesInfo(SerDesPair serDesInfo) {
        Response response;
        try {
            Long serializerId = schemaRegistry.addSerDes(serDesInfo);
            response = WSUtils.respondEntity(serializerId, Response.Status.OK);
        } catch (Exception ex) {
            LOG.error("Encountered error while adding serializer/deserializer  [{}]", serDesInfo, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    private Response _getSerDesInfo(Long serializerId) {
        Response response;
        try {
            SerDesInfo serializerInfo = schemaRegistry.getSerDes(serializerId);
            response = WSUtils.respondEntity(serializerInfo, Response.Status.OK);
        } catch (Exception ex) {
            LOG.error("Encountered error while getting serializer/deserializer [{}]", serializerId, ex);
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }
        return response;
    }

    @POST
    @Path("/schemas/{name}/mapping/{serDesId}")
    @ApiOperation(value = "Bind the given Serializer/Deserializer to the schema identified by the schema name", tags = OPERATION_GROUP_SERDE)
    @Timed
    @UnitOfWork
    public Response mapSchemaWithSerDes(@ApiParam(value = "Schema name", required = true) @PathParam("name") String schemaName,
                                        @ApiParam(value = "Serializer/deserializer identifier", required = true) @PathParam("serDesId") Long serDesId,
                                        @Context UriInfo uriInfo) {
        Response response;
        try {
            schemaRegistry.mapSchemaWithSerDes(schemaName, serDesId);
            response = WSUtils.respondEntity(true, Response.Status.OK);
        } catch (Exception ex) {
            response = WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }

        return response;
    }

    @DELETE
    @Path("/schemas/{name}/versions/{version}")
    @ApiOperation(value = "Delete a schema version given its schema name and version id", tags = OPERATION_GROUP_SCHEMA)
    @UnitOfWork
    public Response deleteSchemaVersion(@ApiParam(value = "Schema name", required = true) @PathParam("name") String schemaName,
                                        @ApiParam(value = "version of the schema", required = true) @PathParam("version") Integer versionNumber,
                                        @Context UriInfo uriInfo) {
        SchemaVersionKey schemaVersionKey = null;
        try {
            schemaVersionKey = new SchemaVersionKey(schemaName, versionNumber);
            schemaRegistry.deleteSchemaVersion(schemaVersionKey);
            return WSUtils.respond(Response.Status.OK);
        } catch (SchemaNotFoundException e) {
            LOG.error("No schemaVersion found with name: [{}], version : [{}]", schemaName, versionNumber);
            return WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, schemaVersionKey.toString());
        } catch (SchemaLifecycleException e) {
            LOG.error("Failed to delete schema name: [{}], version : [{}]", schemaName, versionNumber, e);
            return WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.BAD_REQUEST_WITH_MESSAGE, e.getMessage());
        } catch (Exception ex) {
            LOG.error("Encountered error while deleting schemaVersion with name: [{}], version : [{}]", schemaName, versionNumber, ex);
            return WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }
    }

    @GET
    @Path("/schemas/{name}/branches")
    @ApiOperation(value = "Get list of registered schema branches",
            response = SchemaBranch.class, responseContainer = "List",
            tags = OPERATION_GROUP_OTHER)
    @Timed
    @UnitOfWork
    public Response getAllBranches(@ApiParam(value = "Details about schema name",required = true) @PathParam("name") String schemaName,
                                   @Context UriInfo uriInfo) {
        try {
            Collection<SchemaBranch> schemaBranches = schemaRegistry.getSchemaBranches(schemaName);
            return WSUtils.respondEntities(schemaBranches, Response.Status.OK);
        }  catch(SchemaNotFoundException e) {
            return WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND, schemaName);
        } catch (Exception ex) {
            LOG.error("Encountered error while listing schema branches", ex);
            return WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }
    }

    @POST
    @Path("/schemas/versionsById/{versionId}/branch")
    @ApiOperation(value = "Fork a new schema branch given its schema name and version id",
            response = SchemaBranch.class,
            tags = OPERATION_GROUP_SCHEMA)
    @UnitOfWork
    public Response createSchemaBranch( @ApiParam(value = "Details about schema version",required = true) @PathParam("versionId") Long schemaVersionId,
                                        @ApiParam(value = "Schema Branch Name", required = true) SchemaBranch schemaBranch) {
        try {
            SchemaBranch createdSchemaBranch = schemaRegistry.createSchemaBranch(schemaVersionId, schemaBranch);
            return WSUtils.respondEntity(createdSchemaBranch, Response.Status.OK) ;
        } catch (SchemaBranchAlreadyExistsException e) {
            return WSUtils.respond(Response.Status.CONFLICT, CatalogResponse.ResponseMessage.ENTITY_CONFLICT,  schemaBranch.getName());
        } catch (SchemaNotFoundException e) {
            return WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND,  schemaVersionId.toString());
        } catch (Exception ex) {
            LOG.error("Encountered error while creating a new branch with name: [{}], version : [{}]", schemaBranch.getName(), schemaVersionId, ex);
            return WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }
    }

    @POST
    @Path("/schemas/{versionId}/merge")
    @ApiOperation(value = "Merge a schema version to master given its version id",
            response = SchemaVersionMergeResult.class,
            tags = OPERATION_GROUP_SCHEMA)
    @UnitOfWork
    public Response mergeSchemaVersion(@ApiParam(value = "Details about schema version",required = true) @PathParam("versionId") Long schemaVersionId) {
        try {
            SchemaVersionMergeResult schemaVersionMergeResult = schemaRegistry.mergeSchemaVersion(schemaVersionId);
            return WSUtils.respondEntity(schemaVersionMergeResult, Response.Status.OK);
        } catch (SchemaNotFoundException e) {
            return WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND,  schemaVersionId.toString());
        } catch (IncompatibleSchemaException e) {
            return WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.INCOMPATIBLE_SCHEMA, e.getMessage());
        } catch (Exception ex) {
            LOG.error("Encountered error while merging a schema version to {} branch with version : [{}]", SchemaBranch.MASTER_BRANCH, schemaVersionId, ex);
            return WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }
    }

    @DELETE
    @Path("/schemas/branch/{branchId}")
    @ApiOperation(value = "Delete a branch give its name", tags = OPERATION_GROUP_SCHEMA)
    @UnitOfWork
    public Response deleteSchemaBranch(@ApiParam(value = "Schema Branch Name", required = true) @PathParam("branchId") Long schemaBranchId) {
        try {
            schemaRegistry.deleteSchemaBranch(schemaBranchId);
            return WSUtils.respond(Response.Status.OK);
        } catch (SchemaBranchNotFoundException e) {
            return WSUtils.respond(Response.Status.NOT_FOUND, CatalogResponse.ResponseMessage.ENTITY_NOT_FOUND,  schemaBranchId.toString());
        } catch (InvalidSchemaBranchDeletionException e) {
            return WSUtils.respond(Response.Status.BAD_REQUEST, CatalogResponse.ResponseMessage.BAD_REQUEST_WITH_MESSAGE, e.getMessage());
        } catch (Exception ex) {
            LOG.error("Encountered error while deleting a branch with name: [{}]", schemaBranchId, ex);
            return WSUtils.respond(Response.Status.INTERNAL_SERVER_ERROR, CatalogResponse.ResponseMessage.EXCEPTION, ex.getMessage());
        }
    }

}
