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

package org.apache.hadoop.hive.ql.exec.repl.ranger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.multipart.FormDataMultiPart;
import com.sun.jersey.multipart.MultiPart;
import com.sun.jersey.multipart.file.StreamDataBodyPart;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.utils.Retry;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.http.client.utils.URIBuilder;
import org.eclipse.jetty.util.MultiPartWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.File;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.Reader;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Arrays;

/**
 * RangerRestClientImpl to connect to Ranger and export policies.
 */
public class RangerRestClientImpl implements RangerRestClient {
  private static final Logger LOG = LoggerFactory.getLogger(RangerRestClientImpl.class);
  private static final String RANGER_REST_URL_EXPORTJSONFILE = "service/plugins/policies/exportJson";
  private static final String RANGER_REST_URL_IMPORTJSONFILE =
      "service/plugins/policies/importPoliciesFromFile";

  public RangerExportPolicyList exportRangerPolicies(String sourceRangerEndpoint,
                                                     String dbName,
                                                     String rangerHiveServiceName)throws SemanticException {
    LOG.info("Ranger endpoint for cluster " + sourceRangerEndpoint);
    if (StringUtils.isEmpty(rangerHiveServiceName)) {
      throw new SemanticException("Ranger Service Name cannot be empty");
    }
    Retry<RangerExportPolicyList> retriable = new Retry<RangerExportPolicyList>(Exception.class) {
      @Override
      public RangerExportPolicyList execute() throws Exception {
        String finalUrl = getRangerExportUrl(sourceRangerEndpoint, rangerHiveServiceName, dbName);
        LOG.debug("Url to export policies from source Ranger: {}", finalUrl);
        WebResource.Builder builder = getRangerResourceBuilder(finalUrl);
        RangerExportPolicyList rangerExportPolicyList = new RangerExportPolicyList();
        ClientResponse clientResp = builder.get(ClientResponse.class);
        String response = null;
        if (clientResp != null) {
          if (clientResp.getStatus() == HttpServletResponse.SC_OK) {
            Gson gson = new GsonBuilder().create();
            response = clientResp.getEntity(String.class);
            LOG.debug("Response received for ranger export {} ", response);
            if (StringUtils.isNotEmpty(response)) {
              rangerExportPolicyList = gson.fromJson(response, RangerExportPolicyList.class);
              return rangerExportPolicyList;
            }
          } else if (clientResp.getStatus() == HttpServletResponse.SC_NO_CONTENT) {
            LOG.debug("Ranger policy export request returned empty list");
            return rangerExportPolicyList;
          } else if (clientResp.getStatus() == HttpServletResponse.SC_UNAUTHORIZED) {
            throw new SemanticException("Authentication Failure while communicating to Ranger admin");
          } else if (clientResp.getStatus() == HttpServletResponse.SC_FORBIDDEN) {
            throw new SemanticException("Authorization Failure while communicating to Ranger admin");
          }
        }
        if (StringUtils.isEmpty(response)) {
          LOG.debug("Ranger policy export request returned empty list or failed, Please refer Ranger admin logs.");
        }
        return null;
      }
    };
    try {
      return retriable.runWithDelay();
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  public String getRangerExportUrl(String sourceRangerEndpoint, String rangerHiveServiceName,
                            String dbName) throws URISyntaxException {
    URIBuilder uriBuilder = new URIBuilder(sourceRangerEndpoint);
    uriBuilder.setPath(RANGER_REST_URL_EXPORTJSONFILE);
    uriBuilder.addParameter("serviceName", rangerHiveServiceName);
    uriBuilder.addParameter("polResource", dbName);
    uriBuilder.addParameter("resource:database", dbName);
    uriBuilder.addParameter("serviceType", "hive");
    uriBuilder.addParameter("resourceMatchScope", "self_or_ancestor");
    uriBuilder.addParameter("resourceMatch", "full");
    return uriBuilder.build().toString();
  }

  public List<RangerPolicy> removeMultiResourcePolicies(List<RangerPolicy> rangerPolicies) {
    List<RangerPolicy> rangerPoliciesToImport = new ArrayList<RangerPolicy>();
    if (CollectionUtils.isNotEmpty(rangerPolicies)) {
      Map<String, RangerPolicy.RangerPolicyResource> rangerPolicyResourceMap = null;
      RangerPolicy.RangerPolicyResource rangerPolicyResource = null;
      List<String> resourceNameList = null;
      for (RangerPolicy rangerPolicy : rangerPolicies) {
        if (rangerPolicy != null) {
          rangerPolicyResourceMap = rangerPolicy.getResources();
          if (rangerPolicyResourceMap != null) {
            rangerPolicyResource = rangerPolicyResourceMap.get("database");
            if (rangerPolicyResource != null) {
              resourceNameList = rangerPolicyResource.getValues();
              if (CollectionUtils.isNotEmpty(resourceNameList) && resourceNameList.size() == 1) {
                rangerPoliciesToImport.add(rangerPolicy);
              }
            }
          }
        }
      }
    }
    return rangerPoliciesToImport;
  }

  @Override
  public RangerExportPolicyList importRangerPolicies(RangerExportPolicyList rangerExportPolicyList, String dbName,
                                                     String baseUrl,
                                                     String rangerHiveServiceName)
      throws Exception {
    String sourceClusterServiceName = null;
    String serviceMapJsonFileName = "hive_servicemap.json";
    String rangerPoliciesJsonFileName = "hive_replicationPolicies.json";

    if (!rangerExportPolicyList.getPolicies().isEmpty()) {
      sourceClusterServiceName = rangerExportPolicyList.getPolicies().get(0).getService();
    }

    if (StringUtils.isEmpty(sourceClusterServiceName)) {
      sourceClusterServiceName = rangerHiveServiceName;
    }

    Map<String, String> serviceMap = new LinkedHashMap<String, String>();
    if (!StringUtils.isEmpty(sourceClusterServiceName) && !StringUtils.isEmpty(rangerHiveServiceName)) {
      serviceMap.put(sourceClusterServiceName, rangerHiveServiceName);
    }

    Gson gson = new GsonBuilder().create();
    String jsonServiceMap = gson.toJson(serviceMap);

    String jsonRangerExportPolicyList = gson.toJson(rangerExportPolicyList);
    String finalUrl = getRangerImportUrl(baseUrl, dbName);
    LOG.debug("URL to import policies on target Ranger: {}", finalUrl);
    Retry<RangerExportPolicyList> retriable = new Retry<RangerExportPolicyList>(Exception.class) {
      @Override
      public RangerExportPolicyList execute() throws Exception {
        ClientResponse clientResp = null;

        StreamDataBodyPart filePartPolicies = new StreamDataBodyPart("file",
            new ByteArrayInputStream(jsonRangerExportPolicyList.getBytes(StandardCharsets.UTF_8)),
            rangerPoliciesJsonFileName);
        StreamDataBodyPart filePartServiceMap = new StreamDataBodyPart("servicesMapJson",
            new ByteArrayInputStream(jsonServiceMap.getBytes(StandardCharsets.UTF_8)), serviceMapJsonFileName);

        FormDataMultiPart formDataMultiPart = new FormDataMultiPart();
        MultiPart multipartEntity = null;
        try {
          multipartEntity = formDataMultiPart.bodyPart(filePartPolicies).bodyPart(filePartServiceMap);
          WebResource.Builder builder = getRangerResourceBuilder(finalUrl);
          clientResp = builder.accept(MediaType.APPLICATION_JSON).type(MediaType.MULTIPART_FORM_DATA)
            .post(ClientResponse.class, multipartEntity);
          if (clientResp != null) {
            if (clientResp.getStatus() == HttpServletResponse.SC_NO_CONTENT) {
              LOG.debug("Ranger policy import finished successfully");

            } else if (clientResp.getStatus() == HttpServletResponse.SC_UNAUTHORIZED) {
              throw new Exception("Authentication Failure while communicating to Ranger admin");
            } else {
              throw new Exception("Ranger policy import failed, Please refer target Ranger admin logs.");
            }
          }
        } finally {
          try {
            if (filePartPolicies != null) {
              filePartPolicies.cleanup();
            }
            if (filePartServiceMap != null) {
              filePartServiceMap.cleanup();
            }
            if (formDataMultiPart != null) {
              formDataMultiPart.close();
            }
            if (multipartEntity != null) {
              multipartEntity.close();
            }
          } catch (IOException e) {
            LOG.error("Exception occurred while closing resources: {}", e);
          }
        }
        return rangerExportPolicyList;
      }
    };
    try {
      return retriable.runWithDelay();
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  public String getRangerImportUrl(String rangerUrl, String dbName) throws URISyntaxException {
    URIBuilder uriBuilder = new URIBuilder(rangerUrl);
    uriBuilder.setPath(RANGER_REST_URL_IMPORTJSONFILE);
    uriBuilder.addParameter("updateIfExists", "true");
    uriBuilder.addParameter("polResource", dbName);
    return uriBuilder.build().toString();
  }

  private synchronized Client getRangerClient() {
    Client ret = null;
    ClientConfig config = new DefaultClientConfig();
    config.getClasses().add(MultiPartWriter.class);
    config.getProperties().put(ClientConfig.PROPERTY_FOLLOW_REDIRECTS, true);
    ret = Client.create(config);
    return ret;
  }

  @Override
  public List<RangerPolicy> changeDataSet(List<RangerPolicy> rangerPolicies, String sourceDbName,
                                          String targetDbName) {
    if (targetDbName.equals(sourceDbName)) {
      return rangerPolicies;
    }
    if (CollectionUtils.isNotEmpty(rangerPolicies)) {
      Map<String, RangerPolicy.RangerPolicyResource> rangerPolicyResourceMap = null;
      RangerPolicy.RangerPolicyResource rangerPolicyResource = null;
      List<String> resourceNameList = null;
      for (RangerPolicy rangerPolicy : rangerPolicies) {
        if (rangerPolicy != null) {
          rangerPolicyResourceMap = rangerPolicy.getResources();
          if (rangerPolicyResourceMap != null) {
            rangerPolicyResource = rangerPolicyResourceMap.get("database");
            if (rangerPolicyResource != null) {
              resourceNameList = rangerPolicyResource.getValues();
              if (CollectionUtils.isNotEmpty(resourceNameList)) {
                for (int i = 0; i < resourceNameList.size(); i++) {
                  String resourceName = resourceNameList.get(i);
                  if (resourceName.equals(sourceDbName)) {
                    resourceNameList.set(i, targetDbName);
                  }
                }
              }
            }
          }
        }
      }
    }
    return rangerPolicies;
  }

  private Path writeExportedRangerPoliciesToJsonFile(String jsonString, String fileName, Path stagingDirPath,
                                                     HiveConf conf)
      throws IOException {
    String filePath = "";
    Path newPath = null;
    FSDataOutputStream outStream = null;
    OutputStreamWriter writer = null;
    try {
      if (!StringUtils.isEmpty(jsonString)) {
        FileSystem fileSystem = stagingDirPath.getFileSystem(conf);
        if (fileSystem != null) {
          if (!fileSystem.exists(stagingDirPath)) {
            fileSystem.mkdirs(stagingDirPath);
          }
          newPath = stagingDirPath.suffix(File.separator + fileName);
          outStream = fileSystem.create(newPath, true);
          writer = new OutputStreamWriter(outStream, "UTF-8");
          writer.write(jsonString);
        }
      }
    } catch (IOException ex) {
      if (newPath != null) {
        filePath = newPath.toString();
      }
      throw new IOException("Failed to write json string to file:" + filePath, ex);
    } catch (Exception ex) {
      if (newPath != null) {
        filePath = newPath.toString();
      }
      throw new IOException("Failed to write json string to file:" + filePath, ex);
    } finally {
      try {
        if (writer != null) {
          writer.close();
        }
        if (outStream != null) {
          outStream.close();
        }
      } catch (Exception ex) {
        throw new IOException("Unable to close writer/outStream.", ex);
      }
    }
    return newPath;
  }

  @Override
  public Path saveRangerPoliciesToFile(RangerExportPolicyList rangerExportPolicyList, Path stagingDirPath,
                                       String fileName, HiveConf conf) throws Exception {
    Gson gson = new GsonBuilder().create();
    String jsonRangerExportPolicyList = gson.toJson(rangerExportPolicyList);
    Retry<Path> retriable = new Retry<Path>(IOException.class) {
      @Override
      public Path execute() throws IOException {
        return writeExportedRangerPoliciesToJsonFile(jsonRangerExportPolicyList, fileName,
            stagingDirPath, conf);
      }
    };
    try {
      return retriable.run();
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  @Override
  public RangerExportPolicyList readRangerPoliciesFromJsonFile(Path filePath,
                                                               HiveConf conf) throws SemanticException {
    RangerExportPolicyList rangerExportPolicyList = null;
    Gson gsonBuilder = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").setPrettyPrinting().create();
    try {
      FileSystem fs = filePath.getFileSystem(conf);
      InputStream inputStream = fs.open(filePath);
      Reader reader = new InputStreamReader(inputStream, Charset.forName("UTF-8"));
      rangerExportPolicyList = gsonBuilder.fromJson(reader, RangerExportPolicyList.class);
    } catch (FileNotFoundException e) {
      //If the ranger policies are not present, json file will not be present
      return rangerExportPolicyList;
    } catch (Exception ex) {
      throw new SemanticException("Error reading file :" + filePath, ex);
    }
    return rangerExportPolicyList;
  }

  @Override
  public boolean checkConnection(String url) throws SemanticException {
    Retry<Boolean> retriable = new Retry<Boolean>(Exception.class) {
      @Override
      public Boolean execute() throws Exception {
        WebResource.Builder builder;
        builder = getRangerResourceBuilder(url);
        ClientResponse clientResp = builder.get(ClientResponse.class);
        return (clientResp.getStatus() < HttpServletResponse.SC_UNAUTHORIZED);
      }
    };
    try {
      return retriable.runWithDelay();
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  @Override
  public List<RangerPolicy> addDenyPolicies(List<RangerPolicy> rangerPolicies, String rangerServiceName,
                                            String sourceDb, String targetDb) throws SemanticException {
    if (StringUtils.isEmpty(rangerServiceName)) {
      throw new SemanticException("Ranger Service Name cannot be empty");
    }
    RangerPolicy denyRangerPolicy = new RangerPolicy();
    denyRangerPolicy.setService(rangerServiceName);
    denyRangerPolicy.setName(sourceDb + "_replication deny policy for " + targetDb);
    Map<String, RangerPolicy.RangerPolicyResource> rangerPolicyResourceMap = new HashMap<String,
        RangerPolicy.RangerPolicyResource>();
    RangerPolicy.RangerPolicyResource rangerPolicyResource = new RangerPolicy.RangerPolicyResource();
    List<String> resourceNameList = new ArrayList<String>();

    List<RangerPolicy.RangerPolicyItem> denyPolicyItemsForPublicGroup = denyRangerPolicy.getDenyPolicyItems();
    RangerPolicy.RangerPolicyItem denyPolicyItem = new RangerPolicy.RangerPolicyItem();
    List<RangerPolicy.RangerPolicyItemAccess> denyPolicyItemAccesses = new ArrayList<RangerPolicy.
        RangerPolicyItemAccess>();

    List<RangerPolicy.RangerPolicyItem> denyExceptionsItemsForBeaconUser = denyRangerPolicy.getDenyExceptions();
    RangerPolicy.RangerPolicyItem denyExceptionsPolicyItem = new RangerPolicy.RangerPolicyItem();
    List<RangerPolicy.RangerPolicyItemAccess> denyExceptionsPolicyItemAccesses = new ArrayList<RangerPolicy.
        RangerPolicyItemAccess>();

    resourceNameList.add(sourceDb);
    rangerPolicyResource.setValues(resourceNameList);
    RangerPolicy.RangerPolicyResource rangerPolicyResourceColumn =new RangerPolicy.RangerPolicyResource();
    rangerPolicyResourceColumn.setValues(new ArrayList<String>(){{add("*"); }});
    RangerPolicy.RangerPolicyResource rangerPolicyResourceTable =new RangerPolicy.RangerPolicyResource();
    rangerPolicyResourceTable.setValues(new ArrayList<String>(){{add("*"); }});
    rangerPolicyResourceMap.put("database", rangerPolicyResource);
    rangerPolicyResourceMap.put("table", rangerPolicyResourceTable);
    rangerPolicyResourceMap.put("column", rangerPolicyResourceColumn);
    denyRangerPolicy.setResources(rangerPolicyResourceMap);

    List<String> accessTypes = Arrays.asList("create", "update", "drop", "alter",
        "index", "lock", "write", "ReplAdmin");
    for (String access : accessTypes) {
      denyPolicyItemAccesses.add(new RangerPolicy.RangerPolicyItemAccess(access, true));
    }
    denyPolicyItem.setAccesses(denyPolicyItemAccesses);
    denyPolicyItemsForPublicGroup.add(denyPolicyItem);
    List<String> denyPolicyItemsGroups = new ArrayList<String>();
    denyPolicyItemsGroups.add("public");
    denyPolicyItem.setGroups(denyPolicyItemsGroups);
    denyRangerPolicy.setDenyPolicyItems(denyPolicyItemsForPublicGroup);

    List<String> denyExcludeAccessTypes = Arrays.asList("create", "update", "drop", "alter", "index", "lock", "write",
        "ReplAdmin", "select", "read");
    for (String access : denyExcludeAccessTypes) {
      denyExceptionsPolicyItemAccesses.add(new RangerPolicy.RangerPolicyItemAccess(access, true));
    }
    denyExceptionsPolicyItem.setAccesses(denyExceptionsPolicyItemAccesses);
    denyExceptionsItemsForBeaconUser.add(denyExceptionsPolicyItem);
    List<String> denyExceptionsPolicyItemsUsers = new ArrayList<String>();
    denyExceptionsPolicyItemsUsers.add("hive");
    denyExceptionsPolicyItem.setUsers(denyExceptionsPolicyItemsUsers);
    denyRangerPolicy.setDenyExceptions(denyExceptionsItemsForBeaconUser);

    rangerPolicies.add(denyRangerPolicy);
    return rangerPolicies;
  }


  private WebResource.Builder getRangerResourceBuilder(String url) {
    Client client = getRangerClient();
    WebResource webResource = client.resource(url);
    WebResource.Builder builder = webResource.getRequestBuilder();
    return builder;
  }
}
