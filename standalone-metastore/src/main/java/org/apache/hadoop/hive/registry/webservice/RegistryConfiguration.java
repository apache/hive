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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.hive.registry.common.FileStorageConfiguration;
import org.apache.hadoop.hive.registry.common.ModuleConfiguration;
import org.apache.hadoop.hive.registry.common.ServletFilterConfiguration;
import org.apache.hadoop.hive.registry.storage.core.StorageProviderConfiguration;
import io.dropwizard.Configuration;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;

import javax.validation.constraints.NotNull;
import java.util.List;

public class RegistryConfiguration extends Configuration {

    @NotNull
    private StorageProviderConfiguration storageProviderConfiguration;

    @NotNull
    private FileStorageConfiguration fileStorageConfiguration;

    @NotNull
    @JsonProperty
    private List<ModuleConfiguration> modules;


    @JsonProperty
    private boolean enableCors;

    @JsonProperty("swagger")
    private SwaggerBundleConfiguration swaggerBundleConfiguration;

    private List<ServletFilterConfiguration> servletFilters;

    public StorageProviderConfiguration getStorageProviderConfiguration() {
        return storageProviderConfiguration;
    }

    public void setStorageProviderConfiguration(StorageProviderConfiguration storageProviderConfiguration) {
        this.storageProviderConfiguration = storageProviderConfiguration;
    }

    public FileStorageConfiguration getFileStorageConfiguration() {
        return fileStorageConfiguration;
    }

    public void setFileStorageConfiguration(FileStorageConfiguration fileStorageConfiguration) {
        this.fileStorageConfiguration = fileStorageConfiguration;
    }

    public boolean isEnableCors() {
        return enableCors;
    }

    public List<ModuleConfiguration> getModules() {
        return modules;
    }

    public void setModules(List<ModuleConfiguration> modules) {
        this.modules = modules;
    }

    public void setSwaggerBundleConfiguration(SwaggerBundleConfiguration swaggerBundleConfiguration) {
        this.swaggerBundleConfiguration = swaggerBundleConfiguration;
    }

    public SwaggerBundleConfiguration getSwaggerBundleConfiguration() {
        return swaggerBundleConfiguration;
    }


    public List<ServletFilterConfiguration> getServletFilters() {
        return servletFilters;
    }

    public void setServletFilters(List<ServletFilterConfiguration> servletFilters) {
        this.servletFilters = servletFilters;
    }
}
