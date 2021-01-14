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
package org.apache.hadoop.hive.ql.exec.tez;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstance;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstanceSet;
import org.apache.hadoop.hive.llap.registry.impl.InactiveServiceInstance;
import org.apache.hadoop.hive.llap.registry.impl.LlapFixedRegistryImpl;
import org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService;
import org.apache.hadoop.hive.llap.registry.impl.LlapZookeeperRegistryImpl;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.split.SplitLocationProvider;
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.junit.Before;
import org.junit.Test;

import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

/**
 * Test class for Utils methods.
 */
public class TestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);

  private static final String INACTIVE = "inactive";
  private static final String ACTIVE = "dynamic";
  private static final String DISABLED = "disabled";
  private static final String FIXED = "fix";


  @Mock
  private LlapRegistryService mockRegistry;

  @Mock
  private LlapServiceInstanceSet mockInstanceSet;

  @Before
  public void setUp() {
    initMocks(this);
  }

  @Test
  public void testGetSplitLocationProvider() throws IOException, URISyntaxException {
    // Create test LlapServiceInstances to make sure that we can handle all of the instance types
    List<LlapServiceInstance> instances = new ArrayList<>(3);

    // Set 1 inactive instance to make sure that this does not cause problem for us
    LlapServiceInstance inactive = new InactiveServiceInstance(INACTIVE);
    instances.add(inactive);

    HiveConf conf = new HiveConf();
    conf.set(HiveConf.ConfVars.HIVE_ZOOKEEPER_QUORUM.varname, "localhost");
    LlapZookeeperRegistryImpl dynRegistry = new LlapZookeeperRegistryImpl("dyn", conf);
    Endpoint rpcEndpoint = RegistryTypeUtils.ipcEndpoint("llap", new InetSocketAddress(ACTIVE, 4000));
    Endpoint shuffle = RegistryTypeUtils.ipcEndpoint("shuffle", new InetSocketAddress(ACTIVE, 4000));
    Endpoint mng = RegistryTypeUtils.ipcEndpoint("llapmng", new InetSocketAddress(ACTIVE, 4000));
    Endpoint outputFormat = RegistryTypeUtils.ipcEndpoint("llapoutputformat", new InetSocketAddress(ACTIVE, 4000));
    Endpoint services = RegistryTypeUtils.webEndpoint("services", new URI(ACTIVE + ":4000"));

    // Set 1 active instance
    ServiceRecord enabledSrv = new ServiceRecord();
    enabledSrv.addInternalEndpoint(rpcEndpoint);
    enabledSrv.addInternalEndpoint(shuffle);
    enabledSrv.addInternalEndpoint(mng);
    enabledSrv.addInternalEndpoint(outputFormat);
    enabledSrv.addExternalEndpoint(services);

    enabledSrv.set(LlapRegistryService.LLAP_DAEMON_NUM_ENABLED_EXECUTORS, 10);
    enabledSrv.set(HiveConf.ConfVars.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB.varname, 100);
    LlapZookeeperRegistryImpl.DynamicServiceInstance dynamic =
        dynRegistry.new DynamicServiceInstance(enabledSrv);
    instances.add(dynamic);

    // Set 1 instance with 0 executors
    ServiceRecord disabledSrv = new ServiceRecord(enabledSrv);
    disabledSrv.set(LlapRegistryService.LLAP_DAEMON_NUM_ENABLED_EXECUTORS, 0);
    LlapZookeeperRegistryImpl.DynamicServiceInstance disabled =
        dynRegistry.new DynamicServiceInstance(disabledSrv);
    disabled.setHost(DISABLED);
    instances.add(disabled);

    when(mockRegistry.getInstances()).thenReturn(mockInstanceSet);
    when(mockInstanceSet.getAllInstancesOrdered(anyBoolean())).thenReturn(instances);
    SplitLocationProvider provider = Utils.getCustomSplitLocationProvider(mockRegistry, LOG);

    assertLocations((HostAffinitySplitLocationProvider)provider, new String[] {ACTIVE});

    // Check if fixed stuff is working as well
    LlapFixedRegistryImpl fixRegistry = new LlapFixedRegistryImpl("llap", new HiveConf());

    // Instance for testing fixed registry instances
    LlapServiceInstance fixed = fixRegistry.new FixedServiceInstance(FIXED);
    instances.remove(dynamic);
    instances.add(fixed);

    provider = Utils.getCustomSplitLocationProvider(mockRegistry, LOG);

    assertLocations((HostAffinitySplitLocationProvider)provider, new String[] {FIXED});
  }

  private void assertLocations(HostAffinitySplitLocationProvider provider, String[] expectedLocations)
      throws IOException {
    InputSplit inputSplit1 =
        TestHostAffinitySplitLocationProvider.createMockFileSplit(
            true, "path2", 0, 1000, new String[]  {"HOST-1", "HOST-2"});

    // Check that the provider does not return disabled/inactive instances and returns onl 1 location
    List<String> result = new ArrayList<>(Arrays.asList(provider.getLocations(inputSplit1)));
    assertEquals(1, result.size());
    assertFalse(result.contains(INACTIVE));
    assertFalse(result.contains(DISABLED));

    // Since we can not check the results for every input, dig into the provider internal data to
    // make sure that we have only the available host name in the location list
    // Remove nulls
    Set<String> knownLocations = new HashSet<>();
    knownLocations.addAll(provider.locations);
    knownLocations.remove(null);
    assertArrayEquals(expectedLocations, knownLocations.toArray(new String[] {}));
  }

  public static class NoConstructorSplitLocationProvider implements SplitLocationProvider {

    @Override
    public String[] getLocations(final InputSplit inputSplit) throws IOException {
      return new String[0];
    }
  }

  public static class DefaultConstructorSplitLocationProvider implements SplitLocationProvider {

    public DefaultConstructorSplitLocationProvider() {

    }

    @Override
    public String[] getLocations(final InputSplit inputSplit) throws IOException {
      return new String[0];
    }
  }

  public static class ConfConstructorSplitLocationProvider implements SplitLocationProvider {
    private Configuration configuration;

    public ConfConstructorSplitLocationProvider(Configuration conf) {
      this.configuration = conf;
    }

    @Override
    public String[] getLocations(final InputSplit inputSplit) throws IOException {
      return new String[0];
    }
  }

  @Test
  public void testCustomSplitLocationProvider() throws IOException {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_MODE, "llap");

    conf.setVar(HiveConf.ConfVars.LLAP_SPLIT_LOCATION_PROVIDER_CLASS, NoConstructorSplitLocationProvider.class.getName());
    SplitLocationProvider splitLocationProvider = Utils.getSplitLocationProvider(conf, LOG);
    assertTrue(splitLocationProvider instanceof NoConstructorSplitLocationProvider);

    conf.setVar(HiveConf.ConfVars.LLAP_SPLIT_LOCATION_PROVIDER_CLASS, DefaultConstructorSplitLocationProvider.class.getName());
    splitLocationProvider = Utils.getSplitLocationProvider(conf, LOG);
    assertTrue(splitLocationProvider instanceof DefaultConstructorSplitLocationProvider);

    conf.setVar(HiveConf.ConfVars.LLAP_SPLIT_LOCATION_PROVIDER_CLASS, ConfConstructorSplitLocationProvider.class.getName());
    splitLocationProvider = Utils.getSplitLocationProvider(conf, LOG);
    assertTrue(splitLocationProvider instanceof ConfConstructorSplitLocationProvider);
  }
}
