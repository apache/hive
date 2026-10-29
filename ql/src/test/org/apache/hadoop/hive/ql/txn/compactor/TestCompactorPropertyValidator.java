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
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestCompactorPropertyValidator {

  @ParameterizedTest
  @ValueSource(strings = {
      "mapreduce.map.memory.mb",
      "mapreduce.reduce.memory.mb",
      "mapreduce.map.cpu.vcores",
      "mapreduce.reduce.cpu.vcores",
      "hive.tez.container.size",
      "tez.task.resource.memory.mb",
      "mapreduce.job.heap.memory-mb.ratio",
      "mapreduce.task.io.sort.factor",
      "mapreduce.task.io.sort.mb",
      "yarn.app.mapreduce.am.resource.cpu-vcores",
      "yarn.app.mapreduce.am.resource.mb",
      "tez.am.resource.memory.mb",
      "tez.am.resource.cpu.vcores",
      "tez.grouping.min-size",
      "tez.grouping.max-size",
      "tez.grouping.split-waves"})
  void testResourceConfigs(String key) {
    assertTrue(allowed(new HiveConf(), key), key + " has to be settable in compaction job config.");
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "hive.compactor.gather.stats",
      "hive.mr.compactor.gather.stats",
      "hive.compactor.compact.insert.only",
      "hive.compactor.crud.query.based",
      "hive.compactor.max.num.delta",
      "hive.compactor.aborted.txn.time.threshold",
      "hive.compactor.abortedtxn.threshold",
      "hive.compactor.active.delta.dir.threshold",
      "hive.compactor.delta.num.threshold",
      "hive.compactor.obsolete.delta.dir.threshold",
      "hive.compactor.delta.pct.threshold",
      "hive.compactor.small.delta.dir.threshold"})
  void testCompactorConfigs(String key) {
    assertTrue(allowed(new HiveConf(), key), key + " has to be settable in compaction job config.");
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "mapreduce.job.queuename",
      "mapred.job.queue.name",
      "tez.queue.name",
      "hive.compactor.job.queue",
      "hive.compactor.request.queue"})
  void testQueueConfigs(String key) {
    assertTrue(allowed(new HiveConf(), key), key + " has to be settable in compaction job config.");
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "orc.compress",
      "orc.stripe.size",
      "orc.bloom.filter.columns",
      "hive.exec.orc.default.buffer.size"})
  void testOrcConfigs(String key) {
    assertTrue(allowed(new HiveConf(), key), key + " has to be settable in compaction job config.");
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "fs.defaultFS",
      "fs.s3a.access.key",
      "fs.AbstractFileSystem.abfs.impl",
      "hadoop.security.authentication",
      "hadoop.security.auth_to_local",
      "hadoop.rpc.socket.factory.class.default",
      "hadoop.htrace.span.receiver.classes",
      "hadoop.ssl.enabled.protocols",
      "hadoop.zk.kerberos.principal",
      "hadoop.http.authentication.kerberos.principal",
      "hive.exec.pre.hooks",
      "hive.exec.post.hooks",
      "hive.metastore.rawstore.impl",
      "hadoop.zk.kerberos.keytab",
      "hadoop.http.authentication.kerberos.keytab",
      "hive.security.authenticator.manager",
      "hive.server2.job.credential.provider.path",
      "hive.intermediate.compression.codec",
      "hive.metastore.end.function.listeners",
      "hadoop.token.files",
      "javax.jdo.PersistenceManagerFactoryClass",
      "mapreduce.map.java.opts",
      "mapred.child.java.opts",
      "hive.tez.java.opts",
      "hive.exim.uri.scheme.whitelist",
      "hive.exec.script.maxerrsize",
      "hive.script.operator.env.blacklist",
      "hive.llap.plugin.acl",
      "hive.exec.scratchdir",
      "hive.metastore.event.message.factory",
      "hive.exec.plan",
      "hive.aux.jars.path",
      "hive.added.jars.path",
      "hive.exec.perf.logger",
      "hive.log4j.file",
      "hive.exec.script.wrapper",
      "hive.qtest.additional.partial.mask.pattern",
      "mapreduce.job.classloader",
      "mapreduce.application.classpath",
      "yarn.application.classpath",
      "mapreduce.task.profile.params",
      "hadoop.tmp.dir",
      "hive.added.archives.path",
      "hive.tez.hs2.user.access",
      "orc.encrypt"})
  void testParametersRejected(String key) {
    assertFalse(allowed(new HiveConf(), key),
        key + " has to be rejected by hive.compactor.conf.allowlist or hive.compactor.conf.denylist");
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "hive.exec.pre.hooks",
      "hive.exec.post.hooks",
      "hive.exec.failure.hooks",
      "hive.exec.driver.run.hooks",
      "hive.exec.query.redactor.hooks",
      "hive.exec.perf.logger",
      "hive.optimize.cte.suggester.class",
      "hive.tez.mapreduce.output.committer.class",
      "tez.task.scale.memory.allocator.class",
      "tez.task.scale.task.memory.allocator.class",
      "orc.filter.plugin",
      "orc.filter.plugin.allowlist",
      "mapreduce.map.output.compress.codec",
      "hive.exec.script.wrapper",
      "hive.exec.script.trust",
      "hive.exec.script.allow.partial.consumption",
      "mapreduce.map.java.opts",
      "mapreduce.reduce.java.opts",
      "hive.tez.java.opts",
      "mapreduce.map.env",
      "mapreduce.reduce.env",
      "hive.exec.scratchdir",
      "hive.exec.local.scratchdir",
      "hive.exec.stagingdir",
      "hive.exec.submitviachild",
      "hive.exec.submit.local.task.via.child",
      "hive.exec.plan",
      "hive.exec.log4j.file",
      "orc.key.provider",
      "orc.encrypt","orc.mask",
      "hive.vectorized.adaptor.custom.udf.whitelist",
      "hive.tez.hs2.user.access",
      "hive.stats.dbclass",
      "hive.stats.default.publisher",
      "hive.stats.default.aggregator",
      "orc.kryo.sarg",
      "hive.compactor.table.props",
      "hive.compactor.is.compressed",
      "hive.compactor.input.tmp.dir",
      "hive.compactor.input.dir"})
  void testParametersAllowedByAllowlistButRejectedByDenyList(String key) {
    assertFalse(allowed(new HiveConf(), key), key + " has to be rejected by hive.compactor.conf.denylist");
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "mapreduce.map.java.opts ",
      " mapreduce.map.java.opts",
      "mapreduce.map.env\t",
      "tez.task.launch.cmd-opts\n",
      "hive.exec.pre.hooks\r"})
  void testParametersWithWhitespaceRejected(String key) {
    assertFalse(allowed(new HiveConf(), key), "'" + key + "' must be rejected.");
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "mapreduce.map.memory.mb   ",
      "   mapreduce.reduce.memory.mb",
      "\tmapreduce.map.cpu.vcores",
      "mapreduce.reduce.cpu.vcores\t",
      "hive.tez.container.size\n",
      "tez.task.resource.memory.mb\r"})
  void testParametersWithWhitespaceAllowed(String key) {
    assertTrue(allowed(new HiveConf(), key), key + " has to be settable in compaction job config.");
  }

  @Test
  void testAddConfigToAllowListAppend() {
    HiveConf conf = new HiveConf();
    assertFalse(allowed(conf, "hive.some.random.config"), "Not on the allow list by default");
    assertFalse(allowed(conf, "hive.madeup.config"), "Not on the allow list by default");
    assertFalse(allowed(conf, "hive.mapjoin.full.outer"), "Not on the allow list by default");
    assertFalse(allowed(conf, "hive.metastore.dml.events"), "Not on the allow list by default");
    assertFalse(allowed(conf, "yarn.app.mapreduce.am.hard-kill-timeout-ms"), "Not on the allow list by default");
    conf.setVar(HiveConf.ConfVars.HIVE_COMPACTOR_CONF_ALLOWLIST_APPEND,
        "hive.some.random.config,    hive.madeup.config,hive.mapjoin.full.outer,hive.metastore.dml.events,yarn.app.mapreduce.am.hard-kill-timeout-ms");
    assertTrue(allowed(conf, "hive.some.random.config"), "Admitted by exact name");
    assertTrue(allowed(conf, "hive.madeup.config"), "Whitespace around entries is trimmed");
    assertFalse(allowed(conf, "hive.some.random.otherconfig"), "Not admitted by exact name");
    assertTrue(allowed(conf, "mapreduce.map.memory.mb"), "The allow list regex still applies");
    assertTrue(allowed(conf, "hive.mapjoin.full.outer"), "Admitted by exact name");
    assertTrue(allowed(conf, "hive.metastore.dml.events"), "Admitted by exact name");
    assertTrue(allowed(conf, "yarn.app.mapreduce.am.hard-kill-timeout-ms"), "Admitted by exact name");
  }

  @Test
  void testConfigsAllowedListAppendCannotOverrideTheDenyList() {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.HIVE_COMPACTOR_CONF_ALLOWLIST_APPEND,
        "mapreduce.map.java.opts,hive.exec.pre.hooks,metastore.event.listeners,"
            + "hadoop.zk.ssl.keystore.password,hive.compactor.conf.denylist,"
            + "hive.compactor.conf.allowlist");
    assertFalse(allowed(conf, "mapreduce.map.java.opts"), "JVM options stay denied");
    assertFalse(allowed(conf, "hive.exec.pre.hooks"), "Hooks stay denied");
    assertFalse(allowed(conf, "metastore.event.listeners"), "Metastore listeners stay denied");
    assertFalse(allowed(conf, "hadoop.zk.ssl.keystore.password"), "Credentials stay denied");
    assertFalse(allowed(conf, "hive.compactor.conf.denylist"), "Policy parameters stay denied");
    assertFalse(allowed(conf, "hive.compactor.conf.allowlist"), "The allow list cannot admit itself");
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "hive.compactor.conf.allowlist",
      "hive.compactor.conf.allowlist.append",
      "hive.compactor.conf.denylist",
      "hive.conf.restricted.list",
      "hive.conf.hidden.list",
      "hive.conf.locked.list"})
  void testPolicyParametersAreNotSettable(String key) {
    assertFalse(allowed(new HiveConf(), key), key + " controls the policy and must not be settable");
  }

  /**
   * An unusable allow list admits nothing rather than everything.
   */
  @Test
  void testInvalidRegexInAllowList() {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.HIVE_COMPACTOR_CONF_ALLOWLIST, "([unclosed");
    assertFalse(allowed(conf, "mapreduce.map.memory.mb"), "Even ordinary tuning is rejected");
    conf.setVar(HiveConf.ConfVars.HIVE_COMPACTOR_CONF_ALLOWLIST_APPEND, "mapreduce.map.memory.mb");
    assertTrue(allowed(conf, "mapreduce.map.memory.mb"),
        "The append list is independent of the regex, so it still works");
  }

  /**
   * An unusable deny list allows nothing rather than everything.
   */
  @Test
  void testInvalidRegexInDenyList() {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.HIVE_COMPACTOR_CONF_DENYLIST, "([unclosed");
    assertFalse(allowed(conf, "mapreduce.map.memory.mb"), "Even ordinary tuning is rejected");
    conf.setVar(HiveConf.ConfVars.HIVE_COMPACTOR_CONF_ALLOWLIST_APPEND, "mapreduce.map.memory.mb");
    assertFalse(allowed(conf, "mapreduce.map.memory.mb"),
        "Even if the parameter is added to hive.compactor.conf.allowlist.append, it is still rejected.");
  }

  @Test
  void testEmptyKeyIsRejected() {
    HiveConf conf = new HiveConf();
    assertFalse(allowed(conf, ""), "Empty key is rejected");
    assertFalse(allowed(conf, " "), "Empty key is rejected");
  }

  private static boolean allowed(HiveConf conf, String key) {
    return CompactorPropertyValidator.isParameterAllowed(conf, key);
  }

}
