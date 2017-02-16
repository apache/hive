metainfo = """<?xml version="1.0"?>
<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

<metainfo>
  <schemaVersion>2.0</schemaVersion>
  <application>
    <name>LLAP</name>
    <comment>LLAP is a daemon service that works with a cache and works on SQL constructs.</comment>
    <version>%(version)s</version>
    <exportedConfigs>None</exportedConfigs>
    <exportGroups>
      <exportGroup>
        <name>Servers</name>
        <exports>
          <export>
            <name>instances</name>
            <value>${LLAP_HOST}:${site.global.listen_port}</value>
          </export>
        </exports>
      </exportGroup>
    </exportGroups>

    <components>
      <component>
        <name>LLAP</name>
        <category>MASTER</category>
        <compExports>Servers-instances</compExports>
        <commandScript>
          <script>scripts/llap.py</script>
          <scriptType>PYTHON</scriptType>
        </commandScript>
      </component>
    </components>

    <osSpecifics>
      <osSpecific>
        <osType>any</osType>
        <packages>
          <package>
            <type>tarball</type>
            <name>files/llap-%(version)s.tar.gz</name>
          </package>
        </packages>
      </osSpecific>
    </osSpecifics>

  </application>
</metainfo>
"""

appConfig = """
{
  "schema": "http://example.org/specification/v2.0.0",
  "metadata": {
  },
  "global": {
    "application.def": ".slider/package/LLAP/llap-%(version)s.zip",
    "java_home": "%(java_home)s",
    "site.global.app_user": "yarn",
    "site.global.app_root": "${AGENT_WORK_ROOT}/app/install/",
    "site.global.app_tmp_dir": "${AGENT_WORK_ROOT}/tmp/",
    "site.global.app_logger": "%(daemon_logger)s",
    "site.global.app_log_level": "%(daemon_loglevel)s",
    "site.global.additional_cp": "%(hadoop_home)s",
    "site.global.daemon_args": "%(daemon_args)s",
    "site.global.library_path": "%(hadoop_home)s/lib/native",
    "site.global.memory_val": "%(heap)d",
    "site.global.pid_file": "${AGENT_WORK_ROOT}/app/run/llap-daemon.pid",
    "internal.chaos.monkey.probability.amlaunchfailure": "0",
    "internal.chaos.monkey.probability.containerfailure": "%(monkey_percentage)d",
    "internal.chaos.monkey.interval.seconds": "%(monkey_interval)d",
    "internal.chaos.monkey.enabled": "%(monkey_enabled)s"%(slider_appconfig_global_append)s

  },
  "components": {
    "slider-appmaster": {
      "jvm.heapsize": "%(slider_am_jvm_heapsize)dM",
      "slider.hdfs.keytab.dir": "%(slider_keytab_dir)s",
      "slider.am.login.keytab.name": "%(slider_keytab)s",
      "slider.keytab.principal.name": "%(slider_principal)s"
    }
  }
}
"""

resources = """
{
  "schema" : "http://example.org/specification/v2.0.0",
  "metadata" : {
  },
  "global" : {
    "yarn.log.include.patterns": ".*\\\\.done"
  },
  "components": {
    "slider-appmaster": {
      "yarn.memory": "%(slider.am.container.mb)d",
      "yarn.component.instances": "1"
    },
    "LLAP": {
      "yarn.role.priority": "1",
      "yarn.component.instances": "%(instances)d",
      "yarn.resource.normalization.enabled": "false",
      "yarn.memory": "%(container.mb)d",
      "yarn.component.placement.policy" : "%(placement)d"
    }
  }
}
"""
# placement policy "4" is a bit-mask
# only bit set is Slider PlacementPolicy.ANTI_AFFINITY_REQUIRED(4)

runner = """
#!/bin/bash -e

BASEDIR=$(dirname $0)
slider stop %(name)s --wait 10 || slider stop %(name)s --force --wait 30
slider destroy %(name)s --force || slider destroy %(name)s
slider install-package --name LLAP --package  $BASEDIR/llap-%(version)s.zip --replacepkg
slider create %(name)s --resources $BASEDIR/resources.json --template $BASEDIR/appConfig.json %(queue.string)s
"""
