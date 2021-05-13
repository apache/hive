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

// NOTE: this job expects an OPTS and a SPLITS parameter to be provided in the job configuration
// because of some issues with the gerrit trigger plugin this cant be provided here

if(env.GERRIT_CHANGE_NUMBER != null) {
    currentBuild.displayName = "#${BUILD_NUMBER} [${GERRIT_BRANCH}] ${GERRIT_PATCHSET_UPLOADER_NAME}"
    currentBuild.description = "${GERRIT_CHANGE_SUBJECT}"

    def response = httpRequest "https://gerrit.sjc.cloudera.com/changes/${GERRIT_CHANGE_NUMBER}/revisions/${GERRIT_PATCHSET_REVISION}/related"
    def nParts=response.content.split("${GERRIT_PATCHSET_REVISION}").length
    if(nParts>2) {
      currentBuild.result = 'NOT_BUILT'
      println('Non tip changeset detected - skipping build!')
      return
    } else {
      println("The current patchset is a TIP ${GERRIT_PATCHSET_REVISION}!")
    }
}

def executorNode(run) {
  hdbPodTemplate {
    timeout(time: 6, unit: 'HOURS') {
      node(POD_LABEL) {
        container('hdb') {
          run()
        }
      }
    }
  }
}

// returns a map like: {tez: {user: "CDH", branch: "cdpd-master", component: "tez"}, hadoop: {user: "CDH", branch: "cdpd-master", component: "hadoop"}, ...}
def parseCustomComponentsBuilds(description) {
  if (!description?.trim()) {
    return false
  }
  def customComponents = [:]
  def supportedComponents = ["HADOOP", "TEZ", "ORC", "CALCITE"];

  for(String word : description.split(" ")){
    if (word.startsWith("BUILD_CUSTOM_")){
      component = word.split(":")[0].split("_")[2]; //BUILD_CUSTOM_TEZ:user:branch -> TEZ
      if (!supportedComponents.contains(component)){
          throw new Exception("Not supported custom component '" + component +"', supported components are: " + supportedComponents)
      }
      if (word.split(":").length < 3){
          throw new Exception ("Not enough parts found to describe custom component build from string: " + word)
      }
      customComponents[component.toLowerCase()] = [component: component.toLowerCase(), user: word.split(":")[1], branch: word.split(":")[2]]
    }
  }

  println ("Precommit will attempt to build custom components: " + customComponents)
  return customComponents;
}

def buildCustomComponent(info, mavenOpts, buildSubDir=".") {
  if (!info){
    println ("skipping custom component build, info is not found...")
    return
  }
  configFileProvider([configFile(fileId: 'artifactory', variable: 'SETTINGS')]) {
    withEnv(["BUILD_SUB_DIR=$buildSubDir", "COMPONENT=$info.component", "GIT_USER=$info.user", "GIT_BRANCH=$info.branch"]) {
      sh '''#!/bin/bash -e
. /etc/profile.d/confs.sh
set -x

HIVE_DIR=$PWD
cd ..

git clone --branch $GIT_BRANCH --single-branch http://github.infra.cloudera.com/$GIT_USER/$COMPONENT
cd $COMPONENT/$BUILD_SUB_DIR
git branch
git status
git log -1 --pretty="%h %B"
http_proxy=http://sustwork.bdp.cloudera.com:3128 cdpd-patcher $COMPONENT
cp $SETTINGS $HIVE_DIR/.git/settings.xml
mvn clean install -DskipTests -Dmaven.repo.local=$HIVE_DIR/.git/m2 -s $HIVE_DIR/.git/settings.xml -q '''+mavenOpts+'''
'''
    }
  }
}

def buildHive(args,wrapper="") {
  configFileProvider([configFile(fileId: 'artifactory', variable: 'SETTINGS')]) {
    withEnv(["MULTIPLIER=$params.MULTIPLIER","M_OPTS=$params.OPTS"]) {
      sh '''#!/bin/bash -e
ls -l
echo "@state"
git branch
git status
set
sysctl net.core.somaxconn
set -x
. /etc/profile.d/confs.sh
function retry() { n=$1;shift;for((i=0;i<n;i++));do r=0;"$@" || r=$?; [ "$r" == "0" ] && break;echo "@@@ try#$[ $i + 1 ]/$n failed with $r";done ; return $r;}
export USER="`whoami`"
export MAVEN_OPTS="-Xmx2g"
export -n HIVE_CONF_DIR
cp $SETTINGS .git/settings.xml
OPTS=" -s $PWD/.git/settings.xml  -Dtest.groups= "
OPTS+=" -Pitests,qsplits,dist"
OPTS+=" -Denforcer.skip"
OPTS+=" -Dorg.slf4j.simpleLogger.log.org.apache.maven.plugin.surefire.SurefirePlugin=INFO"
OPTS+=" -Dmaven.wagon.http.retryHandler.count=3"
OPTS+=" -Dmaven.wagon.http.retryHandler.class=standard"
OPTS+=" -Dmaven.repo.local=$PWD/.git/m2"
#OPTS+=" -Dsurefire.rerunFailingTestsCount=1"
git config extra.mavenOpts "$OPTS"


OPTS=" $M_OPTS -Dmaven.test.failure.ignore "
if [ -s inclusions.txt ]; then OPTS+=" -Dsurefire.includesFile=$PWD/inclusions.txt";fi
if [ -s exclusions.txt ]; then OPTS+=" -Dsurefire.excludesFile=$PWD/exclusions.txt";fi
'''+wrapper+''' mvn $OPTS '''+args+'''
du -h --max-depth=1
df -h
'''
    }
  }
}

def hdbPodTemplate(closure) {
  podTemplate(
  name: "${JOB_NAME}-${BUILD_NUMBER}".replaceAll("[^_a-zA-Z0-9-]", "_"),
  containers: [
    containerTemplate(name: 'hdb', image: 'docker-sandbox.infra.cloudera.com/hive/hive-dev-box:executor', ttyEnabled: true, command: 'tini -- cat',
        alwaysPullImage: true,
        resourceRequestCpu: '2800m',
        resourceLimitCpu: '8000m',
        resourceRequestMemory: '6400Mi',
        resourceLimitMemory: '12000Mi',
        envVars: [
            envVar(key: 'DOCKER_HOST', value: 'tcp://localhost:2375')
        ]
    ),
    containerTemplate(name: 'dind', image: 'docker:18.05-dind',
//        alwaysPullImage: true,
        privileged: true,
    ),
  ],
  volumes: [
    emptyDirVolume(mountPath: '/var/lib/docker', memory: false),
  ], yaml:'''
spec:
  securityContext:
        fsGroup: 1000
        sysctls:
        - name: net.ipv4.tcp_rmem
          value: "4096 87380 16777216"
        - name: net.ipv4.tcp_wmem
          value: "4096 16384 16777216"
        - name: net.core.somaxconn
          value: "256000"
        - name: net.ipv4.tcp_syncookies
          value: "1"
        - name: net.ipv4.ip_local_port_range
          value: "1024 65535"
        - name: net.ipv4.tcp_tw_reuse
          value: "1"
        - name: net.ipv4.tcp_congestion_control
          value: "cubic"
        - name: net.ipv4.tcp_max_syn_backlog
          value: "256000"
  tolerations:
    - key: "type"
      operator: "Equal"
      value: "slave"
      effect: "PreferNoSchedule"
    - key: "type"
      operator: "Equal"
      value: "slave"
      effect: "NoSchedule"
''') {
    closure();
  }
}

def jobWrappers(closure) {
  def finalLabel="FAILURE";
  try {
    // allocate 1 precommit token for the execution
    lock(label:'hive-precommit', quantity:1, variable: 'LOCKED_RESOURCE')  {
      timestamps {
        echo env.LOCKED_RESOURCE
        closure()
      }
    }
    finalLabel=currentBuild.currentResult
  } finally {
  }
}

def saveWS() {
  sh '''#!/bin/bash -ex
    function retry() { n=$1;shift;for((i=0;i<n;i++));do r=0;"$@" || r=$?; [ "$r" == "0" ] && break;echo "@@@ try#$[ $i + 1 ]/$n failed with $r";done ; return $r;}
    time tar --exclude=archive.tar -cf archive.tar .
    ls -l archive.tar
    time retry 3 rsync -rltDq --stats archive.tar rsync://rsync/data/$LOCKED_RESOURCE'''
}

def loadWS() {
  sh '''#!/bin/bash -ex
    function retry() { n=$1;shift;for((i=0;i<n;i++));do r=0;"$@" || r=$?; [ "$r" == "0" ] && break;echo "@@@ try#$[ $i + 1 ]/$n failed with $r";done ; return $r;}
    time retry 3 rsync -rltDq --stats rsync://rsync/data/$LOCKED_RESOURCE archive.tar
    time tar -xf archive.tar'''
}

def saveFile(name) {
  sh """#!/bin/bash -e
    rsync -rltDq --stats ${name} rsync://rsync/data/$LOCKED_RESOURCE.${name}"""
}

def loadFile(name) {
  sh """#!/bin/bash -e
    rsync -rltDq --stats rsync://rsync/data/$LOCKED_RESOURCE.${name} ${name}"""
}


jobWrappers {

  def splits
  executorNode {
    container('hdb') {
      stage('Checkout') {
        checkout scm
        if( env.GERRIT_PATCHSET_REVISION != null ) {
          println "@gerrit patchset"
          checkout scm
          sh '''#!/bin/bash -e
echo "@ prepare branch"
git config user.email "you@example.com"
git config user.name "Your Name"
git status
echo "@ on patch branch only"
git log --oneline gerrit/target..gerrit/patch
echo "@ on target branch only"
git log --oneline gerrit/patch..gerrit/target
echo "@ prepare branch"
git merge gerrit/patch
git merge gerrit/target
echo "@ merged"
'''
        }
      }
      stage('Prepare sources') {
        sh '''#!/bin/bash -e

http_proxy=http://sustwork.bdp.cloudera.com:3128 cdpd-patcher hive $VERSION
        '''
      }
      if(false)
      stage('Prechecks') {
        def spotbugsProjects = [
            ":hive-common",
            ":hive-shims",
            ":hive-storage-api",
            ":hive-standalone-metastore-common"
        ]
        buildHive("-Pspotbugs -pl " + spotbugsProjects.join(",") + " -am test-compile com.github.spotbugs:spotbugs-maven-plugin:4.0.0:check")
      }
      stage('Compile') {
        //description = 'TICKET-1234: my commit message BUILD_CUSTOM_HADOOP:CDH:cdpd-master BUILD_CUSTOM_TEZ:CDH:cdpd-master BUILD_CUSTOM_ORC:CDH:cdpd-master BUILD_CUSTOM_CALCITE:CDH:cdpd-master'
        description = env.GERRIT_CHANGE_SUBJECT

        customComponentBuilds = parseCustomComponentsBuilds(description)

        println ("Attempting to build hadoop: ")
        buildCustomComponent(customComponentBuilds.get("hadoop"), "")

        println ("Attempting to build tez: ")
        buildCustomComponent(customComponentBuilds.get("tez"), "-pl '!tez-ui'")

        println ("Attempting to build orc: ")
        buildCustomComponent(customComponentBuilds.get("orc"), "", "java")

        println ("Attempting to build calcite: ")
        buildCustomComponent(customComponentBuilds.get("calcite"), "")

        buildHive("install -Dtest=TestParseDriver#nonExistent","retry 3")
        buildHive("org.apache.maven.plugins:maven-dependency-plugin:get -Dartifact=org.apache.maven.plugins:maven-antrun-plugin:1.8","retry 3")
      }

      stage('Upload') {
        saveWS()
        sh '''#!/bin/bash -e
            # make parallel-test-execution plugins source scanner happy ~ better results for 1st run
            find . -name '*.java'|grep /Test|grep -v src/test/java|grep org/apache|while read f;do t="`echo $f|sed 's|.*org/apache|happy/src/test/java/org/apache|'`";mkdir -p  "${t%/*}";touch "$t";done
        '''
        splits = splitTests parallelism: count(Integer.parseInt(params.SPLIT)), generateInclusions: true, estimateTestsFromFiles: true
      }
    }
  }

  def branches = [:]
if(false)
    for (def d in ['derby']) {
    def dbType=d
    def splitName = "init@$dbType"
    branches[splitName] = {
      executorNode {
        stage('Prepare') {
            loadWS();
        }
        stage('init-metastore') {
           withEnv(["dbType=$dbType"]) {
             sh '''#!/bin/bash -e
set -x
echo 127.0.0.1 dev_$dbType | sudo tee -a /etc/hosts
. /etc/profile.d/confs.sh
sw hive-dev $PWD
ping -c2 dev_$dbType
export DOCKER_NETWORK=host
reinit_metastore $dbType
'''
          }
        }
      }
    }
  }
  for (int i = 0; i < splits.size(); i++) {
    def num = i
    def split = splits[num]
    def splitName=String.format("split-%02d",num+1)
    branches[splitName] = {
      executorNode {
        stage('Prepare') {
            loadWS();
            writeFile file: (split.includes ? "inclusions.txt" : "exclusions.txt"), text: split.list.join("\n")
            writeFile file: (split.includes ? "exclusions.txt" : "inclusions.txt"), text: ''
            sh '''echo "@INC";cat inclusions.txt;echo "@EXC";cat exclusions.txt;echo "@END"'''
        }
        try {
          stage('Test') {
            buildHive("org.apache.maven.plugins:maven-antrun-plugin:run@{define-classpath,setup-test-dirs,setup-metastore-scripts} org.apache.maven.plugins:maven-surefire-plugin:test -q")
          }
        } finally {
          stage('PostProcess') {
            try {
              sh """#!/bin/bash -e
                # removes all stdout and err for passed tests
                xmlstarlet ed -L -d 'testsuite/testcase/system-out[count(../failure)=0]' -d 'testsuite/testcase/system-err[count(../failure)=0]' `find . -name 'TEST*xml' -path '*/surefire-reports/*'`
                # remove all output.txt files
                find . -name '*output.txt' -path '*/surefire-reports/*' -exec unlink "{}" \\;
              """
            } finally {
              def fn="${splitName}.tgz"
              sh """#!/bin/bash -e
              tar -czf ${fn} --files-from  <(find . -path '*/surefire-reports/*')"""
              saveFile(fn)
              junit '**/TEST-*.xml'
            }
          }
        }
      }
    }
  }
  try {
    stage('Testing') {
      parallel branches
    }
  } finally {
    stage('Archive') {
      executorNode {
        for (int i = 0; i < splits.size(); i++) {
          def num = i
          def splitName=String.format("split-%02d",num+1)
          def fn="${splitName}.tgz"
          loadFile(fn)
          sh("""#!/bin/bash -e
              mkdir ${splitName}
              tar xzf ${fn} -C ${splitName}
              unlink ${fn}""")
        }
        sh("""#!/bin/bash -e
        tar czf test-results.tgz split*""")
        archiveArtifacts artifacts: "**/test-results.tgz"
      }
    }
  }
}
