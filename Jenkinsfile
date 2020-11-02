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

properties([
    // max 5 build/branch/day
    rateLimitBuilds(throttle: [count: 5, durationName: 'day', userBoost: true]),
    // do not run multiple testruns on the same branch
    disableConcurrentBuilds(),
    parameters([
        string(name: 'SPLIT', defaultValue: '20', description: 'Number of buckets to split tests into.'),
        string(name: 'OPTS', defaultValue: '', description: 'additional maven opts'),
    ])
])

this.prHead = null;
def checkPrHead() {
  if(env.CHANGE_ID) {
    println("checkPrHead - prHead:" + prHead)
    println("checkPrHead - prHead2:" + pullRequest.head)
    if (prHead == null) {
      prHead = pullRequest.head;
    } else {
      if(prHead != pullRequest.head) {
        currentBuild.result = 'ABORTED'
        error('Found new changes on PR; aborting current build')
      }
    }
  }
}
checkPrHead()

def setPrLabel(String prLabel) {
  if (env.CHANGE_ID) {
   def mapping=[
    "SUCCESS":"tests passed",
    "UNSTABLE":"tests unstable",
    "FAILURE":"tests failed",
    "PENDING":"tests pending",
   ]
   def newLabels = []
   for( String l : pullRequest.labels )
     newLabels.add(l)
   for( String l : mapping.keySet() )
     newLabels.remove(mapping[l])
   newLabels.add(mapping[prLabel])
   echo ('' +newLabels)
   pullRequest.labels=newLabels
  }
}

setPrLabel("PENDING");

def executorNode(run) {
  hdbPodTemplate {
    timeout(time: 24, unit: 'HOURS') {
      node(POD_LABEL) {
        container('hdb') {
          run()
        }
      }
    }
  }
}

def buildHive(args) {
  configFileProvider([configFile(fileId: 'artifactory', variable: 'SETTINGS')]) {
    withEnv(["MULTIPLIER=$params.MULTIPLIER","M_OPTS=$params.OPTS"]) {
      sh '''#!/bin/bash -e
ls -l
set -x
. /etc/profile.d/confs.sh
export USER="`whoami`"
export MAVEN_OPTS="-Xmx2g"
export -n HIVE_CONF_DIR
cp $SETTINGS .git/settings.xml
OPTS=" -s $PWD/.git/settings.xml -B -Dtest.groups= "
OPTS+=" -Pitests,qsplits,dist"
OPTS+=" -Dorg.slf4j.simpleLogger.log.org.apache.maven.plugin.surefire.SurefirePlugin=INFO"
OPTS+=" -Dmaven.repo.local=$PWD/.git/m2"
git config extra.mavenOpts "$OPTS"
OPTS=" $M_OPTS -Dmaven.test.failure.ignore "
if [ -s inclusions.txt ]; then OPTS+=" -Dsurefire.includesFile=$PWD/inclusions.txt";fi
if [ -s exclusions.txt ]; then OPTS+=" -Dsurefire.excludesFile=$PWD/exclusions.txt";fi
mvn $OPTS '''+args+'''
du -h --max-depth=1
df -h
'''
    }
  }
}

def hdbPodTemplate(closure) {
  podTemplate(
  containers: [
    containerTemplate(name: 'hdb', image: 'kgyrtkirk/hive-dev-box:executor', ttyEnabled: true, command: 'cat',
        alwaysPullImage: true,
        resourceRequestCpu: '1800m',
        resourceLimitCpu: '8000m',
        resourceRequestMemory: '6400Mi',
        resourceLimitMemory: '12000Mi'
    ),
  ], yaml:'''
spec:
  securityContext:
    fsGroup: 1000
  tolerations:
    - key: "type"
      operator: "Equal"
      value: "slave"
      effect: "PreferNoSchedule"
    - key: "type"
      operator: "Equal"
      value: "slave"
      effect: "NoSchedule"
  nodeSelector:
    type: slave
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
        checkPrHead()
        closure()
      }
    }
    finalLabel=currentBuild.currentResult
  } finally {
    setPrLabel(finalLabel)
  }
}

def saveWS() {
  sh '''#!/bin/bash -e
    tar --exclude=archive.tar -cf archive.tar .
    ls -l archive.tar
    rsync -rltDq --stats archive.tar rsync://rsync/data/$LOCKED_RESOURCE'''
}

def loadWS() {
  sh '''#!/bin/bash -e
    rsync -rltDq --stats rsync://rsync/data/$LOCKED_RESOURCE archive.tar
    tar -xf archive.tar'''
}

jobWrappers {

  def splits
  executorNode {
    container('hdb') {
      stage('Checkout') {
        checkout scm
      }
      stage('Compile') {
        buildHive("install -Dtest=noMatches")
      }
      checkPrHead()
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

  stage('Testing') {

    def branches = [:]
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
            stage('Archive') {
              junit '**/TEST-*.xml'
            }
          }
        }
      }
    }
    parallel branches
  }
}
