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

def discardDaysToKeep = '365'
def discardNumToKeep = '' // Unlimited
if (env.BRANCH_NAME != 'master') {
  discardDaysToKeep = '60'
  discardNumToKeep = '5'
}
properties([
    buildDiscarder(logRotator(daysToKeepStr: discardDaysToKeep, numToKeepStr: discardNumToKeep)),
    // max 5 build/branch/day
    rateLimitBuilds(throttle: [count: 5, durationName: 'day', userBoost: true]),
    // do not run multiple testruns on the same branch
    disableConcurrentBuilds(),
    parameters([
        string(name: 'SPLIT', defaultValue: '22', description: 'Number of buckets to split tests into.'),
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
    timeout(time: 12, unit: 'HOURS') {
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
set -x
. /etc/profile.d/confs.sh
export USER="`whoami`"
export MAVEN_OPTS="-Xmx2g"
export -n HIVE_CONF_DIR
cp $SETTINGS .git/settings.xml
OPTS=" -s $PWD/.git/settings.xml -B -Dtest.groups= "
OPTS+=" -Pitests,qsplits,dist,errorProne,iceberg"
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

def sonarAnalysis(args) {
  withCredentials([string(credentialsId: 'sonar', variable: 'SONAR_TOKEN')]) {
      def mvnCmd = """mvn org.sonarsource.scanner.maven:sonar-maven-plugin:3.9.1.2184:sonar \
      -Dsonar.organization=apache \
      -Dsonar.projectKey=apache_hive \
      -Dsonar.host.url=https://sonarcloud.io \
      """+args+" -DskipTests -Dit.skipTests -Dmaven.javadoc.skip"

      sh """#!/bin/bash -e
      sw java 17 && . /etc/profile.d/java.sh
      export MAVEN_OPTS=-Xmx5G
      """+mvnCmd
  }
}

def hdbPodTemplate(closure) {
  podTemplate(
  containers: [
    containerTemplate(name: 'hdb', image: 'wecharyu/hive-dev-box:executor', ttyEnabled: true, command: 'tini -- cat',
        alwaysPullImage: true,
        resourceRequestCpu: '1800m',
        resourceLimitCpu: '8000m',
        resourceRequestMemory: '6400Mi',
        resourceLimitMemory: '12000Mi',
        envVars: [
            envVar(key: 'DOCKER_HOST', value: 'tcp://localhost:2375')
        ]
    ),
    containerTemplate(name: 'dind', image: 'docker:18.05-dind',
        alwaysPullImage: true,
        privileged: true,
    ),
  ],
  volumes: [
    emptyDirVolume(mountPath: '/var/lib/docker', memory: false),
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
    time tar -xf archive.tar
    rm archive.tar
'''
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
        if(env.CHANGE_ID) {
          checkout([
            $class: 'GitSCM',
            branches: scm.branches,
            doGenerateSubmoduleConfigurations: scm.doGenerateSubmoduleConfigurations,
            extensions: scm.extensions,
            userRemoteConfigs: scm.userRemoteConfigs + [[
              name: 'origin',
              refspec: scm.userRemoteConfigs[0].refspec+ " +refs/heads/${CHANGE_TARGET}:refs/remotes/origin/target",
              url: scm.userRemoteConfigs[0].url,
              credentialsId: scm.userRemoteConfigs[0].credentialsId
            ]],
          ])
	  sh '''#!/bin/bash
set -e
echo "@@@ patches in the PR but not on target ($CHANGE_TARGET)"
git log --oneline origin/target..HEAD
echo "@@@ patches on target but not in the PR"
git log --oneline HEAD..origin/target
echo "@@@ merging target"
git config --global user.email "you@example.com"
git config --global user.name "Your Name"
git merge origin/target
'''

        } else {
          checkout scm
        }
      }
      stage('Prechecks') {
        def spotbugsProjects = [
            ":hive-common",
            ":hive-shims",
            ":hive-storage-api",
            ":hive-standalone-metastore-common",
            ":hive-service-rpc"
        ]
        sh '''#!/bin/bash
set -e
xmlstarlet edit -L `find . -name pom.xml`
git diff
n=`git diff | wc -l`
if [ $n != 0 ]; then
  echo "!!! incorrectly formatted pom.xmls detected; see above!" >&2
  exit 1
fi
'''
        buildHive("-Pspotbugs -pl " + spotbugsProjects.join(",") + " -am test-compile com.github.spotbugs:spotbugs-maven-plugin:4.0.0:check")
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

  def branches = [:]
  for (def d in ['derby','postgres',/*'mysql','oracle'*/]) {
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
export DOCKER_NETWORK=host
export DBNAME=metastore
reinit_metastore $dbType
time docker rm -f dev_$dbType || true
'''
          }
        }
      }
    }
  }
  branches['nightly-check'] = {
      executorNode {
        stage('Prepare') {
            loadWS();
        }
        stage('Build') {
            sh '''#!/bin/bash
set -e
dev-support/nightly
'''
            buildHive("install -Dtest=noMatches -Pdist -Piceberg -pl packaging -am")
        }
        stage('Verify') {
            sh '''#!/bin/bash
set -e
tar -xzf packaging/target/apache-hive-*-nightly-*-src.tar.gz
'''
            buildHive("install -Dtest=noMatches -Pdist,iceberg -f apache-hive-*-nightly-*/pom.xml")
        }
      }
  }
  branches['sonar'] = {
      executorNode {
          if(env.BRANCH_NAME == 'master') {
              stage('Prepare') {
                  loadWS();
              }
              stage('Sonar') {
                  sonarAnalysis("-Dsonar.branch.name=${BRANCH_NAME}")
              }
          } else if(env.CHANGE_ID) {
              stage('Prepare') {
                  loadWS();
              }
              stage('Sonar') {
                  sonarAnalysis("""-Dsonar.pullrequest.github.repository=apache/hive \
                                   -Dsonar.pullrequest.key=${CHANGE_ID} \
                                   -Dsonar.pullrequest.branch=${CHANGE_BRANCH} \
                                   -Dsonar.pullrequest.base=${CHANGE_TARGET} \
                                   -Dsonar.pullrequest.provider=GitHub""")
              }
          } else {
              echo "Skipping sonar analysis, we only run it on PRs and on the master branch, found ${env.BRANCH_NAME}"
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
                FAILED_FILES=`find . -name "TEST*xml" -exec grep -l "<failure" {} \\; 2>/dev/null | head -n 10`
                for a in \$FAILED_FILES
                do
                  RENAME_TMP=`echo \$a | sed s/TEST-//g`
                  mv \${RENAME_TMP/.xml/-output.txt} \${RENAME_TMP/.xml/-output-save.txt}
                done
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
  branches['javadoc-check'] = {
    executorNode {
      stage('Prepare') {
          loadWS();
      }
      stage('Generate javadoc') {
          sh """#!/bin/bash -e
mvn install javadoc:javadoc javadoc:aggregate -DskipTests -pl '!itests/hive-jmh,!itests/util'
"""
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
