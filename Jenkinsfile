
properties([
    // max 5 build/branch/day
//    rateLimitBuilds(throttle: [count: 5, durationName: 'day', userBoost: true]),
    // do not run multiple testruns on the same branch
//    disableConcurrentBuilds(),
    parameters([
        string(name: 'SPLIT', defaultValue: '1', description: 'Number of buckets to split tests into.'),
        string(name: 'OPTS', defaultValue: '-pl storage-api -am', description: 'additional maven opts'),
        string(name: 'SCRIPT', defaultValue: '', description: 'custom build script'),
    ])
])


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
    node(POD_LABEL) {
      container('hdb') {
        run()
      }
  }
}

// FIXME decomission this method
def testInParallel(parallelism, inclusionsFile, exclusionsFile, results, image, prepare, run) {
  def splits
  container('hdb') {
    splits = splitTests parallelism: parallelism, generateInclusions: true, estimateTestsFromFiles: true
  }
  def branches = [:]
  for (int i = 0; i < splits.size(); i++) {
    def num = i
    def split = splits[num]
    def splitName=String.format("split-%02d",num+1)
    branches[splitName] = {
      executorNode {
      	stage('Prepare') {
            prepare()
            writeFile file: (split.includes ? inclusionsFile : exclusionsFile), text: split.list.join("\n")
            writeFile file: (split.includes ? exclusionsFile : inclusionsFile), text: ''
        }
        try {
      		stage('Test') {
            run()
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


def buildHive(args) {
  configFileProvider([configFile(fileId: 'artifactory', variable: 'SETTINGS')]) {
    withEnv(["MULTIPLIER=$params.MULTIPLIER","M_OPTS=$params.OPTS"]) {
      sh '''#!/bin/bash -e
ls -l
set -x
. /etc/profile.d/confs.sh
export USER="`whoami`"
#export MAVEN_OPTS="-Xmx1333m"
export -n HIVE_CONF_DIR
#export HIVE_HOME="$PWD"
OPTS=" -s $SETTINGS -B -Dmaven.test.failure.ignore -Dtest.groups= "
OPTS+=" -Pitests,qsplits"
OPTS+=" -Dorg.slf4j.simpleLogger.log.org.apache.maven.plugin.surefire.SurefirePlugin=INFO"
OPTS+=" -Dmaven.repo.local=$PWD/.m2"
OPTS+=" $M_OPTS "
if [ -s inclusions.txt ]; then OPTS+=" -Dsurefire.includesFile=$PWD/inclusions.txt";fi
if [ -s exclusions.txt ]; then OPTS+=" -Dsurefire.excludesFile=$PWD/exclusions.txt";fi
#cd hive
mvn $OPTS '''+args+'''
du -h --max-depth=1
'''
    }
  }
}


def jobWrappers(closure) {
  // allocate 1 precommit token for the execution
  try{
    lock(label:'hive-precommit',quantity:1)  {
      timestamps {
        closure()
      }
    }
  } finally {
    setPrLabel(currentBuild.currentResult)
  }
}


def rsyncPodTemplate(closure) {
  podTemplate(
  containers: [
    containerTemplate(name: 'rsync', image: 'kgyrtkirk/htk-rsync:latest', ttyEnabled: true,
        alwaysPullImage: true,
        resourceRequestCpu: '1m',
        resourceLimitCpu: '100m',
        resourceRequestMemory: '10Mi',
    ),
  ]) {
    closure();
  }

}


def hdbPodTemplate(closure) {
  podTemplate(
  containers: [
    containerTemplate(name: 'hdb', image: 'kgyrtkirk/hive-dev-box:executor', ttyEnabled: true, command: 'cat',
        alwaysPullImage: true,
        resourceRequestCpu: '1300m',
        resourceLimitCpu: '3000m',
        resourceRequestMemory: '6750Mi',
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

jobWrappers {

// launch the "rsync" container to store build data
rsyncPodTemplate {
  node(POD_LABEL) {
    container('rsync') {
      stage('Prepare rsync') {
        sh '''printf 'env.S="%s"' "`hostname -i`" | tee /tmp/load.props'''
        load '/tmp/load.props'
        sh 'df -h /data'
      }
    }

    hdbPodTemplate {
      // launch the main pod
      node(POD_LABEL) {
        container('hdb') {
          stage('Checkout') {
            checkout scm
            // why dup?
            sh '''#!/bin/bash -e
                # make parallel-test-execution plugins source scanner happy ~ better results for 1st run
                find . -name '*.java'|grep /Test|grep -v src/test/java|grep org/apache|while read f;do t="`echo $f|sed 's|.*org/apache|happy/src/test/java/org/apache|'`";mkdir -p  "${t%/*}";touch "$t";done
            '''
          }
          stage('Compile') {
            buildHive("install -Dtest=noMatches")
            sh '''#!/bin/bash -e
                # make parallel-test-execution plugins source scanner happy ~ better results for 1st run
                find . -name '*.java'|grep /Test|grep -v src/test/java|grep org/apache|while read f;do t="`echo $f|sed 's|.*org/apache|happy/src/test/java/org/apache|'`";mkdir -p  "${t%/*}";touch "$t";done
            '''
          }
          stage('Upload') {
            sh  'rsync -arq --stats . rsync://$S:9873/data'
          }
        }
      }

      stage('Testing') {
        testInParallel(count(Integer.parseInt(params.SPLIT)), 'inclusions.txt', 'exclusions.txt', '**/target/surefire-reports/TEST-*.xml', 'maven:3.5.0-jdk-8', {
          sh  'rsync -arq --stats rsync://$S:9873/data .'
        }, {
          sh '''
echo "@INC"
cat inclusions.txt
echo "@ENC"
cat exclusions.txt
echo "@END"
'''
          buildHive("install -q")
          withEnv(["SCRIPT=$params.SCRIPT"]) {
            sh '''$SCRIPT'''
          }
       })
      }
    }
  }
}
}


