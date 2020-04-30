

def executorNode(run) {
    node(POD_LABEL) {
      container('maven') {
        timestamps {
          run()
        }
      }
  }
}

// FIXME decomission this method
def testInParallel(parallelism, inclusionsFile, exclusionsFile, results, image, prepare, run) {
  def splits
  container('maven') {
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
export MAVEN_OPTS="-Xmx1333m"
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

podTemplate(
  //workspaceVolume: dynamicPVC(requestsSize: "16Gi"),
  containers: [
  //cloudbees/jnlp-slave-with-java-build-tools
  //kgyrtkirk/hive-dev-box:executor
    containerTemplate(name: 'maven', image: 'kgyrtkirk/hive-dev-box:executor', ttyEnabled: true, command: 'cat',
        alwaysPullImage: true,
        resourceRequestCpu: '900m',
        resourceLimitCpu: '3000m',
        resourceRequestMemory: '4500Mi',
        resourceLimitMemory: '10000Mi'
    ),
//    containerTemplate(name: 'maven', image: 'maven:3.3.9-jdk-8-alpine', ttyEnabled: true, command: 'cat'),
//    containerTemplate(name: 'golang', image: 'golang:1.8.0', ttyEnabled: true, command: 'cat')
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

properties([
    parameters([
        string(name: 'SPLIT', defaultValue: '29', description: 'Number of buckets to split tests into.'),
        string(name: 'OPTS', defaultValue: '', description: 'additional maven opts'),
        string(name: 'SCRIPT', defaultValue: '', description: 'custom build script'),
    ])
])

// launch the main pod
node(POD_LABEL) {
  container('maven') {
stage('Prepare') {
    // FIXME can this be moved outside?

    checkout scm
    sh '''printf 'env.S="%s"' "`hostname -i`" >> /home/jenkins/agent/load.props'''
    sh '''cat /home/jenkins/agent/load.props'''
    load '/home/jenkins/agent/load.props'
    sh 'df -h'
    sh '''echo S==$S'''
    sh '''cat << EOF > rsyncd.conf
[ws]
path = $PWD
read only = true
timeout = 300
use chroot = false
EOF
cat rsyncd.conf
'''
    sh '''rsync --daemon --config=rsyncd.conf --port 9873'''
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
}




stage('Testing') {
  testInParallel(count(Integer.parseInt(params.SPLIT)), 'inclusions.txt', 'exclusions.txt', '**/target/surefire-reports/TEST-*.xml', 'maven:3.5.0-jdk-8', {
    sh  'rsync -arvvq --stats rsync://$S:9873/ws .'
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


//jenkins/jnlp-slave:3.27-1
}


