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
    branches["split${num}"] = {
      stage("Test #${num + 1}") {
      executorNode {
        //docker.image(image).inside {
//          stage('Preparation') {
            prepare()
            writeFile file: (split.includes ? inclusionsFile : exclusionsFile), text: split.list.join("\n")
            writeFile file: (split.includes ? exclusionsFile : inclusionsFile), text: ''
  //        }
        //  stage('Main') {
            realtimeJUnit(results) {
              run()
            }
//          }
        }
      }
    }
  }
  parallel branches
}


podTemplate(workspaceVolume: dynamicPVC(requestsSize: "16Gi"), containers: [
    containerTemplate(name: 'maven', image: 'cloudbees/jnlp-slave-with-java-build-tools', ttyEnabled: true, command: 'cat',
        resourceRequestCpu: '1500m',
        resourceLimitCpu: '4000m',
        resourceRequestMemory: '3000Mi',
        resourceLimitMemory: '8000Mi'
    ),
//    containerTemplate(name: 'maven', image: 'maven:3.3.9-jdk-8-alpine', ttyEnabled: true, command: 'cat'),
//    containerTemplate(name: 'golang', image: 'golang:1.8.0', ttyEnabled: true, command: 'cat')
  ], yaml:'''
spec:
  securityContext:
    fsGroup: 1000
''') {

properties([
    parameters([
        string(name: 'SPLIT', defaultValue: '3', description: 'Number of buckets to split tests into.'),
        string(name: 'OPTS', defaultValue: '-pl ql -am', description: 'additional maven opts')
    ])
])




node(POD_LABEL) {
stage('Prepare') {
  container('maven') {
    // FIXME can this be moved outside?
    configFileProvider([configFile(fileId: 'artifactory', variable: 'SETTINGS')]) {
    withEnv(["MULTIPLIER=$params.MULTIPLIER","M_OPTS=$params.OPTS"]) {

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

stage('Compile') {

  // FIXME: dup
    sh '''#!/bin/bash -e
OPTS=" -s $SETTINGS -B install -Dmaven.test.failure.ignore -Dtest.groups= "
OPTS+=" -Pitests -Pqsplits"
OPTS+=" -Dorg.slf4j.simpleLogger.log.org.apache.maven.plugin.surefire.SurefirePlugin=INFO"
OPTS+=" -Dmaven.repo.local=$PWD/.m2"
OPTS+=" $M_OPTS "
mvn $OPTS -Dtest=noMatches
du -h --max-depth=1
'''
    }
    sh '''rsync -q --daemon --config=rsyncd.conf --port 9873'''

  }
  }
  }
}




stage('Testing') {
  testInParallel(count(Integer.parseInt(params.SPLIT)), 'inclusions.txt', 'exclusions.txt', '**/target/surefire-reports/TEST-*.xml', 'maven:3.5.0-jdk-8', {
    sh  'rsync -arvv --stats rsync://$S:9873/ws .'
    sh 'du -h --max-depth=1'
  }, {
    configFileProvider([configFile(fileId: 'artifactory', variable: 'SETTINGS')]) {

      withEnv(["MULTIPLIER=$params.MULTIPLIER","M_OPTS=$params.OPTS"]) {
        sh '''#!/bin/bash -e
OPTS=" -s $SETTINGS -B install -Dmaven.test.failure.ignore -Dtest.groups= "
OPTS+=" -Pitests -Pqsplits"
OPTS+=" -Dorg.slf4j.simpleLogger.log.org.apache.maven.plugin.surefire.SurefirePlugin=INFO"
if [ -s inclusions.txt ]; then OPTS+=" -Dsurefire.includesFile=$PWD/inclusions.txt";fi
if [ -s exclusions.txt ]; then OPTS+=" -Dsurefire.excludesFile=$PWD/exclusions.txt";fi
echo "@INC"
cat inclusions.txt
echo "@ENC"
cat exclusions.txt
echo "@END"

OPTS+=" -Dmaven.repo.local=$PWD/.m2"
OPTS+=" $M_OPTS "
mvn $OPTS
du -h --max-depth=1
'''
      }
    }
  })
}

}


//jenkins/jnlp-slave:3.27-1
}


