def executorNode(run) {
  stage("An Executor") {
    node(POD_LABEL) {
      container('maven') {
        run()
      }
    }
  }
}


def testInParallel(parallelism, inclusionsFile, exclusionsFile, results, image, prepare, run) {
  //def splits
  executorNode {
    prepare()
    splits = splitTests parallelism: parallelism, generateInclusions: true, estimateTestsFromFiles: true
  }
  def branches = [:]
  for (int i = 0; i < 3;/*splits.size()*/ i++) {
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



podTemplate(containers: [
    containerTemplate(name: 'maven', image: 'cloudbees/jnlp-slave-with-java-build-tools', ttyEnabled: true, command: 'cat'),
//    containerTemplate(name: 'maven', image: 'maven:3.3.9-jdk-8-alpine', ttyEnabled: true, command: 'cat'),
//    containerTemplate(name: 'golang', image: 'golang:1.8.0', ttyEnabled: true, command: 'cat')
  ]) {

properties([
    parameters([
        string(name: 'MULTIPLIER', defaultValue: '1', description: 'Factor by which to artificially slow down tests.'),
        string(name: 'SPLIT', defaultValue: '5', description: 'Number of buckets to split tests into.')
    ])
])


/*
node(POD_LABEL) {
  container('maven') {
  	sh 'ls -l'
  }
}
*/

stage('Testing') {
  testInParallel(count(Integer.parseInt(params.SPLIT)), 'inclusions.txt', 'exclusions.txt', '**/target/surefire-reports/TEST-*.xml', 'maven:3.5.0-jdk-8', {
//    checkout scm
//    unstash 'sources'
  }, {
    configFileProvider([configFile(fileId: 'artifactory', variable: 'SETTINGS')]) {
      withEnv(["MULTIPLIER=$params.MULTIPLIER"]) {
        sh 'pwd'
        sh 'git clone -b pipe1 https://github.com/kgyrtkirk/pipeline-test'
        sh 'cd pipeline-test;mvn -s $SETTINGS -B install -Dmaven.test.failure.ignore -Dtest.groups= '
      }
    }
  })
}



//jenkins/jnlp-slave:3.27-1
}

