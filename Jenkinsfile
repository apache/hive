
def testInParallel(parallelism, inclusionsFile, exclusionsFile, results, image, prepare, run) {
  def splits
  node {
    prepare()
    splits = splitTests parallelism: parallelism, generateInclusions: true, estimateTestsFromFiles: true
  }
  def branches = [:]
  for (int i = 0; i < splits.size(); i++) {
    def num = i
    def split = splits[num]
    branches["split${num}"] = {
      stage("Test #${num + 1}") {
node {
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

properties([
    parameters([
        string(name: 'MULTIPLIER', defaultValue: '1', description: 'Factor by which to artificially slow down tests.'),
        string(name: 'SPLIT', defaultValue: '5', description: 'Number of buckets to split tests into.')
    ])
])


stage('Testing') {
  testInParallel(count(Integer.parseInt(params.SPLIT)), 'inclusions.txt', 'exclusions.txt', 'target/surefire-reports/TEST-*.xml', 'maven:3.5.0-jdk-8', {
    checkout scm
//    unstash 'sources'
  }, {
    configFileProvider([configFile(fileId: 'artifactory', variable: 'SETTINGS')]) {
      withEnv(["MULTIPLIER=$params.MULTIPLIER"]) {
        sh 'mvn -s $SETTINGS -B test -Dmaven.test.failure.ignore'
      }
    }
  })
}


//jenkins/jnlp-slave:3.27-1
