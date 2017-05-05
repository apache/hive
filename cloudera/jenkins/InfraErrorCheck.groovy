import hudson.tasks.junit.TestResultAction;

String downstreamBaseUrl = "https://playground-01.jenkins.cloudera.com/job/Hive%20Build%20Failure%20Check/";
String downstreamBuildUrl = downstreamBaseUrl + "buildWithParameters?token=DnF8AkpfSaso8tEBtT5R";

InfraError[] INFRA_ERRORS = [
        new InfraError("No space left on device", "No space left on device", "folder.gif"),
        new InfraError("Aborting drone during", "Error creating drones", "warning.gif"),
        new InfraError("Error creating group", "Error creating drones", "warning.gif"),
        new InfraError("Failed to transfer file:", "Artifactory error", "warning.gif"),
        new InfraError("COMPILATION ERROR", "Compilation error", "error.gif"),
];


class InfraError {
    String pattern;
    String label;
    String badge;

    InfraError(String pattern, String label, String badge) {
        this.pattern = pattern;
        this.label = label;
        this.badge = badge;
    }
}

def String startDownstreamUrl(String url) {
    queueItemUrl = url.toURL();
    queueItemJobConnection = queueItemUrl.openConnection();
    String jobItemLocation = queueItemJobConnection. getHeaderField("Location");
    queueItemJobConnection.disconnect();

    // The build number is not available at trigger time, just after the job is started. We try to wait for the result
    sleep(10000);

    jobItemUrl = (jobItemLocation + "api/xml").toURL();
    jobDescription = jobItemUrl.getText();

    def leftItem = new XmlParser().parseText(jobDescription)
    return jobUrl = leftItem.executable.url.text();
}

for(InfraError error in INFRA_ERRORS) {
    if(manager.logContains(".*" + error.pattern + ".*")) {
        manager.build.setDescription(error.label);
        manager.build.setResult(error.result);

        manager.createSummary(error.badge).appendText("<h1>" + error.label + "</h1>", false, false, false, "black");
    }
}

TestResultAction testResults = manager.build.getAction(TestResultAction.class);
if(testResults!=null && testResults.getFailedTests().size()>0) {
    String buildUrl = manager.build.getEnvironment(manager.listener)['BUILD_URL'];
    String downstreamUrl = downstreamBuildUrl + "&BUILD_URL=" + buildUrl;
    manager.listener.logger.println("Calculated downstream job start url: " + downstreamUrl);

    def jobUrl = null;
    try {
        jobUrl = startDownstreamUrl(downstreamUrl);
    } catch(Exception ex) {
        manager.listener.logger.println("Exception " + ex);
    }
    manager.listener.logger.println("Downstream job url: " + jobUrl );

    manager.addBadge("db_out.gif", "Analyzis started");
    if(jobUrl?.trim()) {
        manager.build.setDescription("Analyzis is <a href='" + jobUrl + "'>here</a>");
    } else {
        manager.build.setDescription("Look for the results <a href='" + downstreamBaseUrl + "'>here</a>");
    }
}
