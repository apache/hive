class ApacheHiveBeeline < Formula
  desc "Hive Beeline JDBC command-line client"
  homepage "https://hive.apache.org/"
  url "https://repo1.maven.org/maven2/org/apache/hive/hive-beeline/4.2.1/hive-beeline-4.2.1-standalone.jar"
  sha256 "REPLACE_WITH_SHA256_FROM_hive-beeline-4.2.1-standalone.jar.sha256"
  license "Apache-2.0"

  livecheck do
    url "https://repo1.maven.org/maven2/org/apache/hive/hive-beeline/maven-metadata.xml"
    regex(%r{<release>(.+?)</release>}i)
  end

  depends_on "openjdk@21"

  def install
    libexec.install "hive-beeline-#{version}-standalone.jar" => "hive-beeline-standalone.jar"

    java_opts = %w[
      java.nio java.net java.lang java.util
      java.util.concurrent java.util.concurrent.atomic
      java.util.regex java.lang.reflect java.io
    ].map { |pkg| "--add-opens=java.base/#{pkg}=ALL-UNNAMED" }.join(" ")
    java_opts += " -Dlog4j.configurationFile=beeline-log4j2.properties"

    write_jar_script libexec/"hive-beeline-standalone.jar", "beeline",
                      java_version: "21", java_opts: java_opts
  end

  test do
    output = shell_output("#{bin}/beeline --help 2>&1", 1)
    assert_match "Usage: java org.apache.hive.beeline.BeeLine", output
  end
end
