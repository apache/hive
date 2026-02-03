#!/bin/bash
set -e

# Ensure SDKMAN is loaded
export SDKMAN_DIR="/usr/local/sdkman"
[[ -s "${SDKMAN_DIR}/bin/sdkman-init.sh" ]] && source "${SDKMAN_DIR}/bin/sdkman-init.sh"

echo "=== Installing Java versions ==="
# Install the necessary JDKs for Hive 3 and 4
echo "n" | sdk install java 11.0.29-amzn
echo "n" | sdk install java 17.0.17-amzn
echo "y" | sdk install java 8.0.472-amzn

echo "=== Installing Maven ==="
echo "y" | sdk install maven 3.9.12
echo "n" | sdk install maven 3.6.3

echo "=== Installing Just (task runner) ==="
# Install just from GitHub releases
JUST_VERSION=$(curl -s https://api.github.com/repos/casey/just/releases/latest | grep tag_name | head -1 | cut -d'"' -f4)
if [ -z "$JUST_VERSION" ]; then
    JUST_VERSION="1.28.0"
fi
JUST_URL="https://github.com/casey/just/releases/download/${JUST_VERSION}/just-${JUST_VERSION}-x86_64-unknown-linux-gnu.tar.gz"
wget -q -O /tmp/just.tar.gz "$JUST_URL" && tar -xzf /tmp/just.tar.gz -C /tmp && sudo mv /tmp/just /usr/local/bin/ && rm /tmp/just.tar.gz || echo "Warning: Could not install just"

echo "=== Fixing pentaho-aggdesigner-algorithm dependency ==="
# Fix the known issue with pentaho-aggdesigner-algorithm
# conjars.org is shutdown, so we download from mapr repo instead
mkdir -p /tmp/pentaho-fix
cd /tmp/pentaho-fix

PENTAHO_JAR="pentaho-aggdesigner-algorithm-5.1.5-jhyde.jar"
MAPR_URL="https://repository.mapr.com/nexus/content/groups/mapr-public/conjars/org/pentaho/pentaho-aggdesigner-algorithm/5.1.5-jhyde/${PENTAHO_JAR}"
BACKUP_URL="https://mvnrepository.com/artifact/org/pentaho/pentaho-aggdesigner-algorithm/5.1.5-jhyde"

if wget --timeout=5 -q -O "${PENTAHO_JAR}" "${MAPR_URL}" 2>/dev/null; then
    echo "Downloaded from mapr repo"
else
    echo "MapR repo failed, trying alternate source..."
    # Try alternate sources if mapr doesn't work
    wget -q -O "${PENTAHO_JAR}" "${BACKUP_URL}" 2>/dev/null || \
    wget -q -O "${PENTAHO_JAR}" "https://repo1.maven.org/maven2/conjars/org/pentaho/pentaho-aggdesigner-algorithm/5.1.5-jhyde/${PENTAHO_JAR}" 2>/dev/null || \
    echo "Warning: Could not download pentaho jar, it will be fetched during build"
fi

if [ -f "${PENTAHO_JAR}" ]; then
    mvn install:install-file \
        -DgroupId=org.pentaho \
        -DartifactId=pentaho-aggdesigner-algorithm \
        -Dversion=5.1.5-jhyde \
        -Dpackaging=jar \
        -Dfile="${PENTAHO_JAR}"
    rm -rf /tmp/pentaho-fix
fi

echo "=== Installing AWS tools ==="
# Install AWS CLI and assume role helper
sudo apt-get update && sudo apt-get install -y \
    awscli \
    jq \
    curl \
    git || true

echo "=== Setting bash as default shell ==="
sudo chsh -s /bin/bash vscode 2>/dev/null || true

echo "=== Configuring bash prompt ==="
if ! grep -q 'DEVPOD_WORKSPACE_ID' /home/vscode/.bashrc; then
    cat >> /home/vscode/.bashrc << 'EOF'

# Custom prompt for dev environments
if [ -n "$DEVPOD_WORKSPACE_ID" ]; then
    export PS1="\[\e[32m\]\u@\h\[\e[33m\]($PLAYGROUND_NAME)\[\e[34m\]:\w\[\e[0m\]\$ "
else
    export PS1="\[\e[32m\]\u@\h:\[\e[34m\]\w\[\e[0m\]\$ "
fi
EOF
    echo "Bash prompt configured"
fi

echo "=== Pre-fetching some Hive dependencies ==="
# Pre-fetch some Hive dependencies to save time
if [ -d "/workspaces/hive" ]; then
    cd /workspaces/hive
    mvn dependency:go-offline -DskipTests || true
else
    echo "Workspace not yet available, skipping dependency pre-fetch"
fi

echo "=== Setup complete ==="