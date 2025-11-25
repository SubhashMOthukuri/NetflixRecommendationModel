#!/bin/bash
# Run Bronze Pipeline with Java setup

export JAVA_HOME=$(/usr/libexec/java_home -v 11 2>/dev/null || echo "/opt/homebrew/opt/openjdk@11")
export PATH="$JAVA_HOME/bin:$PATH"

echo "Java: $(java -version 2>&1 | head -1)"
echo "Starting Bronze Pipeline..."
echo ""

python -m data_streaming.bronze_ingestion.jobs.launchers.job_launcher \
    --topic user_events \
    --environment dev \
    --max-retries 1

