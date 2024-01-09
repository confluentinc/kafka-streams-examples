#!/usr/bin/env groovy

dockerfile {
    dockerRepos = ['confluentinc/kafka-streams-examples']
    mvnPhase = 'package'  // streams examples integration-test needs host-based networking, won't work in CI as-is
    mvnSkipDeploy = true
    upstreamProjects = 'confluentinc/rest-utils'
    nodeLabel = 'docker-debian-jdk8-compose'
    cron = ''
    cpImages = true
    osTypes = ['deb9', 'ubi8']
    slackChannel = 'kafka-streams-quality'
    disableConcurrentBuilds = true
}
