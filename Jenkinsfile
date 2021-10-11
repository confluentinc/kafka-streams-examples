#!/usr/bin/env groovy

dockerfile {
    dockerRepos = ['confluentinc/kafka-streams-examples']
    mvnPhase = 'package'  // streams examples integration-test needs host-based networking, won't work in CI as-is
    mvnSkipDeploy = true
    upstreamProjects = 'confluentinc/rest-utils'
    nodeLabel = 'docker-debian-jdk8-compose'
    cron = ''
    cpImages = true
    osTypes = ['ubi8']
    slackChannel = 'ksqldb-warn'
    nanoVersion = true
}
