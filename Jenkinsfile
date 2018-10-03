#!/usr/bin/env groovy

dockerfile {
    dockerRepos = ['confluentinc/kafka-streams-examples']
    mvnPhase = 'package'  // streams examples integration-test needs host-based networking, won't work in CI as-is
    mvnSkipDeploy = true
    slackChannel = '#streams-team'
    upstreamProjects = 'confluentinc/rest-utils'
}
