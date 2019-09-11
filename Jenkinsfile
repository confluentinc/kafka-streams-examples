#!/usr/bin/env groovy

docker_oraclejdk8 {
    dockerRepos = ['confluentinc/kafka-streams-examples']
    dockerRegistry = '368821881613.dkr.ecr.us-west-2.amazonaws.com/'
    dockerUpstreamRegistry = 'docker.io/'  // Temporary; use public images until new base images for trunk are published
    dockerUpstreamTag = 'latest'  // Temporary; use trunk-latest when available
    mvnPhase = 'package'  // streams examples integration-test needs host-based networking, won't work in CI as-is
    mvnSkipDeploy = true
    // add for integration-test
    // nodeLabel = 'docker-oraclejdk8-compose'
    upstreamProjects = 'confluentinc/rest-utils'
    withPush = true
}
