#!/usr/bin/env python

from pip.req import parse_requirements
import setuptools

# Filters out relative/local requirements (i.e. ../lib/utils)
remote_requirements = '\n'.join(str(r.req) for r in parse_requirements("requirements.txt", session='dummy') if r.req)

setuptools.setup(
    name='kafka-streams-examples-tests',
    version='3.3.0',

    author="Confluent, Inc.",
    author_email="tools@confluent.io",

    description='Docker image tests',

    url="https://github.com/confluentinc/kafka-streams-examples",

    install_requires=remote_requirements,

    packages=['test'],

    include_package_data=True,

    python_requires='>=2.7',
    setup_requires=['setuptools-git'],

)
