import setuptools

setuptools.setup(
    name='kafka-streams-examples-tests',
    version='5.4.0-SNAPSHOT',

    author="Confluent, Inc.",
    author_email="tools@confluent.io",

    description='Docker image tests',

    url="https://github.com/confluentinc/kafka-streams-examples",

    dependency_links=open("requirements.txt").read().split("\n"),

    packages=['test'],

    include_package_data=True,

    python_requires='>=2.7',
    setup_requires=['setuptools-git'],

)
