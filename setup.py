from setuptools import setup

setup(name='RemoteTopicReader',
      version='0.1',
      description='Quicly print topics and their contents from an Apache Kafka server',
      author='xtcdo',
      licence='MIT',
      packages=['RemoteTopicReader'],
      zip_safe=False,
      scripts=['bin/rtr'],
      install_requires=['kafka', 'kafka-python'])
