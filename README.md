# RemoteTopicReader
A small python program to read out records on an Apache Kafka server.

## Usage
To simply listen on any topic on a given Kafka server (default port value is *9092*):
```bash
rtr -u url_to_kafka_bootstrap_server:port -t topic
```

To print a list of topics:
```bash
rtr -u url_to_kafka_bootstrap_server:port -l
```

More information:
```bash
rtr -h
```

## Installation
```bash
git clone https://github.com/xtcdo/RemoteTopicReader.git
cd RemoteTopicReader
sudo pip install .
```

## Uninstallation
```bash
sudo pip uninstall RemoteTopicReader
```

## Todo
* Better help function
* Print only last *n* records
* Listen to multiple records
* Configuration file for often-used connections/topics