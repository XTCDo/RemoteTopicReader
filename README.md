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

## Configuration
An example config file looks like this:
```
[configuration]
bootstrap_server = domain.name:port
topic = topic_name
```
The `topic` config is optional and can be left out.

To generate an empty config file in `~/.config/rtr/`:
```
rtr --init-config
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
* Print only last *n* records
* Listen to multiple records
~~* Configuration file for often-used connections/topics~~
~~* Better help function~~
