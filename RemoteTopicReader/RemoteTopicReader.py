"""
Author: xtcdo
Version: 0.2

Print records on a given topic on an Apache Kafka server.
"""

from argparse import ArgumentParser, RawTextHelpFormatter
from kafka import KafkaConsumer
from time import sleep
import configparser
import os


def check_config(config_file_name):
    """
    Check if a configuration file is valid
    :param config_file_name: name of the configuration file to check
    :return: True/False for file validity and a message if the file is invalid
    """
    config_file = get_config_file(config_file_name)

    if config_file is None:
        return False, "Configuration file ~/.config/rtr/%s not found" % config_file_name

    if config_file['configuration'] is None:
        return False, "Configuration file must begin with [configuration]"

    if config_file['configuration']['bootstrap_server'] is None:
        return False, "Configuration file must contain a bootstrap_server entry"

    return True, ""


def list_topics(consumer):
    """
    Prints a list of topics found on the Apache Kafka server on the provided url:port
    :param bootstrap_server: The url:port of the Apache Kafka server
    """

    topics = sorted(consumer.topics())
    print('Topics:')

    for topic in topics:
        print('  - %s' % topic)

    consumer.close()


def print_record(record_to_print, verbosity):
    """
    Prints a record at a specified level of verbosity
    For all levels of verbosity the key of record_to_print will be printed only if it is not None
    If verbosity is None: Print the value and key
    If verbosity is 1: Print the topic, value and key
    If verbosity is 2: Print the topic, partition, offset, value and key
    :param record_to_print: The record to print
    :param verbosity: The level of verbosity
    """
    if verbosity is None:
        if record_to_print.key is None:
            print('%s' % record_to_print.value)
        else:
            print('%s : %s' % (record_to_print.key, record_to_print.value))

    elif verbosity is 1:
        if record_to_print.key is None:
            print('[%s] %s' % (record_to_print.topic, record_to_print.value))
        else:
            print('[%s] %s : %s' % (record_to_print.topic, record_to_print.key, record_to_print.value))

    elif verbosity is 2:
        if record_to_print.key is None:
            print('[%s:%d:%d] %s' % (record_to_print.topic, record_to_print.partition, record_to_print.offset,
                                     record_to_print.value))
        else:
            print('[%s:%d:%d] %s : %s' % (record_to_print.topic, record_to_print.partition, record_to_print.offset,
                                          record_to_print.key, record_to_print.value))


def print_records(topic, verbosity, consumer):
    """
    Print records on a specified topic found on an Apache Kafka server at the url in bootstrap_server at a specified
    level of verbosity. Enters an infinite loop that has to be exited by KeyboardInterrupt
    :param bootstrap_server: The url:port of the Apache Kafka server
    :param topic: The topic of which to print the records
    :param verbosity: The level of verbosity
    """
    while True:
        try:
            for record in consumer:
                print_record(record, verbosity)
            sleep(1)
        except KeyboardInterrupt:
            print('Interrupted')
            consumer.close()
            break


def get_arguments():
    """
    Wrapper function for parsing command line arguments
    :return: A dict containing provided command line arguments
    """
    argument_parser = ArgumentParser(description=__doc__, formatter_class=RawTextHelpFormatter)

    argument_parser.add_argument('--config', '-c', help='Path or name of the config file to use', dest='config')

    argument_parser.add_argument('--kafka-url', '-u', help='Url to the Apache Kafka server as url:port.\n'
                                 'The default port is 9092.', dest='kafka_url')

    argument_parser.add_argument('--topic', '-t', help='Topic to subscribe to.', dest='topic')

    argument_parser.add_argument('--list', '-l', help='List available topics.', dest='list_topics', action='store_true')

    argument_parser.add_argument('--verbose', '-v', help='Print records with their topic', dest='verbosity',
                                 action='store_const', const=1)

    argument_parser.add_argument('--very-verbose', '-vv', help='Print records with their topic, partition and offset.',
                                 dest='verbosity', action='store_const', const=2)

    argument_parser.add_argument('--init-config', help='Generate an empty config file in ~/.config/rtr/',
                                 dest='init_config', action='store_true')

    return argument_parser.parse_args()


def get_config_file(config_file_name):
    """
    Gets a config file located at ~/.config/rtr/config_file_name
    :param config_file_name: Name of the config file in the folder ~/.config/rtr/
    :return: The config file if none exists
    """
    config_path = os.path.expanduser('~/.config/rtr/%s' % config_file_name)
    if os.path.isfile(config_path):
        config = configparser.ConfigParser()
        config.read(config_path)
        return config
    else:
        return None


def combine_args(arguments, config_file_name):
    """
    Combines the commandline arguments with those in the configuration file
    with the ones provided in the commandline taking precedence
    :param arguments:
    :param config_file_name:
    :return:
    """
    config_file = get_config_file(config_file_name)

    if arguments.kafka_url is None and config_file.has_option('configuration', 'bootstrap_server'):
        arguments.kafka_url = config_file['configuration']['bootstrap_server']

    if arguments.topic is None and arguments.list_topics is False and config_file.has_option('configuration', 'topic'):
        arguments.topic = config_file['configuration']['topic']

    return arguments


def topic_exists(bootstrap_url, topic):
    """
    Check if a topic exists on a kafka server
    :param bootstrap_url: The url leading to the kafka server
    :param topic: The topic of which to check if it exists
    :return: True/False, True if the topic exists, False if it doesn't
    """
    topics = KafkaConsumer(group_id='RemoteListener', bootstrap_servers=[bootstrap_url]).topics()
    return topic in topics


def required_args_present(arguments):
    """
    Check if all arguments that are needed for the program to run successfully are present
    :param arguments: The arguments that have been provided by the config file, user input or both combined
    :return: True/False for if all required arguments are present and a message if not all required args are present
    """
    if arguments.kafka_url is not None \
            and arguments.topic is None \
            and arguments.list_topics is False:
        return False, "--topic/-t or --list/-l must be set or the configuration file must have a topic option"
    elif arguments.list_topics is False \
            and arguments.topic is not None \
            and not topic_exists(arguments.kafka_url, arguments.topic):
        return False, "Topic %s does not exist at %s" % (arguments.topic, arguments.kafka_url)

    elif arguments.kafka_url is None:
        return False, "--kafka-url/-u must be set or the configuration file must have a bootstrap_server option"

    else:
        return True, ""


def init_config_file():
    """
    Generate an empty default configuration file `empty_config` in ~/.config/rtr/
    :return: nothing
    """
    config_file_path = os.path.expanduser("~/.config/rtr/empty_config")
    config_file = open(config_file_path, "+w")
    config_file.write("[configuration]\n")
    config_file.write("bootstrap_server = \n")
    config_file.write("topic = ")
    config_file.close()


def main():
    args = get_arguments()

    if args.init_config is True:
        init_config_file()
        return 0

    if args.config is not None:
        args = combine_args(args, args.config)

    args_ok, args_msg = required_args_present(args)
    if args_ok:
        if args.list_topics:
            consumer = KafkaConsumer(group_id='RemoteListener', bootstrap_servers=[args.kafka_url])
            list_topics(consumer)

        else:
            consumer = KafkaConsumer(args.topic, group_id='RemoteListener', bootstrap_servers=[args.kafka_url])
            print_records(args.topic, args.verbosity, consumer)

    else:
        print(args_msg)


if __name__ == '__main__':
    main()
