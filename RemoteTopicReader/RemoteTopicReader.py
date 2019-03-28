from argparse import ArgumentParser
from kafka import KafkaConsumer
from time import sleep


def check_args(args_to_check):
    if args_to_check.topic is None and args_to_check.list_topics is False:
        return False, "--topic/-t is required if --kafka_url/-u is set"
    else:
        return True, ""


def list_topics(bootstrap_server):
    consumer = KafkaConsumer(group_id='RemoteListener', bootstrap_servers=[bootstrap_server])

    topics = sorted(consumer.topics())
    print('Topics:')

    for topic in topics:
        print('  - %s' % topic)

    consumer.close()


def print_record(record_to_print, verbosity):
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


def print_records(bootstrap_server, topic, verbosity):
    consumer = KafkaConsumer(topic, group_id='RemoteListener', bootstrap_servers=[bootstrap_server])
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
    argument_parser = ArgumentParser()
    argument_parser.add_argument('--kafka-url', '-u', help='Url to the Apache Kafka server', dest='kafka_url',
                                 required=True)

    argument_parser.add_argument('--topic', '-t', help='Topic to listen to', dest='topic')

    argument_parser.add_argument('--list', '-l', help='List available topics', dest='list_topics', action='store_true')

    argument_parser.add_argument('--verbose', '-v', help='Print records verbosely', dest='verbosity',
                                 action='store_const', const=1)

    argument_parser.add_argument('--very-verbose', '-vv', help='Print records very verbosely', dest='verbosity',
                                 action='store_const', const=2)

    return argument_parser.parse_args()


def main():
    args = get_arguments()

    args_ok, check_args_msg = check_args(args)

    if args_ok:
        if args.list_topics:
            list_topics(args.kafka_url)

        else:
            print_records(args.kafka_url, args.topic, args.verbosity)

    else:
        print(check_args_msg)


if __name__ == '__main__':
    main()
