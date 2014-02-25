"""
index the data from file
"""
from __future__ import unicode_literals

import argparse
import csv
import redis

REDIS_PORT = 6379


def _generate_prefixes(min_prefix_size,
                       max_prefix_size,
                       description):
    prefix_set = set()
    words = description.split(' ')
    for word in words:
        word = word.lower()
        word_len = len(word)
        if word_len >= min_prefix_size:
            for index in xrange(min_prefix_size, word_len):
                if index < max_prefix_size:
                    prefix_set.add(word[:index])

    return prefix_set


def _add_to_index(redis_server,
                  prefix,
                  description):
    redis_server.rpush(prefix, description)


def run(input_file,
        redis_server,
        max_samples):
    redis_server.flushall()
    min_prefix_size = 1
    max_prefix_size = 10
    with open(input_file, 'rb') as f:
        reader = csv.DictReader(f)
        for row_number, row in enumerate(reader):
            if max_samples and (row_number >= max_samples):
                break
            definition = row['DRG Definition']
            description = definition.split('- ')[1].lower()
            prefixes = _generate_prefixes(min_prefix_size=min_prefix_size,
                                          max_prefix_size=max_prefix_size,
                                          description=description)
            print description
            for prefix in prefixes:
                _add_to_index(redis_server,
                              prefix,
                              description)


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--input', action='store',
                            dest='input',)
    arg_parser.add_argument('--max_samples', action='store',
                            dest='max_samples', type=int)
    args = arg_parser.parse_args()
    redis_server = redis.Redis("localhost")
    run(input_file=args.input,
        redis_server=redis_server,
        max_samples=args.max_samples)


if __name__ == '__main__':
    main()
