#!/usr/bin/python3
"""reducer.py"""

import sys

# Between "map and reduce stages" there is an implicit "sort stage" meaning:
#
#   - we don't have to provide any function to sort keys
#   - the reducer will receive keys with the same value sequentially
#   - the reducer logic will operate on the value part of the key/value pairs

# The following variables will help us process all values for the same key
inprocess_genre = None
inprocess_count = 0
genre_read = None

# Iterate over every key/value pair
for line in sys.stdin:
    # Extract the key/value pair into a couple of variables
    genre_read, count_read = line.split('\t', 1)

    # Convert count from text to number and skip if it's not possible
    try:
        count_read = int(count_read)
    except ValueError:
        continue

    # inprocess_movie initialization in the first iteration
    inprocess_genre = genre_read if inprocess_genre is None \
                                 else inprocess_genre

    # Are we still processing the same genre?
    if inprocess_genre == genre_read:
        # YES -> add read_count to the in process counter
        inprocess_count += count_read
    else:
        if genre_read is not None:
            # NO (and read_genre is not None) -> time to print the result out
            print(f"{inprocess_genre}\t{inprocess_count}")
            # Initialize variables to process the next genre
            inprocess_genre = genre_read
            inprocess_count = count_read

# Print out the last key processed
if inprocess_genre == genre_read:
    print(f"{inprocess_genre}\t{inprocess_count}")