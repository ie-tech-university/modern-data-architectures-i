#!/usr/bin/python3
"""reducer2.py"""

import sys

# Between "map and reduce stages" there is an implicit "sort stage" meaning:
#
#   - we don't have to provide any function to sort keys
#   - the reducer will receive keys with the same value sequentially
#   - the reducer logic will operate on the value part of the key/value pairs

# The following variables will help us process all values for the same key
inprocess_movie = None
inprocess_total = 0.0
inprocess_count = 0
movie_read = None

# Iterate over every key/value pair
for line in sys.stdin:
    # Extract the key/value pair into a couple of variables
    movie_read, rating_read = line.strip().split('\t', 1)

    # Convert rating from text to number and skip it if it's not possible
    try:
        rating_read = float(rating_read)
    except ValueError:
        continue

    # inprocess_movie initialization in the first iteration
    inprocess_movie = movie_read if inprocess_movie is None \
                                 else inprocess_movie

    # Are we still processing the same movie?
    if inprocess_movie == movie_read:
        # YES -> update in process total (sum all ratings up) and 
        #        in process counter
        inprocess_total += rating_read
        inprocess_count += 1
    else:
        if movie_read is not None:
            # NO (and movie_read is not None) -> time to print the result out
            print(f"{inprocess_movie}\t{inprocess_total/inprocess_count}\t{inprocess_count}")
            # Initialize variables to process the next movie
            inprocess_movie = movie_read
            inprocess_total = rating_read
            inprocess_count = 1

# Print out the last key processed
if inprocess_movie == movie_read:
    print(f"{inprocess_movie}\t{inprocess_total/inprocess_count}\t{inprocess_count}")