#!/usr/bin/python3
"""mapper.py"""

import sys

# CSV file header - we need to skip it
header = 'userId,movieId,rating,timestamp'

# Iterate over every line from the CSV file
for line in sys.stdin:
    # Process lines which are not the header (header skipping)
    if header not in line:
        # Remove white spaces and split by , -> list of fields
        fields = line.strip().split(',')

        # Get the movie identifier (second position)
        movieId = fields[1]
        # Get the movie rating (third position)
        rating = fields[2]

        # Print key/value out
        print(f"{movieId}\t{rating}")