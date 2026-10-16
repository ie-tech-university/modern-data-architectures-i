#!/usr/bin/python3
"""mapper.py"""

import sys

# CSV file header - we need to skip it
header = 'movieId,title,genres'

# Iterate over every line from the CSV file
for line in sys.stdin:
    # Process lines which are not the header (header skipping)
    if header not in line:
        # Remove white spaces find the last comma (identify last field)
        index = line.strip().rfind(',')

        # Extract last field and split by | -> list of genres
        genres = line[index+1:].split('|')

        # Iterate over genres and print out key/value pairs like this:
        #
        #   genre   1
        #
        # where the space between the genre and number 1 is defined by \t (tabulator)
        for genre in genres:
            # Skip empty text
            if genre.strip() != '':
                # Print key/value out
                print(f"{genre.strip()}\t1")