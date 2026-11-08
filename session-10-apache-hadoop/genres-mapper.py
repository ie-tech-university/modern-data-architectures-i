#!/usr/bin/env python3
# genres-mapper.py: line → (genre, 1)
import sys

for line in sys.stdin:
    line = line.strip()
    if not line or line.startswith("movieId"):
        continue                          # skip header
    genres = line.rsplit(",", 1)[-1]      # last column
    for genre in genres.split("|"):
        print(f"{genre}\t1")
