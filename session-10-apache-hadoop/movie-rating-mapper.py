#!/usr/bin/env python3
# movie-rating-mapper.py: line → (movieId, rating)
import sys

for line in sys.stdin:
    line = line.strip()
    if not line or line.startswith("userId"):
        continue
    user_id, movie_id, rating, ts = line.split(",")
    print(f"{movie_id}\t{rating}")
