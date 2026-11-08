#!/usr/bin/env python3
# movie-rating-reducer.py → (movieId, avg, n)
import sys

def emit(movie, total, n):
    print(f"{movie}\t{total / n:.2f}\t{n}")

current, total, n = None, 0.0, 0
for line in sys.stdin:
    movie, rating = line.rstrip("\n").split("\t")
    if movie != current:
        if current is not None: emit(current, total, n)
        current, total, n = movie, 0.0, 0
    total += float(rating)
    n += 1
if current is not None: emit(current, total, n)
