#!/usr/bin/env python3
# genres-reducer.py: sorted pairs → (genre, total)
import sys

current, total = None, 0
for line in sys.stdin:
    genre, count = line.rstrip("\n").split("\t")
    if genre != current:                  # a new key starts
        if current is not None:
            print(f"{current}\t{total}")
        current, total = genre, 0
    total += int(count)
if current is not None:
    print(f"{current}\t{total}")
