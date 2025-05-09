#!/usr/bin/env python3
import sys
import csv

next(sys.stdin)

reader = csv.reader(sys.stdin)
for row in reader:
    if len(row) != 5:
        continue
    print(f"{row[0]},{row[1]},{row[2]},{row[3]},{row[4]}")
        
        
