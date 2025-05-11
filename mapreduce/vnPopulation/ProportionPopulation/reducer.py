#!/usr/bin/env python3
import sys
import csv

reader = csv.reader(sys.stdin)
data = []
tong_dan_so = 0

for row in reader:
    if len(row) != 5:
        continue
    try:
        population = int(row[1].replace(",", ""))
        tong_dan_so += population
        data.append((row, population))
    except ValueError:
        continue

for row, population in data:
    ty_le = population / tong_dan_so if tong_dan_so > 0 else 0
    print(f"{row[0]},{row[1]},{row[2]},{row[3]},{row[4]},{ty_le:.6f}")
