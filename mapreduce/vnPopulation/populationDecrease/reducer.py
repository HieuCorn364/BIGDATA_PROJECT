#!/usr/bin/env python3
import sys

data = []

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    fileds = line.split(',')
    try:
        data.append(fileds)
    except ValueError:
        continue

for i in range(len(data)-1):
    for j in range(i+1, len(data)):
        if int(data[i][1]) < int(data[j][1]): 
            temp = data[i]
            data[i] = data[j]
            data[j] = temp
for line in data:
    print(f"{line[0]},{line[1]},{line[2]},{line[3]},{line[4]}")