#!/usr/bin/env python3
import sys

max_density = -1
min_density = float('inf')
max_province = ""
min_province = ""

for line in sys.stdin:
    try:
        province, density = line.strip().split("\t")
        density = float(density)
        if density > max_density:
            max_density = density
            max_province = province
        if density < min_density:
            min_density = density
            min_province = province
    except:
        continue

print(f"MAX_DENSITY\t{max_province},{max_density}")
print(f"MIN_DENSITY\t{min_province},{min_density}")
