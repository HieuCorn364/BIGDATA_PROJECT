#!/usr/bin/env python3
import sys

vungs = ['Đồng bằng sông Hồng', 'Đông Bắc Bộ', 'Tây Bắc Bộ', 
         'Bắc Trung Bộ', 'Nam Trung Bộ', 'Tây Nguyên', 'Đông Nam Bộ', 'Đồng bằng sông Cửu Long']

lines = [line.strip().split(',') for line in sys.stdin]

for vung in vungs:
    #dan_so = sum(int(fields[1]) for fields in data if fields[4] == vung)
    for line in lines
        if line[4] == vung
            dan_so =+ (int(fields[1]))
    print(f"{vung},{dan_so}")
