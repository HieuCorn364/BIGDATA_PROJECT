#!/usr/bin/env python3
import sys

vungs = ['Đồng bằng sông Hồng', 'Đông Bắc Bộ', 'Tây Bắc Bộ', 
        'Bắc Trung Bộ', 'Nam Trung Bộ', 'Tây Nguyên', 'Đông Nam Bộ', 'Đồng bằng sông Cửu Long']
lines = [line.strip().split(',') for line in sys.stdin]

print ("vung    dan_so  dien_tich   mat_do_dan_so")
for vung in vungs:
    total_dan_so = 0
    total_dien_tich = 0
    for line in lines:
        if vung == line[4]:
            total_dan_so += int(line[1])
            total_dien_tich += float(line[2])
    print(f"{vung},{total_dan_so},{total_dien_tich},{total_dan_so/total_dien_tich}")
    
        