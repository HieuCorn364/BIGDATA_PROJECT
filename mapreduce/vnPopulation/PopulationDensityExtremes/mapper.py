#!/usr/bin/env python3
import sys
import csv

# Bỏ qua dòng tiêu đề
next(sys.stdin)

reader = csv.reader(sys.stdin)
for row in reader:
    if len(row) >= 4:
        province = row[0].strip()
        try:
            density = float(row[3].replace(",", "").strip())
            print(f"{province}\t{density}")
        except:
            continue  # Bỏ qua nếu mật độ không hợp lệ
