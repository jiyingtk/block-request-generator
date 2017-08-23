#!/usr/bin/env python
# coding=utf-8
import re
import sys
method_strs = ['oi-raid-', 'parity-declustering-', 'raid-5-', 'rs69-', 's2-raid-']
disk_nums = ['21', '45']
lat = [{}, {}]

def process(fn):
    f = open(fn, 'r')
    inArea = 0
    which = [0, 0]
    for l in lat:
        for method_str in method_strs:
            l[method_str] = 0
    for line in f.readlines():
        for i in range(len(method_strs)):
            for j in range(len(disk_nums)):
                m = re.search(method_strs[i] + disk_nums[j], line)
                if m:
                  inArea = 0
                  which = [i, j]
        inArea += 1
        if inArea == 3:
            lat[which[1]][method_strs[which[0]]] += float(line)
    for l in lat:
        for method_str in method_strs:
            l[method_str] /= 3
    for i in range(len(lat)):
        for method_str in lat[i]:
            print(method_str + disk_nums[i] + ": " + str(lat[i][method_str]))

    f.close()


process(sys.argv[1])
