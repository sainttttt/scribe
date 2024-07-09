import std/[strutils, streams, os, osproc,
            strformat, json, times, paths]
import print
var path = "/Volumes/HDD1A/SSD3/ML Res/txt_sentoken/pos/cv988_18740.txt"

print path
var fs = newFileStream(path, fmRead)
print fs
