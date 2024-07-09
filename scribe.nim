import std/[strutils, streams, os, osproc,
            strformat, json, times, paths, math]
import print
import xxhash


let seed = 153.uint64
let baseDir = "./"


let scribeDir = (if paramCount() > 0 : paramStr(1) else: "./")

let ignorePaths = @[".scribe", ".vim", ".git", "."]

proc shouldIgnore(path: string, ignorePaths: seq[string]): bool =
  for pathPart in path.split('/'):

    for p in ignorePaths:
      if pathPart.endsWith(p):
        return true

      if pathPart.startsWith(p):
        return true

  return false


proc streamingHash(path: string): string =
  var fs = newFileStream(path, fmRead)
  defer: fs.close()
  var state = newXxh3_64(seed)
  const chunkSize = 2 ^ 16
  var buffer: array[chunkSize, char]
  var size: int

  try:
    while not fs.atEnd:
      size = fs.readData(addr(buffer), chunkSize)
      state.update(cast[string](buffer[0..size-1]))
  except:
    print getCurrentExceptionMsg()

  return $(%*{path: state.digest().toHex.toLowerAscii})

proc hashFiles() =

  let mainScribeFolder = scribeDir & "/.scribe"
  if not dirExists(mainScribeFolder):
    print "doesn't exist"
    discard execCmd(fmt"mkdir -p '{mainScribeFolder}'")


  var dateStr = now().format("yyyy-MM-dd'_'HH-mm-ss")

  let mainScribeFile = open(fmt"{mainScribeFolder}/{dateStr}.scribe",
                     fmWrite)

  defer: mainScribeFile.close()

  var subRootScribeFile: File
  defer: subRootScribeFile.close()

  var prevSubroot: string
  for path in walkDirRec(scribeDir & "/"):

    var logPaths = @[scribeDir]
    if path.shouldIgnore(ignorePaths):
      continue

    let relpath =  relativePath(path, scribeDir)
    let relPathSplit = relpath.splitPath

    var subroot = ""
    if not relPathSplit[0].isEmptyOrWhiteSpace:
      subroot = relPathSplit[0].split("/")[0]


    # get hash

    let hash = streamingHash(path)
    print hash

    mainScribeFile.writeLine(hash)

    # write to main file

    if true:
      # wrap this in option to write to subroot
      if subroot != "":
        if subroot != prevSubroot:
          prevSubroot = subroot
          if subRootScribeFile != nil:
            subRootScribeFile.close()
          let subRootScribeDir = fmt"{scribeDir}/{subRoot}/.scribe/"
          if not dirExists(subRootScribeDir):
            discard execCmd(fmt"mkdir -p '{subRootScribeDir}'")
          subRootScribeFile = open(fmt"{subRootScribeDir}/{dateStr}.scribe", fmWrite)
        # write to subroot file
        subRootScribeFile.writeLine(hash)


    # discard readline(stdin)

hashFiles()
