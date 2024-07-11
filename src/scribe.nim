import std/[strutils, streams, os, osproc,
            strformat, times, paths, math]
import print
import xxhash
import argparse

import db_connector/db_sqlite


# seed used for xxhash - need this seed
# for hashes to be reproducable with another client
const seed = 153.uint64

# this limits the number of saves that we store
# a save is a list of all the files on the system.
# we want to do saves often so we have a list of what's
# currently on the system but it takes up a lot of space
# so we have a rolling buffer thing
const numSaves = 3

var db: DbConn

proc initDb(path: string): void =

  let dbPath = fmt"{path}/.scribe.db"
  db = open(dbPath, "", "", "")

  # schema
  db.exec(sql"""CREATE TABLE IF NOT EXISTS hashes
                      (name TEXT, filesize INTEGER, hash TEXT, date TEXT)""")

  db.exec(sql"""CREATE TABLE IF NOT EXISTS files
                      (name TEXT, filesize INTEGER, date TEXT)""")

  db.exec(sql"""CREATE TABLE IF NOT EXISTS dates
                      (date TEXT)""")

  db.exec(sql"""CREATE INDEX IF NOT EXISTS hashes_name_idx
                      ON hashes(name)""")

  db.exec(sql"""CREATE INDEX IF NOT EXISTS files_name_idx
                      ON files(name)""")

  db.exec(sql"""CREATE INDEX IF NOT EXISTS files_date_idx
                      ON files(date)""")

  db.exec(sql"""CREATE INDEX IF NOT EXISTS files_name_date_idx
                      ON files(name, date)""")



proc shouldIgnore(path: string): bool =
  # files that we ignore, add more stuff here if needed
  # we are not hashing hidden folders for now, I guess that's okay
  let ignorePaths = @[".scribe", ".vim", ".git", "."]
  for pathPart in path.split('/'):

    for p in ignorePaths:
      if p.len == 1: continue

      if pathPart.endsWith(p):
        return true

      if pathPart.startsWith(p):
        return true

  return false


# so this is what chunk size we use to do the streaming
# segmented hash thing. This number seems to be tweakable
# and there seems to be a sweet spot for speed, too small
# or too large affects the speed. Idk if this is the right
# approach or what

# Also I wanted to optimize it by not creating a newString
# but there was a bug, the hash works but it varies each time
# you call it, so something is not right. I think the speed increase
# is extremely minimal so it's not worth it. Was just doing it for
# fun really

const chunkSize = 2 ^ 17
# var buffer = newSeqUninitialized[uint8](chunkSize)
var buffer = newString(chunkSize)

proc streamingHash(path: string): string =
  var fs = newFileStream(path, fmRead)
  defer: fs.close()

  var state = newXxh3_64(seed)
  var size: int

  while not fs.atEnd:
    fs.readStr(chunkSize, buffer)
    # let size = fs.readData(addr(buffer[0]), chunkSize - 1)
    # buffer[size] = 0

    state.update(cast[string](buffer))

  return state.digest().toHex.toLowerAscii



var insertCount = 0

# How often we should commit to the database
# We sort of have to handroll the whole transaction thing here
# instead of how python does it. Anyways this does affect the speed
# a lot so this number can be tweaked, actually the larger the better
# probably
let COMMIT_RATE = 1000

# this is the date format for sqlite too although we aren't really
# using the date format features in it yet
var dateStr = now().format("yyyy-MM-dd' 'HH:mm:ss")

var count = 0
var txStarted = false

proc hashFiles(rootPath: string) =
  for p in walkDirRec(rootPath & "/"):
    if p.shouldIgnore:
      continue

    let path = p.normalizedPath

    let filesize = path.getFileSize
    let fetchedSize = db.getValue(sql"""SELECT filesize FROM files
                                      WHERE name = ? ORDER BY date
                                      DESC LIMIT 1""", path)

    var hash = ""
    # check filesizes first, if there is no previously stored filesize
    # or the filesizes don't match then hash
    if fetchedSize.len == 0 or fetchedSize.parseInt != filesize:
      hash = streamingHash(path)
      print path, hash

    # we have to handroll the transaction stuff
    # in the future we should be able to make some sort of nim macro to
    # handle this
    if not txStarted:
      db.exec(sql"BEGIN")
      txStarted = true

    if hash == "":
      db.exec(sql"INSERT INTO files(name, filesize, date) VALUES(?,?,?)",
                 path,filesize, dateStr)
    else:
      db.exec(sql"INSERT INTO files(name, filesize, date) VALUES(?,?,?)",
                 path,filesize, dateStr)

      db.exec(sql"INSERT INTO hashes(name, filesize, date, hash) VALUES(?, ?,?,?)",
                 path,filesize, dateStr, hash)
    count += 1

    if count mod COMMIT_RATE == 0:
      print path
      db.exec(sql"COMMIT")
      txStarted = false
      count = 0

  # more of the sqlite transaction handroll
  if txStarted:
    db.exec(sql"COMMIT")


  # this is the rolling buffer of how many saves we allow
  db.exec(sql"""DELETE FROM files WHERE date IN
          (SELECT DISTINCT date FROM files
           ORDER BY date DESC limit -1 offset ?)""",
              numSaves)

  # this removes entries from hashes that aren't in any
  # of the saves
  db.exec(sql"""DELETE FROM hashes WHERE name IN
          (SELECT hashes.name from HASHES
          LEFT JOIN files ON hashes.name = files.name
          WHERE files.name IS NULL)""")

proc checkFiles(checkHash = false) =
  var errors = false
  let lastTime = db.getValue(sql"""SELECT date FROM files
                                    ORDER BY date
                                    DESC LIMIT 1""")


  # this first part checks if there are missing files or not only
  for row in db.rows(sql"SELECT name FROM files WHERE date = ?", lastTime):
    if not row[0].fileExists:
      echo fmt"not found: {row[0].normalizedPath}"
      errors = true
    else:

      # this does a hashcheck on each found file
      if checkHash:
        let firstHash = db.getValue(sql"""SELECT hash FROM hashes
                                    WHERE name = ?
                                          ORDER BY date
                                          ASC LIMIT 1""", row[0])

        let newHash = streamingHash(row[0])

        if newHash != firstHash:
          echo fmt"hash miss: {row[0]}, stored: {firstHash}, calc: {newHash}"
          errors = true
  if not errors:
    echo "all checks good"


var p = newParser:
  flag("-hs", "--hash", help="Run a full hash calculation")
  flag("-cf", "--check-files",  help="Check if files are missing from the last save")
  flag("-ch", "--check-hashes", help="Check all the hashes of the last save to the specified destination")
  arg("path")
  arg("others", nargs = -1)


try:
  var opts = p.parse(commandLineParams())
  initDb(opts.path)
  if opts.hash: hashFiles(opts.path)
  elif opts.check_files: checkFiles(opts.check_hashes)
  elif opts.check_hashes: checkFiles(opts.check_hashes)
  db.close()

except ShortCircuit as err:
  if err.flag == "argparse_help":
    echo err.help
    quit(1)
