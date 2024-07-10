import std/[strutils, streams, os, osproc,
            strformat, times, paths, math]
import print
import xxhash
import db_connector/db_sqlite


let seed = 153.uint64

let scribeDir = (if paramCount() > 0 : paramStr(1) else: "./")

let dbPath = fmt"{scribeDir}/.scribe2.db"
let db = open(dbPath, "", "", "")

proc initDb(): void =
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
  let ignorePaths = @[".scribe", ".vim", ".git", "."]
  for pathPart in path.split('/'):

    for p in ignorePaths:
      if p.len == 1: continue

      if pathPart.endsWith(p):
        return true

      if pathPart.startsWith(p):
        return true

  return false


const chunkSize = 2 ^ 17
var buffer = newSeqUninitialized[uint8](chunkSize)
# var buffer = newString(chunkSize)

proc streamingHash(path: string): string =
  var fs = newFileStream(path, fmRead)
  defer: fs.close()

  var state = newXxh3_64(seed)
  var size: int

  while not fs.atEnd:
    # fs.readStr(chunkSize, buffer)
    let size = fs.readData(addr(buffer[0]), chunkSize - 1)
    buffer[size] = 0

    state.update(cast[string](buffer))

  return state.digest().toHex.toLowerAscii



var insertCount = 0
let COMMIT_RATE = 150

var dateStr = now().format("yyyy-MM-dd' 'HH:mm:ss")
var count = 0
var txStarted = false
proc hashFiles() =
  for p in walkDirRec(scribeDir & "/"):

    if p.shouldIgnore:
      continue

    let path = p.normalizedPath
    insertCount += 1
    # if insertCount mod 1000 == 0:
    #   print path

    let filesize = path.getFileSize

    let fetchedSize = db.getValue(sql"""SELECT filesize FROM files
                                      WHERE name = ? ORDER BY date
                                      DESC LIMIT 1""", path)


    var hash = ""
    if fetchedSize.len == 0 or fetchedSize.parseInt != filesize:
      hash = streamingHash(path)

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

    if count mod 300 == 0:
      db.exec(sql"COMMIT")
      print "commit"
      txStarted = false
      count = 0

    # print path
    # print hash
  if txStarted:
    db.exec(sql"COMMIT")

proc checkFiles() =
  let lastTime = db.getValue(sql"""SELECT date FROM files
                                    ORDER BY date
                                    DESC LIMIT 1""")

  for row in db.rows(sql"SELECT files.name, hashes.hash FROM files JOIN hashes ON files.name = hashes.name WHERE files.date = ?", lastTime):
    if not row[0].fileExists:
      print "not found"
      print row[0], row[1]
    else:
      let newHash = streamingHash(row[0])
      print newHash, row[1]
      if newHash == row[1]:
        print "match"
      else:
        print row[0]
        print "no match ERROROROROROR"
        discard readline stdin






initDb()
# hashFiles()
# checkFiles()
db.close()
