import sys, os, glob
from functools import partial
import xxhash
# import rapidjson as json
from datetime import datetime
import sqlite3
# import concurrent.futures

db = sqlite3.connect(".scribe.db")
cur = db.cursor()

def init_db():
    cur.execute("""CREATE TABLE IF NOT EXISTS files
                        (name TEXT, filesize INTEGER, hash TEXT, date TEXT)""")

    cur.execute("""CREATE TABLE IF NOT EXISTS dates
                        (date TEXT)""")

    cur.execute("""CREATE INDEX IF NOT EXISTS files_name_idx
                        ON files(name)""")

    cur.execute("""CREATE INDEX IF NOT EXISTS files_date_idx
                        ON files(date)""")

    cur.execute("""CREATE INDEX IF NOT EXISTS files_name_date_idx
                        ON files(name, date)""")

init_db()

SEED=153

scribe_dir = ".scribe"
path = sys.argv[1]
num_threads = 1

# pool = concurrent.futures.ThreadPoolExecutor(max_workers=num_threads)

def get_hash(file):
    CHUNK_SIZE = 2 ** 32  # Or whatever you have memory to handle
    with open(file, 'rb') as input_file:
        x = xxhash.xxh3_64(seed=SEED)
        for chunk in iter(partial(input_file.read, CHUNK_SIZE), b''):
            x.update(chunk)
        return x.hexdigest()

def write_hash(f, datetime, force_hash=False):
    filesize = os.stat(f).st_size

    cur.execute('SELECT name, filesize FROM files WHERE name = ? ORDER BY date DESC LIMIT 1', (f, ))
    res = cur.fetchone()

    h = ""
    if not res:
        h = get_hash(f)
    elif res[1] != filesize:
        h = get_hash(f)

    if h == "":
        d = (f,filesize, datetime,)
        cur.execute("INSERT INTO files(name, filesize, date) VALUES(?,?,?)", (f,filesize, datetime))
        print(f"{d=}")
    else:
        d = (f,filesize, datetime,h,)
        cur.execute("INSERT INTO files(name, filesize, date, hash) VALUES(?,?,?,?)", (f,filesize, datetime, h))
        print(f"{d=}")



def get_datetime():
    today = datetime.now()
    date_time_str = today.strftime("%Y-%m-%d %H:%M:%S")
    return date_time_str

def write_hashes():
    datetime = get_datetime()
    for subdir, dirs, files in os.walk(path):
        for file in files:
            f = subdir + os.sep + file
            if os.path.isdir(f): continue
            if f.endswith(".scribe"): continue

            print(f)
            write_hash(f, datetime)


write_hashes()
db.commit()
db.close()
