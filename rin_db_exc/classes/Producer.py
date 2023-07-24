import os
import random
import sqlite3
import string
from time import sleep

from rin_db_exc.classes.PIQ import PersistentQInterface
from rin_db_exc.classes.PersistentQSQLite import PersistentQSQLite


class Producer(PersistentQInterface):
    def __init__(self, cpath: str):
        super().__init__()
        self.config = cpath
        self.db = PersistentQSQLite(cpath)

    def __call__(self, *args, **kwargs):
        fname = ''.join(random.choice(string.ascii_lowercase) for _ in range(10))
        for _ in range(6):
            try:
                self.db.add_file_to_queue(fname)
                break
            except sqlite3.OperationalError:
                sleep(10)
                continue
        else:
            print("Database locked for more than 60s. SKipping...")
            return ""

        file_path = os.path.join(self.config, fname)
        with open(file_path, 'w') as f:
            f.write('This is some sample content')
        return fname
