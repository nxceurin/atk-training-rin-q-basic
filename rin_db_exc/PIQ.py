import logging
import os
import random
import sqlite3
import string
from abc import ABC, abstractmethod
from datetime import datetime as dt
from typing import List

from rin_db_exc.process_tasks import stair_case, coalesce_spaces, append_date

logger: logging.Logger = logging.getLogger(__name__)


class PersistentQSQLite:
    def __init__(self, path):
        self.db_name = "basic"
        self.conn = sqlite3.connect(f"{path}/{self.db_name}.db")
        self.conn.row_factory = sqlite3.Row
        self.create_queue_table()

    def create_queue_table(self):
        self.conn.execute('''CREATE TABLE IF NOT EXISTS queue (id INTEGER PRIMARY KEY, filename TEXT NOT NULL, 
        state TEXT)''')
        self.conn.commit()

    def add_file_to_queue(self, filename):
        self.conn.execute('INSERT INTO queue (filename) VALUES (?)', (filename, ))
        self.conn.commit()

    def get_next_file_from_queue(self):
        cursor = self.conn.execute('SELECT id, filename, state FROM queue WHERE state = "" LIMIT 1')
        row = cursor.fetchone()
        if row:
            file_id, filename = row
            return filename, file_id
        return None

    def delete_entry(self):
        _, file_id = self.get_next_file_from_queue()
        self.conn.execute('DELETE FROM queue WHERE id = ?', (file_id,))
        self.conn.commit()

    def get_jobs(self) -> List:
        cursor = self.conn.execute('SELECT id, filename FROM queue')
        row = cursor.fetchall()
        return row

    def close(self):
        self.conn.close()


class PersistentQInterface(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def __call__(self, *args, **kwargs):
        pass


class Producer(PersistentQInterface):
    def __init__(self, cpath: str):
        super().__init__()
        self.config = cpath
        self.db = PersistentQSQLite(cpath)

    def __call__(self, *args, **kwargs):
        fname = ''.join(random.choice(string.ascii_lowercase) for _ in range(10))
        self.db.add_file_to_queue(fname)

        file_path = os.path.join(self.config, fname)
        with open(file_path, 'w') as f:
            f.write('This is some sample content')
        return fname


class Consumer(PersistentQInterface):
    def __init__(self, name: str, cpath: str):
        super().__init__()
        self.name = name
        self.config = cpath
        self.db = PersistentQSQLite(cpath)

    def __call__(self):
        curr_job, _ = self.db.get_next_file_from_queue()
        if not curr_job:
            print("Queue empty. Exiting...")
            return

        file_path = os.path.join(self.config, curr_job)
        with open(file_path, 'r') as read_file:
            content = read_file.read()

        content = coalesce_spaces(content)
        content = stair_case(content)
        content = append_date(content)

        with open(file_path+'.processed', 'w') as write_file:
            write_file.write(content)
        print(f"[{self.name}] - {dt.now()} - [{curr_job}]")

    def delete_entry(self):
        self.db.delete_entry()

    def get_job_name(self):
        if self.db.get_next_file_from_queue():
            return self.db.get_next_file_from_queue()


class Cleaner(PersistentQInterface):
    def __init__(self, cpath: str):
        super().__init__()
        self.config = cpath

    def __call__(self):
        curr_dir = self.config
        files = os.listdir(curr_dir)

        for file in files:
            if file.endswith('.failed'):
                file_path = os.path.join(curr_dir, file)
                os.remove(file_path)
                print(f"Deleted: {file}")
