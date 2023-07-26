import sqlite3
from datetime import datetime as dt
from typing import Union, Tuple


class PersistentQSQLite:
    def __init__(self, path):
        """
        Establishes a connection to the database and immediately creates a table
        """
        self.db_name = "basic"
        self.conn = sqlite3.connect(f"{path}/{self.db_name}.db")
        self.conn.row_factory = sqlite3.Row
        self.create_queue_table()

    def create_queue_table(self) -> None:
        """
        Creates a table 'queue' in basic.db if non-existent with schema id INTEGER (primary key), filename TEXT,
        state TEXT, proc_time TEXT
        """
        self.conn.execute('''CREATE TABLE IF NOT EXISTS queue (id INTEGER PRIMARY KEY, filename TEXT NOT NULL, 
        state TEXT NOT NULL, proc_time TEXT)''')
        self.conn.commit()

    def add_file_to_queue(self, filename: str) -> None:
        """
        Inserts a row into the database, specifically containing columns filename, state and proc_time
        """
        self.conn.execute("BEGIN IMMEDIATE")
        self.conn.execute('INSERT INTO queue (filename, state, proc_time) VALUES (?, ?, ?)',
                          (filename, "unprocessed", str(dt.now())))
        self.conn.commit()

    def get_next_file_from_queue(self) -> Union[None, Tuple[str, int]]:
        """
        get the top-most file from the queue with the status unprocessed and sets it to "processing"
        """
        self.conn.execute("BEGIN IMMEDIATE")
        cursor = self.conn.execute('SELECT id, filename, state FROM queue WHERE state = "unprocessed" LIMIT 1')
        row = cursor.fetchone()
        if row:
            self.conn.execute("UPDATE queue SET state='processing', proc_time=? WHERE ROWID=?",
                              (str(dt.now()), row["id"],))
            file_id, filename, _ = row
            self.conn.commit()
            return filename, file_id
        self.conn.commit()
        return None

    def set_state(self, job_id: int, state: str):
        self.conn.execute("BEGIN IMMEDIATE")
        if state == "processed":
            self.conn.execute("UPDATE queue SET state=? WHERE id= ? AND state='processing'", (state, job_id,))
        else:
            self.conn.execute("UPDATE queue SET state=? WHERE id= ?", (state, job_id,))
        self.conn.commit()

    def get_job_details(self):
        cursor = self.conn.execute('SELECT id, filename, state FROM queue WHERE state = "unprocessed" LIMIT 1')
        row = cursor.fetchone()
        if row:
            file_id, filename, _ = row
            return filename, file_id
        return None

    def get_state(self, state: str):
        return self.conn.execute('SELECT id, filename, state FROM queue WHERE state = ?', (state,)).fetchall()
