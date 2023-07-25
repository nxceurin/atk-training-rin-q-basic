import sqlite3
from datetime import datetime as dt
from typing import Union, Tuple, List


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

    def delete_entry(self):
        _, file_id = self.get_next_file_from_queue()
        self.conn.execute("BEGIN IMMEDIATE")
        self.conn.execute('DELETE FROM queue WHERE id = ?', (file_id,))
        self.conn.commit()

    def get_jobs(self) -> List:
        cursor = self.conn.execute('SELECT id, filename FROM queue')
        row = cursor.fetchall()
        return row

    def update_status(self):
        self.conn.commit("BEGIN IMMEDIATE")
        processing_files = self.conn.execute("SELECT ROWID, * FROM queue WHERE state='processing'").fetchall()
        for row in processing_files:
            job_time = row["proc_time"]  # Assuming you have a state_time column in your table
            job_time = dt.strptime(job_time, "%Y-%m-%d %H:%M:%S.%f")
            if (dt.now() - job_time).total_seconds() >= 60:
                self.conn.execute("UPDATE queue SET state='unprocessed' WHERE id=?", (row["id"],))
                print(f'Set {row["filename"]} to unprocessed')
                self.conn.commit()
