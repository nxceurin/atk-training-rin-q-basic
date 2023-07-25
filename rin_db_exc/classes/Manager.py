from datetime import datetime as dt

from rin_db_exc.classes.PIQ import PersistentQInterface
from rin_db_exc.classes.PersistentQSQLite import PersistentQSQLite


class Manager(PersistentQInterface):
    def __init__(self, cpath: str):
        super().__init__()
        self.config = cpath
        self.db = PersistentQSQLite(cpath)

    def __call__(self):
        processing_files = self.db.conn.execute("SELECT ROWID, * FROM queue WHERE state='processing'").fetchall()
        for row in processing_files:
            job_time = row["proc_time"]
            job_time = dt.strptime(job_time, "%Y-%m-%d %H:%M:%S.%f")

            if (dt.now() - job_time).total_seconds() >= 60:
                self.db.conn.execute("BEGIN IMMEDIATE")
                self.db.conn.execute("UPDATE queue SET state='unprocessed' WHERE id=?", (row["id"],))
                self.db.conn.commit()
                print(f'Set {row["filename"]} to unprocessed')
        self.db.conn.close()
