from rin_db_exc.classes.PIQ import PersistentQInterface
from rin_db_exc.classes.PersistentQSQLite import PersistentQSQLite


class Cleaner(PersistentQInterface):
    def __init__(self, cpath: str):
        super().__init__()
        self.config = cpath
        self.db = PersistentQSQLite(cpath)

    def __call__(self):
        self.db.conn.execute("BEGIN IMMEDIATE")
        self.db.conn.execute("DELETE FROM queue WHERE state='invalid'")
        self.db.conn.commit()
        print("Cleaned invalid processes")
