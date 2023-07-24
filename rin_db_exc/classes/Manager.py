from rin_db_exc.classes.PIQ import PersistentQInterface
from rin_db_exc.classes.PersistentQSQLite import PersistentQSQLite


class Manager(PersistentQInterface):
    def __init__(self, cpath: str):
        super().__init__()
        self.config = cpath
        self.db = PersistentQSQLite(cpath)

    def __call__(self):
        self.db.update_status()
