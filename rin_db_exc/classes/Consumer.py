import os
from datetime import datetime as dt

from rin_db_exc.classes.PIQ import PersistentQInterface
from rin_db_exc.classes.PersistentQSQLite import PersistentQSQLite
from rin_db_exc.process_tasks import coalesce_spaces, stair_case, append_date


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

        with open(file_path + '.processed', 'w') as write_file:
            write_file.write(content)
        print(f"[{self.name}] - {dt.now()} - [{curr_job}]")

    def delete_entry(self):
        self.db.delete_entry()

    def get_job_name(self):
        if self.db.get_next_file_from_queue():
            return self.db.get_next_file_from_queue()
