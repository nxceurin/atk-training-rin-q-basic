import os
import sqlite3
from datetime import datetime as dt
from time import sleep

from rin_db_exc.classes.PIQ import PersistentQInterface
from rin_db_exc.classes.PersistentQSQLite import PersistentQSQLite
from rin_db_exc.process_tasks import coalesce_spaces, stair_case, append_date


class Consumer(PersistentQInterface):
    def __init__(self, name: str, cpath: str, tries: int = 3):
        super().__init__()
        self.name: str = name
        self.config: str = cpath
        self.max_tries: int = tries
        self.db = PersistentQSQLite(cpath)

    def __call__(self):
        for _ in range(5):
            try:
                curr_job, job_id = self.db.get_job_details()
                # if not curr_job:
                #     print("Queue empty. Exiting...")
                #     return
                self.db.set_state(job_id, "processing", "unprocessed")
                break
            except sqlite3.OperationalError:
                print("Database locked. Trying again in 2s.")
                sleep(2)
                continue
        else:
            print("Database locked for over 10s. Skipping...")
            return

        file_path = os.path.join(self.config, curr_job)

        for _ in range(self.max_tries):
            try:
                with open(file_path, 'r') as read_file:
                    content = read_file.read()
                content = coalesce_spaces(content)
                content = stair_case(content)
                content = append_date(content)
                break
            except FileNotFoundError:
                print(f"{file_path} can't be found!")
                self.db.set_state(job_id, "invalid")
                return
            except Exception:
                continue
        else:
            print(f"Can't process after {self.max_tries} tries. Skipping...")
            self.db.set_state(job_id, "invalid")
            return

        with open(file_path + '.processed', 'w') as write_file:
            write_file.write(content)
        print(f"[{self.name}] - {dt.now()} - [{curr_job}]")

    def get_job_name(self):
        return self.db.get_job_details()

    def set_invalid(self, job_id: int):
        self.db.set_state(job_id, "invalid")

    def set_completed(self, job_id: int):
        self.db.set_state(job_id, "processed")
