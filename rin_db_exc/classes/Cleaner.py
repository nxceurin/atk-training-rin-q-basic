import os

from rin_db_exc.classes.PIQ import PersistentQInterface


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
