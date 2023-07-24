import os
from time import sleep

from rin_db_exc.PIQ import Cleaner
from rin_db_exc.log_error import get_yaml

fpath: str = get_yaml(cpath).get('general', {"primary_path": os.getcwd()}).get('primary_path', os.getcwd())
while True:
    cln = Cleaner(fpath)
    cln()
    sleep(30)
