import os
import sys
from time import sleep

from rin_db_exc.PIQ import Manager
from rin_db_exc.log_error import get_yaml

cpath: str = sys.argv[1:]
fpath: str = get_yaml(cpath[0]).get('general', {"primary_path": os.getcwd()}).get('primary_path', os.getcwd())
while True:
    manager = Manager(fpath)
    manager()
    sleep(60)
