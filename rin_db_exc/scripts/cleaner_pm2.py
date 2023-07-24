import os
import sys
from time import sleep
from typing import List

from rin_db_exc.classes.Cleaner import Cleaner
from rin_db_exc.log_error import get_yaml

cpath: List[str] = sys.argv[1:]
fpath: str = get_yaml(cpath[0]).get('general', {"primary_path": os.getcwd()}).get('primary_path', os.getcwd())
while True:
    cln = Cleaner(fpath)
    cln()
    sleep(30)
