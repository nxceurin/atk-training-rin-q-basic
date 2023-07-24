import os
import sys
from time import sleep
from typing import List

from rin_db_exc.classes.Producer import Producer
from rin_db_exc.log_error import get_yaml

cpath: List[str] = sys.argv[1:]
fpath: str = get_yaml(cpath[0]).get('general', {"primary_path": os.getcwd()}).get('primary_path', os.getcwd())
while True:
    prod = Producer(fpath)
    prod()
    sleep(5)
