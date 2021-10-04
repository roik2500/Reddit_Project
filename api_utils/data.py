import json
from pathlib import Path
import tempfile
import tarfile
import zstandard

# pip install zstandard


def data():
    f = open('RC_2020-10-24.json')
    data = json.load(f)
    for i in data:
        print(i)
    f.close()

print(data())