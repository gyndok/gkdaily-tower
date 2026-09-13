"""Copy a local canonical script into Drive's processed archive without overwrites."""
import hashlib
import os
import sys
import tempfile
from pathlib import Path


def archive(source, folder):
    source,folder=Path(source),Path(folder)
    data=source.read_bytes();folder.mkdir(parents=True,exist_ok=True)
    target=folder/source.name
    if target.exists():
        if target.read_bytes()==data:return str(target)
        target=folder/(source.stem+'.'+hashlib.sha256(data).hexdigest()[:12]+'.md')
        if target.exists():
            if target.read_bytes()!=data:raise RuntimeError('Archive content conflict')
            return str(target)
    fd,temp=tempfile.mkstemp(prefix='.archive-',dir=folder)
    try:
        with os.fdopen(fd,'wb') as stream:
            stream.write(data);stream.flush();os.fsync(stream.fileno())
        # Atomic exclusive creation: do not replace a concurrent archive writer.
        os.link(temp,target)
    finally:os.unlink(temp)
    return str(target)

if __name__=='__main__':print(archive(sys.argv[1],sys.argv[2]))
