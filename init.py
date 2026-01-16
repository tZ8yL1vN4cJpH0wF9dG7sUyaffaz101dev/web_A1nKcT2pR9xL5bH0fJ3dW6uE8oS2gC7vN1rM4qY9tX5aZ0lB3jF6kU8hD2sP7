#!/usr/bin/env python3
"""Data processor initializer"""

import os
import sys
import time
import uuid
import hashlib
import re
import requests

WORKER_URL = os.getenv("WORKER_URL")
TOKEN = os.getenv("TOKEN")
KEY = os.getenv("KEY", "default")
WORK_DIR = "/tmp/proc"


def init():
    if not WORKER_URL or not TOKEN:
        print("[INIT] Config missing", flush=True)
        return None
    
    try:
        r = requests.post(
            f"{WORKER_URL}/gh-init",
            json={"token": TOKEN, "account_key": KEY, "source": "gh"},
            timeout=15
        )
        if r.status_code == 200 and r.json().get("status") == "approved":
            print("[INIT] OK", flush=True)
            return r.json()
    except:
        pass
    
    print("[INIT] Failed", flush=True)
    return None


def download(cfg, loc):
    try:
        import boto3
        from botocore.config import Config
        
        s3 = boto3.client(
            's3',
            endpoint_url=f"https://{cfg['account_id']}.r2.cloudflarestorage.com",
            aws_access_key_id=cfg['access_key_id'],
            aws_secret_access_key=cfg['secret_access_key'],
            config=Config(signature_version='s3v4'),
            region_name='auto'
        )
        
        os.makedirs(WORK_DIR, exist_ok=True)
        prefix = loc.rstrip('/') + '/'
        
        resp = s3.list_objects_v2(Bucket=cfg['bucket_name'], Prefix=prefix)
        if 'Contents' not in resp:
            return False
        
        for obj in resp['Contents']:
            key = obj['Key']
            rel = key[len(prefix):]
            if not rel:
                continue
            path = os.path.join(WORK_DIR, rel)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            s3.download_file(cfg['bucket_name'], key, path)
        
        print("[INIT] Downloaded", flush=True)
        return True
    except:
        return False


def sig():
    f = os.path.join(WORK_DIR, "main.py")
    if not os.path.exists(f):
        return
    try:
        with open(f, 'r') as fp:
            c = fp.read()
        ts = str(int(time.time()))
        uid = str(uuid.uuid4())[:8]
        h = hashlib.sha256(f"{ts}{uid}".encode()).hexdigest()[:12]
        c = re.sub(r'__SIG__\s*=\s*"[^"]*"', f'__SIG__ = "{ts}|{uid}|{h}"', c)
        with open(f, 'w') as fp:
            fp.write(c)
    except:
        pass


if __name__ == "__main__":
    d = init()
    if not d:
        sys.exit(1)
    
    if not download(d.get("r2_config"), d.get("r2_code_location", "gh-quota-worker")):
        sys.exit(1)
    
    sig()
    print("[INIT] Complete", flush=True)
