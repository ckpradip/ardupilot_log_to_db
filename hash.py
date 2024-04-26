import os
import hashlib

def md5_hash_file(file_path):
    """Calculate MD5 hash of a file."""
    if not os.path.isfile(file_path):
        print(f"**Error : File does not exist in MD5: {file_path}**")
        return "NA"
    with open(file_path, 'rb') as file:
        file_data = file.read()
        md5_hash = hashlib.md5(file_data).hexdigest()
        return md5_hash