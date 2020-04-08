import sys
import hashlib

filename = sys.argv[1]
# Open,close, read file and calculate MD5 on its contents
with open(filename, "r", encoding="utf-8") as file_to_check:
    # read contents of the file
    data = file_to_check.read()
    # pipe contents of the file through
    md5_returned = hashlib.md5(data.encode("utf-8")).hexdigest()

print(md5_returned)