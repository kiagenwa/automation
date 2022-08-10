import os
import requests

site = "http://34.136.239.129/feedback/"
datadir = "/data/feedback"
os.chdir(datadir)
flist = [f for f in os.listdir(datadir) if os.path.isfile(os.path.join(datadir, f))]
for file in flist:
    if file[-4:] != '.txt':
        continue
    with open(file) as data:
        fb = {}
        fb["title"] = data.readline().rstrip()
        fb["name"] = data.readline().rstrip()
        fb["date"] = data.readline().rstrip()
        fb["feedback"] = data.readline().rstrip()
        data.close()
    response = requests.post(site, json=fb)
    print("status: " + str(response.status_code))