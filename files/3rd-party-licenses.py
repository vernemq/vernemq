#!/bin/env python

import os
import json

os.system("license_scout -c files/license_scout.yml")

license_json = 'vernemq-dependency-licenses.json'

with open(license_json, 'r') as f:
    licenses = json.load(f)
f = open("3rd-party-licenses.txt","w")
f.write("3rd-party Licenses Summary\n")
f.write("==========================\n")
for l in licenses['dependencies']:
    license_ids = ""
    has_other_licenses = False
    for ll in l['licenses']:
        if ll['id'] == None:
            has_other_licenses = True
        else:
            license_ids += " " + ll['id']
    if has_other_licenses:
        license_ids += " (others see below)"
    f.write("%s: %s\n" % (l['name'], license_ids))

f.write("\n3rd-party Licenses Detailed\n")
f.write("===========================\n")
for l in licenses['dependencies']:
    f.write("\n%s:\n" % l['name'])
    f.write("---------------------------------------\n")
    for ll in l['licenses']:
        if ll['content']:
            f.write(ll['content'].encode('utf8'))
            f.write("\n")

f.close()

os.remove(license_json)
