#!/usr/bin/env python
import time
import sys
import subprocess
import os


def get_size(start_path = './recathondata'):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size

def run(cmd):
   call = ["/bin/bash", "-c", cmd]
   ret = subprocess.call(call, stdout=None, stderr=None)
   if ret > 0:
      print "Warning - result was %d" % ret

args = sys.argv
args.remove(sys.argv[0])
args = " ".join(args)
print args

modelsizebefore = get_size()
start =  time.time()
run("java -jar -Xms1024m -Xmx1024m createrecommender.jar "+ args)
end =  time.time()
modelsizeafter = get_size()

recommendersize = modelsizeafter - modelsizebefore
elapsed = end - start

print recommendersize
print elapsed  

# Write mode creates a new file or overwrites the existing content of the file. 
# Write mode will _always_ destroy the existing contents of a file.
try:
    # This will create a new file or **overwrite an existing file**.
    f = open("createrecexpresults.txt", "a")
    try:
        f.write(args+"::"+ str(recommendersize) + "::" +str(elapsed) +"\n") # Write a string to a file        
    finally:
        f.close()
except IOError:
    pass                
