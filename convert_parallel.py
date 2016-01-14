# /usr/bin/python
import subprocess
import csv
import time
import os
import argparse
import pprint
import itertools
import glob
from subprocess import Popen, PIPE
import time

start_time = time.time()

parser = argparse.ArgumentParser(description='Runs msconvert in parallel on Windows machine')
parser.add_argument('-i', '--input_dir', required=True, type=str, help='Input directory')
parser.add_argument('-o', '--output_suffix', default='_converted', type=str,
                    help='Ending added to input_dir for output')
parser.add_argument('-n', '--number_threads', default=4, type=int, help='Number of threads to use at a time')
parser.add_argument('-f', '--format', default="mzML", type=str, help='Format to convert to')
args = parser.parse_args()  # create an object having name and age as attributes)

data_dir = args.input_dir
output_suffix = args.output_suffix
new_format = args.format
threads = args.number_threads

output_dir = data_dir + output_suffix

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

bin = "msconvert.exe"
program = 'C:\\"Program Files"\ProteoWizard\\"ProteoWizard 3.0.9172"' + "\\" + bin


def split_seq(iterable, size):
    it = iter(iterable)
    item = list(itertools.islice(it, size))
    while item:
        yield item
        item = list(itertools.islice(it, size))


# filtering input files
myfiles = glob.glob(data_dir + '\*.wiff')

# splitting input into chunks of threads size
chunks = split_seq(myfiles, threads)

for chunk in list(chunks):

    for input_file in chunk:
        filename, file_extension = os.path.splitext(input_file)
        command = " ".join(['start', program, "--" + new_format, '--32 --zlib --filter "peakPicking true 1-" -o',
                            output_dir, input_file, "--outfile", filename + "." + new_format])
        print command
        Popen(command, shell=True)
        time.sleep(10)

    perform_check = True  # waiting until msconvert finishes
    while perform_check:
        p_tasklist = subprocess.Popen('tasklist.exe /fo csv',
                                      stdout=subprocess.PIPE,
                                      universal_newlines=True)
        pythons_tasklist = []
        for p in csv.DictReader(p_tasklist.stdout):
            if p['Image Name'] == bin:
                pythons_tasklist.append(p)

        if len(pythons_tasklist) > 0:
            print "Waiting task to complete...{}".format(time.time() - start_time)
            time.sleep(10)
            continue
        else:
            perform_check = False
            print "Chunk done"

elapsed_time = time.time() - start_time

print "Total time {}".format(elapsed_time)
os.system('pause')
