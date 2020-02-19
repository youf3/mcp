import os, sys
import argparse
import math
import subprocess, shlex
import logging
logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)

def copy(src, dst, numjobs, blocksize, iflag, oflag):
    logging.info('Copying {} to {}'.format(src, dst))
    fsize = os.stat(args.infile).st_size
    processes = []
    
    bsb = blocksize * 1024 * 1024
    numblocks = math.ceil((int(fsize / (bsb)) + 1) / numjobs)

    for i in range(numjobs):
        cmd = 'dd if={0} of={1} bs={2}M skip={3} conv=sparse seek={3} iflag={4} oflag={5} count={6}'.format(src, dst, blocksize, numblocks * i, iflag, oflag, numblocks)

        logging.info(cmd)
        p = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        processes.append(p)

    for p in processes:
        out,err = p.communicate(timeout=None)
        logging.info(str(out))
        logging.error(str(err))

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('infile', type=str, help='File input')
parser.add_argument('outfile', type=str, help='File output')
parser.add_argument('--numjobs', '-n', type=int, default=3, help='Number of jobs')
parser.add_argument('--blocksize', '-bs', type=int, default=4, help='block size in MB')
parser.add_argument('--iflag', type=str, default='direct', help='iflag for dd')
parser.add_argument('--oflag', type=str, default='direct', help='oflag for dd')
args = parser.parse_args()

copy(args.infile, args.outfile, args.numjobs, args.blocksize, args.iflag, args.oflag)

print()