import struct, sys
sys.stdout.buffer.write(struct.pack("<HHQQQQ", 5, 0, 0, 0, 0, 0))
