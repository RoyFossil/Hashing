from Record import *
from Block import *
import math

class StaticlyHashedFile1:

	def __init__(self,blockSize, recordSize, fieldSize,fileLoc):

	        self.file=fileLoc		 
		self.blockSize= blockSize
		self.fieldSize=fieldSize
		self.recordSize=recordSize
		self.numOfblocks=256
	#if there is any space in the block when colission occurs
	        numOfBlocksare=[]
		#self.blockPointerSize= 4
		self.bfr= math.floor((self.blockSize)/self.recordSize)
		 with open(self.file, 'wb') as f:
			f.write(b"This is a shorter less ridiculous file header")
			f.seek(self.blockSize*2)
			f.write(bytearray(self.blockSize))
		
	#hash function:
	 def hash_function(self, value):
    i = value % self.numOfblocks
    return i
	
	#insert method
	def insert(self, value,record):
	#pass value to the hash function
    slot = self.hash_function(value)
    orig = slot
	#original logic
    while True:
        if self.numOfBlocksare[slot] is None:
            self.numOfBlocksare[slot] = 1
            return slot
			else:
			 if (self.numOfBlocksare[slot] +1) > self.bfr:
	 			slot = (slot + 1) % self.numOfblocks
				if slot == orig:
					return -1
			else:
				self.numOfBlocksare[slot] = self.numOfBlocksare[slot] + 1
				return slot
            #which block it should get into
			# format the record to be inserted
		formattedRecord = Record.new(self.recordSize, self.fieldSize, value, record)
			# open the file as binary read and write
			with open(self.file, 'r+b', buffering=self.blockSize) as f:
			# navigate to the appropriate bucket
			# plus 2 is to account for the header
			f.seek(self.blockSize*(slot+2))
			# check to see if data exists in this bucket
			theBlock = self.makeBlock(f.read(self.blockSize))
			space = theBlock.hasSpace()
			if space>=0:
	           # spot was open, move pointer back
				f.seek(self.blockSize*(bucket+2) + self.recordSize*space)
				# slot data in there boiii
				f.write(formattedRecord.bytes)
	         else:
			 print
	
	
	
	
	
	
	
	
	
	
	
	
	