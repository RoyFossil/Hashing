from Record import *
from Block import *
import math

class StaticlyHashedFile:
	def __init__(self, blockSize, recordSize, fieldSize,fileSize, fileLoc):
		self.file = fileLoc
		self.blockSize = blockSize
		# record size supplied by user should include the hash field size
		# 1 is added for the deletion marker
		# or maybe not, because if a use submits a record size, that should be the record size
		# we should just notify the user that the available space for data is 
		# record size - (fieldSize + 1) and plus one is for deletion marker.
		self.recordSize = recordSize
		self.fieldSize = fieldSize
		self.fileSize= fileSize
		self.blockPointerSize = 4
		self.bfr = math.floor((blockSize-self.blockPointerSize)/self.recordSize)
		self.maxnoOfEntries= (self.bfr*fileSize)
		self.noOfEntries=0
		
		# truncates the file
		with open(self.file, 'wb') as f:
			f.write(b"This is a shorter less ridiculous file header")
			f.seek(self.blockSize*2)
			f.write(bytearray(self.blockSize))

	
	def h1(self, value):
		return value % self.fileSize
	
	def formatValue(self, value):
		if type(value) is int:
			return value
		elif type(value) is str:
			return self.sumChars(value)
			
	def sumChars(self, string):
		sum = 0
		for c in string:
			sum += ord(c)
		return sum
	
	def insert(self, value, record):
		if self.noOfEntries < self.maxnoOfEntries:
			# used to accept strings
			intValue = self.formatValue(value)
			# pass value to first hash function
			bucket = self.h1(intValue)
			formattedRecord = Record.new(self.recordSize, self.fieldSize, value, record)
			with open(self.file, 'r+b') as f:
				# navigate to the appropriate bucket
				# plus 2 is to account for the header
				f.seek(self.blockSize*(bucket+2))
				# check to see if data exists in this bucket
				theBlock = self.makeBlock(f.read(self.blockSize))
				space = theBlock.hasSpace()
				if space>=0:
					# spot was open, move pointer back
					f.seek(self.blockSize*(bucket+2) + self.recordSize*space)
					# slot data in there boiii
					f.write(formattedRecord.bytes)
				else:
					# there has been a collision. handle it
					print("move to the next available space")
					# check to see if data exists in the next avilable bucket
					bucket+=1
					# spot was open, move pointer back
					f.seek(self.blockSize*(bucket+2) + self.recordSize*space)
					# slot data in there boiii
					f.write(formattedRecord.bytes)
			self.noOfEntries=self.noOfEntries+1
		else:
			print("nahhhh dude, file's full")
		
				
	# def search(self, value):	
		# intValue = self.formatValue(value)
		# bucket = self.h1(value)
		# with open(self.file, 'rb') as f:
			# # navigate to the appropriate bucket
			# # plus 2 is to account for the header
			# f.seek(self.blockSize*(bucket+2))
			# # load bucket into memory
			# theBlock = self.makeBlock(f.read(self.blockSize))
			# # currently only built to handle key values
			# if theBlock.containsRecordWithValue(value):
				# theRecord = theBlock.getRecordWithValue(value)
				# print(theRecord.bytes)
			# else:
				# print("search in the next available space")
				# # check to see if data exists in the next avilable bucket
				# bucket+=1
				# # spot was open, move pointer back
				# f.seek(self.blockSize*bucket)
				# theRecord = theBlock.getRecordWithValue(value)
				# print(theRecord.bytes)
				# # # slot data in there boiii
				# # f.write(formattedRecord.bytes)

				
	def makeBlock(self, data):
		return Block(self.blockSize, self.blockPointerSize, self.recordSize, self.fieldSize, self.bfr, data)			
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	# #insert method
	# def insert(self, value,record):
		# #pass value to the hash function
		# slot = self.hash_function(value)
		# orig = slot
		# #original logic
		# while True:
			# if self.numOfBlocksare[slot] is None:
				# # self.numOfBlocksare[slot] = 1
				# return slot
			# elif (self.numOfBlocksare[slot] +1) > self.bfr:
				# slot = (slot + 1) % self.numOfblocks
			# elif slot == orig:
				# return -1
			# else:
				# self.numOfBlocksare[slot] = self.numOfBlocksare[slot] + 1
				# return slot
            # #which block it should get into
			# # format the record to be inserted
		# formattedRecord = Record.new(self.recordSize, self.fieldSize, value, record)
			# # open the file as binary read and write
		# with open(self.file, 'r+b') as f:
			# # navigate to the appropriate bucket
			# # plus 2 is to account for the header
			# f.seek(self.blockSize*(slot+2))
			# # check to see if data exists in this bucket
			# theBlock = self.makeBlock(f.read(self.blockSize))
			# space = theBlock.hasSpace()
			# if space>=0:
	           # # spot was open, move pointer back
				# f.seek(self.blockSize*(bucket+2) + self.recordSize*space)
				# # slot data in there boiii
				# f.write(formattedRecord.bytes)
			# else:
				# print("sorry yo")
	
	
	
	
	
	
	
	
	
	
	
	
	