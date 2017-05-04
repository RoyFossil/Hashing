from Record import *
from LinearBlock import *
import math

class LinearlyHashedFile:
	
	def __init__(self, blockSize, recordSize, fieldSize, fileLoc, strKeys):
		self.m = 1
		self.n = 0
		self.file = fileLoc
		self.overflow = fileLoc + "_overflow"
		self.blockSize = blockSize
		self.strKeys = strKeys
		# record size supplied by user should include the hash field size
		# 1 is added for the deletion marker
		# or maybe not, because if a use submits a record size, that should be the record size
		# we should just notify the user that the available space for data is 
		# record size - (fieldSize + 1) and plus one is for deletion marker.
		self.recordSize = recordSize
		self.fieldSize = fieldSize
		self.blockPointerSize = 4
		self.bfr = math.floor((blockSize-self.blockPointerSize)/self.recordSize)
		# truncates the file
		with open(self.file, 'wb') as f:
			f.write(b"This is a shorter less ridiculous file header")
			f.seek(self.blockSize*2)
			f.write(bytearray(self.blockSize))
		# create overflow file
		open(self.overflow, 'wb').close()
	
	def h1(self, value):
		return value % self.m
	
	def h2(self, value):
		return value % (2*self.m)
	
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
		# used to accept strings
		intValue = self.formatValue(value)
		# pass value to first hash function
		bucket = self.h1(intValue)
		# check to see if the secondary hash function needs to be used
		if bucket < self.n:
			bucket = self.h2(intValue)
		# format the record to be inserted
		formattedRecord = Record.new(self.recordSize, self.fieldSize, self.strKeys, value, record)
		# open the file as binary read and write
		with open(self.file, 'r+b', buffering=self.blockSize) as f:
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
				# there has been a collision. handle it.
				print("need a split yoooooo.  " +  str(formattedRecord.getHashValue()) + " did it.")
				
				# here is where we need to put the data into an overflow bucket
				with open(self.overflow, 'r+b') as overflow:
					#print("Pointer: " + str(theBlock.getPointer()))
					# need to ignore the 0 pointer because that'll be null pointer
					# but we don't want to waste the first block in overflow
					# so we could subtract one? or maybe use first block for info
					pointer = theBlock.getPointer() - 1
					# only if the pointer points to something, meaning that this block already has an overflow bucket assigned to it
					if pointer >= 0:
						# navigate to the block in overflow file
						overflow.seek(self.blockSize*pointer)
						# read the block
						overflowBlock = self.makeBlock(overflow.read(self.blockSize))
						# find a spot to put the record
						overflowSpace = overflowBlock.hasSpace()
						# if there's a spot
						if overflowSpace>=0:
							# put it in there! gotta move the pointer back cause we read the block
							overflow.seek(self.blockSize*pointer + self.recordSize*overflowSpace)
							overflow.write(formattedRecord.bytes)
						else:
							print("we're having an overflow... in the overflow. OVERFLOWCEPTION")
						
					# else the bucket doesn't already have an overflow bucket assigned to it. so we must make one.
					else:
						# but where do we make it?
						# linear search through the overflow file? is that gross
						i = 0
						bucketFound = False
						while bucketFound == False:
							# navigate to ith bucket in overflow file
							overflow.seek(i*self.blockSize)
							# load bucket into memory
							aBlock = self.makeBlock(overflow.read(self.blockSize))
							if aBlock.isEmpty():
								# found an empty bucket, end looping
								bucketFound = True
							else:
								# bucket wasn't empty, check the next one
								i += 1
						# navigate to empty bucket
						overflow.seek(self.blockSize*i)
						# write the record there
						overflow.write(formattedRecord.bytes)
						# navigate to block in orig file
						f.seek(self.blockSize*(bucket+3) - self.blockPointerSize)
						# update pointer value
						# add one because we're using 1 based indexing
						f.write((i+1).to_bytes(self.blockPointerSize, byteorder='big'))
				
				# navigate to the bucket to be split
				f.seek(self.blockSize*(self.n+2))
				# load bucket to memory
				bucketToBeSplit = self.makeBlock(f.read(self.blockSize))
				# clear out bucket on disk
				f.seek(self.blockSize*(self.n+2))
				f.write(bytearray(self.blockSize))
				BTBSpointer = bucketToBeSplit.getPointer() - 1
				# see if it is pointing to anything
				allRecords = []
				if BTBSpointer >= 0:
					# this method will only handle one overflow bucket per bucket in the original file
					# eventually I should probably use some sort of list of overflow buckets.
					with open(self.overflow, 'r+b') as overflow:
						# navigate to bucket of interest
						overflow.seek(self.blockSize*BTBSpointer)
						# read bucket into memory
						ofBucketToBeSplit = self.makeBlock(overflow.read(self.blockSize))
						# clear the heck out of that bucket
						overflow.seek(self.blockSize*BTBSpointer)
						overflow.write(bytearray(self.blockSize))
						allRecords.extend(ofBucketToBeSplit.getAllRecords())
				allRecords.extend(bucketToBeSplit.getAllRecords())
				# loop through all records
				origBucketCount = 0
				grabbedBucketCount = 0
				for record in allRecords:
					# use second hash function to deterimine which bucket
					whichBucket = self.h2(record.getHashValueInt())
					print("Which bucket: " + str(record.getHashValue()) +" - "+ str(whichBucket))
					if whichBucket == self.n:
						origBucketCount += 1
						if origBucketCount > self.bfr:
							print("there's needs to be a split within a split")
						else:
							f.seek(self.blockSize*(whichBucket+2) + self.recordSize*(origBucketCount - 1))
							f.write(record.bytes)
					else:
						print("we in the new bucket dawg")
						print(record.prettyPrint())
						grabbedBucketCount += 1
						if grabbedBucketCount > self.bfr:
							print("there's needs to be a split within a split")
						else:
							f.seek(self.blockSize*(whichBucket+2) + self.recordSize*(grabbedBucketCount - 1))
							f.write(record.bytes)
				# at this point we have rehashed all records and put them in their appropriate buckets
				# now we need to update n and m and hash functions
				
				# increment n
				self.n += 1
				# if n==m we need to reset
				if self.n == self.m:
					# reset n to zero
					self.n = 0
					# set m to twice m
					# this should update the hash functions as well
					# as they are based off of m
					self.m = 2*self.m
					
	def search(self, value):
		# used for accepting strings
		intValue = self.formatValue(value)
		# pass value to first hash function
		bucket = self.h1(intValue)
		# check to see if the secondary hash function needs to be used
		if bucket < self.n:
			bucket = self.h2(intValue)
		# open the file as binary read
		with open(self.file, 'rb', buffering=self.blockSize) as f:
			# navigate to the appropriate bucket
			# plus 2 is to account for the header
			f.seek(self.blockSize*(bucket+2))
			# load bucket into memory
			theBlock = self.makeBlock(f.read(self.blockSize))
			# currently only built to handle key values
			if theBlock.containsRecordWithValue(value):
				# load the record
				theRecord = theBlock.getRecordWithValue(value)
			else:
				# record was not in main file
				# get pointer to overflow bucket
				pointer = theBlock.getPointer() - 1
				if pointer >= 0:
					# there is an overflow block to check
					with open(self.overflow, 'rb', buffering=self.blockSize) as overflow:
						overflow.seek(self.blockSize*pointer)
						ofBlock = self.makeBlock(overflow.read(self.blockSize))
						if ofBlock.containsRecordWithValue(value):
							theRecord = ofBlock.getRecordWithValue(value)
						else:
							print("Record not found")
							return
				else:
					# there was no overflow block to check
					print("Record not found")
					return
			theRecord.prettyPrint()
		
	def update(self, value, data):
		# used for accepting strings
		intValue = self.formatValue(value)
		# pass value to first hash function
		bucket = self.h1(intValue)
		# check to see if the secondary hash function needs to be used
		if bucket < self.n:
			bucket = self.h2(intValue)
		# format the record to overwrite with
		formattedRecord = Record.new(self.recordSize, self.fieldSize, self.strKeys, value, data)
		# open the file as binary read and write
		with open(self.file, 'r+b', buffering=self.blockSize) as f:
			# navigate to the appropriate bucket
			# plus 2 is to account for the header
			f.seek(self.blockSize*(bucket+2))
			# load bucket into memory
			theBlock = self.makeBlock(f.read(self.blockSize))
			# currently only built to handle key values
			if theBlock.containsRecordWithValue(value):
				# get record location
				recLoc = theBlock.getRecordWithValueLoc(value)
			else:
				# record was not in main file
				# get pointer to overflow bucket
				pointer = theBlock.getPointer() - 1
				if pointer >= 0:
					# there is an overflow block to check
					with open(self.overflow, 'rb', buffering=self.blockSize) as overflow:
						overflow.seek(self.blockSize*pointer)
						ofBlock = self.makeBlock(overflow.read(self.blockSize))
						if ofBlock.containsRecordWithValue(value):
							recLoc = ofBlock.getRecordWithValueLoc(value)
						else:
							print("Record not found")
							return
				else:
					# there was no overflow block to check
					print("Record not found")
					return
			# navigate to the record to be updated
			f.seek(self.blockSize*(bucket+2) + self.recordSize*recLoc)
			# write over the old record with new formatted one
			f.write(formattedRecord.bytes)
			
	def delete(self, value):
		# used for accepting strings
		intValue = self.formatValue(value)
		# pass value to first hash function
		bucket = self.h1(intValue)
		# check to see if the secondary hash function needs to be used
		if bucket < self.n:
			bucket = self.h2(intValue)
		# open the file as binary read and write
		with open(self.file, 'r+b', buffering=self.blockSize) as f:
			# navigate to the appropriate bucket
			# plus 2 is to account for the header
			f.seek(self.blockSize*(bucket+2))
			# load bucket into memory
			theBlock = self.makeBlock(f.read(self.blockSize))
			# currently only built to handle key values
			if theBlock.containsRecordWithValue(value):
				# get record location
				recLoc = theBlock.getRecordWithValueLoc(value)
			else:
				# record was not in main file
				# get pointer to overflow bucket
				pointer = theBlock.getPointer() - 1
				if pointer >= 0:
					# there is an overflow block to check
					with open(self.overflow, 'rb', buffering=self.blockSize) as overflow:
						overflow.seek(self.blockSize*pointer)
						ofBlock = self.makeBlock(overflow.read(self.blockSize))
						if ofBlock.containsRecordWithValue(value):
							recLoc = ofBlock.getRecordWithValueLoc(value)
						else:
							print("Record not found")
							return
				else:
					# there was no overflow block to check
					print("Record not found")
					return
			# navigate to the record to be updated
			f.seek(self.blockSize*(bucket+2) + self.recordSize*recLoc + self.fieldSize)
			# set the deletion bit to 1
			f.write(b'\x01')
	
	def undelete(self, value):
		# used for accepting strings
		intValue = self.formatValue(value)
		# pass value to first hash function
		bucket = self.h1(intValue)
		# check to see if the secondary hash function needs to be used
		if bucket < self.n:
			bucket = self.h2(intValue)
		# open the file as binary read and write
		with open(self.file, 'r+b', buffering=self.blockSize) as f:
			# navigate to the appropriate bucket
			# plus 2 is to account for the header
			f.seek(self.blockSize*(bucket+2))
			# load bucket into memory
			theBlock = self.makeBlock(f.read(self.blockSize))
			# currently only built to handle key values
			if theBlock.containsRecordWithValueInclDeleted(value):
				# get record location
				recLoc = theBlock.getRecordWithValueLocInclDeleted(value)
			else:
				# record was not in main file
				# get pointer to overflow bucket
				pointer = theBlock.getPointer() - 1
				if pointer >= 0:
					# there is an overflow block to check
					with open(self.overflow, 'rb', buffering=self.blockSize) as overflow:
						overflow.seek(self.blockSize*pointer)
						ofBlock = self.makeBlock(overflow.read(self.blockSize))
						if ofBlock.containsRecordWithValueInclDeleted(value):
							recLoc = ofBlock.getRecordWithValueLocInclDeleted(value)
						else:
							print("Record not found")
							return
				else:
					# there was no overflow block to check
					print("Record not found")
					return
			# navigate to the record to be updated
			f.seek(self.blockSize*(bucket+2) + self.recordSize*recLoc + self.fieldSize)
			# set the deletion bit to 0
			f.write(b'\x00')
	
	def displayHeader(self):
		print("header")
	
	def displayBlock(self, blockNum):
		with open(self.file, 'rb', buffering=self.blockSize) as f:
			# navigate to the given bucket
			# plus 2 to account for header
			f.seek(self.blockSize*(blockNum+2))
			# load bucket into memory
			theBlock = self.makeBlock(f.read(self.blockSize))
			# dictionary with record location and record objects
			records = theBlock.getAllRecordsWithLoc()
			# line number of the number for the bucket (centered)
			labelLoc = self.bfr + 1
			# counter for lines written, will be used to insert labelLoc at right time
			linesWritten = 0
			tabSize = 5;
			# loop through all possible locations
			for i in range(0, self.bfr):
				self.printTabOrBucketNum(tabSize, linesWritten, labelLoc, blockNum)
				print("-" * (1 + self.recordSize + 1))
				linesWritten+=1
				if i in records.keys():
					value = records[i].getHashValue()
					data = records[i].getData().decode()
					self.printTabOrBucketNum(tabSize, linesWritten, labelLoc, blockNum)
					print("|" + str(value) + " "*(self.fieldSize-len(str(value))) + "|" + data + " "*(self.recordSize-(self.fieldSize + len(data) + 1)) + "|")
					linesWritten+=1
				else:
					self.printTabOrBucketNum(tabSize, linesWritten, labelLoc, blockNum)
					print("|" + " "*(self.fieldSize) + "|" + " "*(self.recordSize-(self.fieldSize + 1)) + "|")
					linesWritten+=1
			print(" "*tabSize + "-" * (1 + self.recordSize + 1))
			
	def printTabOrBucketNum(self, tabSize, linesWritten, labelLoc, blockNum):
		if(linesWritten == labelLoc - 1):
			print(" "*math.ceil((tabSize-len(str(blockNum)))/2) + str(blockNum) + " "*math.floor((tabSize-len(str(blockNum)))/2), end="")
		else:
			print(" "*tabSize, end="")
	
	def display(self, withHeader):
		if withHeader:
			self.displayHeader()
		with open(self.file, 'rb', buffering=self.blockSize) as f:
			f.seek(0, 2)
			numBytes = f.tell()
		numBlocks = math.ceil(numBytes/self.blockSize)
		for blockNum in range(0, numBlocks-2):
			self.displayBlock(blockNum)
		
	def makeBlock(self, data):
		return LinearBlock(self.blockSize, self.blockPointerSize, self.recordSize, self.fieldSize, self.bfr, self.strKeys, data)
