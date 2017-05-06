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
			# writes null to entire first block
			# this is mainly so that the pointer for this block is marked as null
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
				self.writeRecordToOverflow(bucket, theBlock, formattedRecord)
				self.split(f)
				
	
	def split(self, mainFile):
		# navigate to the bucket to be split
		mainFile.seek(self.blockSize*(self.n+2))
		# load bucket to memory
		bucketToBeSplit = self.makeBlock(mainFile.read(self.blockSize))
		# clear out bucket on disk
		mainFile.seek(self.blockSize*(self.n+2))
		mainFile.write(bytearray(self.blockSize))
		pointer = bucketToBeSplit.getPointer() - 1
		# see if it is pointing to anything
		allRecords = []
		while pointer >= 0:
			with open(self.overflow, 'r+b') as overflow:
				# navigate to bucket of interest
				overflow.seek(self.blockSize*pointer)
				# read bucket into memory
				ofBucketToBeSplit = self.makeBlock(overflow.read(self.blockSize))
				# clear the heck out of that bucket
				overflow.seek(self.blockSize*pointer)
				overflow.write(bytearray(self.blockSize))
				allRecords.extend(ofBucketToBeSplit.getAllRecords())
				# check to see if overflow bucket had a pointer as well
				pointer = ofBucketToBeSplit.getPointer() - 1
		allRecords.extend(bucketToBeSplit.getAllRecords())
		# loop through all records
		origBucketRecords=[]
		grabbedBucketRecords=[]
		grabbedBucketNum=0
		for record in allRecords:
			# use second hash function to deterimine which bucket
			whichBucket = self.h2(record.getHashValueInt())
			if whichBucket == self.n:
				origBucketRecords.append(record)
			else:
				grabbedBucketRecords.append(record)
				grabbedBucketNum=whichBucket
		
		needAnotherSplit=False
		count=0
		for record in origBucketRecords:
			count+=1
			if count <= self.bfr:
				mainFile.seek(self.blockSize*(self.n+2) + self.recordSize*(count - 1))
				mainFile.write(record.bytes)
			else:
				self.writeRecordToOverflow(self.n, None, record)
				needAnotherSplit=True
				
		count=0
		for record in grabbedBucketRecords:
			count+=1
			if count <= self.bfr:
				mainFile.seek(self.blockSize*(grabbedBucketNum+2) + self.recordSize*(count - 1))
				mainFile.write(record.bytes)
			else:
				self.writeRecordToOverflow(grabbedBucketNum, None, record)
				needAnotherSplit=True
		# at this point we have rehashed all records and put them in their appropriate buckets
		# now we need to update n and m and hash functions
		
		self.increment_n()
		if needAnotherSplit:
			self.split(mainFile)
		
	def writeRecordToOverflow(self, bucketNum, origBucket, theRecord):
		with open(self.file, 'r+b') as mainFile:
			# origBucket was not passed in as an argument
			if not origBucket:
				# we can still created it by using the bucketNum
				mainFile.seek(self.blockSize*(bucketNum+2))
				origBucket = self.makeBlock(mainFile.read(self.blockSize))
			with open(self.overflow, 'r+b') as overflow:
				# need to ignore the 0 pointer because that'll be null pointer
				# but we don't want to waste the first block in overflow
				# so we could subtract one? or maybe use first block for info
				pointer = origBucket.getPointer() - 1
				# only if the pointer points to something, meaning that this block already has an overflow bucket assigned to it
				if pointer >= 0:
					while True:
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
							overflow.write(theRecord.bytes)
							break;
						else:
							print("we're having an overflow... in the overflow. OVERFLOWCEPTION")
							# check to see if this overflow block points to another overflow block
							nextPointer = overflowBlock.getPointer()-1
							# if it does not, we should make it point to another overflow block
							if nextPointer < 0:
								anotherOFBlock = self.findNextAvailableOFBucket()
								# navigate to it
								overflow.seek(self.blockSize*anotherOFBlock)
								# slot record in there
								overflow.write(theRecord.bytes)
								# navigate to previous overflow block
								overflow.seek(self.blockSize*(pointer+1) - self.blockPointerSize)
								# update pointer value
								overflow.write((anotherOFBlock+1).to_bytes(self.blockPointerSize, byteorder='big'))
								break;
							else:
								pointer = nextPointer
					
				# else the bucket doesn't already have an overflow bucket assigned to it. so we must make one.
				else:
					# but where do we make it?
					availableOFBucket = self.findNextAvailableOFBucket()
					# navigate to empty bucket
					overflow.seek(self.blockSize*availableOFBucket)
					# write the record there
					overflow.write(theRecord.bytes)
					# navigate to block in orig file
					mainFile.seek(self.blockSize*(bucketNum+3) - self.blockPointerSize)
					# update pointer value
					# add one because we're using 1 based indexing
					mainFile.write((availableOFBucket+1).to_bytes(self.blockPointerSize, byteorder='big'))
	
	def findNextAvailableOFBucket(self):
		# linear search through the overflow file? is that gross
		with open(self.overflow, 'r+b') as overflow:
			i = 0
			while True:
				# navigate to ith bucket in overflow file
				overflow.seek(i*self.blockSize)
				# load bucket into memory
				aBlock = self.makeBlock(overflow.read(self.blockSize))
				if aBlock.isEmpty():
					# found an empty bucket, end looping
					return i
				else:
					# bucket wasn't empty, check the next one
					i += 1
	
	def increment_n(self):
		self.n += 1
		# if n==m we need to reset
		if self.n == self.m:
			# reset n to zero
			self.n = 0
			# set m to twice m
			# this should update the hash functions as well
			# as they are based off of m
			self.m = 2*self.m
	
	def utilSearch(self, value, loc, searchDeleted):
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
			if searchDeleted and theBlock.containsRecordWithValueInclDeleted(value):
				if loc:
					main = True
					blockLoc = bucket
					recordLoc = theBlock.getRecordWithValueLocInclDeleted(value)
					return {"main": main, "blockLoc": blockLoc, "recordLoc": recordLoc}
				else:
					return theBlock.getRecordWithValueInclDeleted(value)
			elif (not searchDeleted) and theBlock.containsRecordWithValue(value):
				# load the record
				if loc:
					main = True
					blockLoc = bucket
					recordLoc = theBlock.getRecordWithValueLoc(value)
					return {"main": main, "blockLoc": blockLoc, "recordLoc": recordLoc}
				else:
					return theBlock.getRecordWithValue(value)
				
			else:
				# record was not in main file
				# get pointer to overflow bucket
				pointer = theBlock.getPointer() - 1
				while pointer >= 0:
					# there is an overflow block to check
					with open(self.overflow, 'rb', buffering=self.blockSize) as overflow:
						# navigate to overflow block
						overflow.seek(self.blockSize*pointer)
						# read into memory
						ofBlock = self.makeBlock(overflow.read(self.blockSize))
						# if its got the record
						if searchDeleted and ofBlock.containsRecordWithValueInclDeleted(value):
							if loc:
								main = False
								blockLoc = pointer
								recordLoc = ofBlock.getRecordWithValueLocInclDeleted(value)
								return {"main": main, "blockLoc": blockLoc, "recordLoc": recordLoc}
							else:
								return ofBlock.getRecordWithValueInclDeleted(value)
						elif (not searchDeleted) and ofBlock.containsRecordWithValue(value):
							# then return it
							if loc:
								main = False
								blockLoc = pointer
								recordLoc = ofBlock.getRecordWithValueLoc(value)
								return {"main": main, "blockLoc": blockLoc, "recordLoc": recordLoc}
							else:
								return ofBlock.getRecordWithValue(value)
						else:
							# else, check to see if this block points to another
							pointer = ofBlock.getPointer() - 1
				# there was no overflow block to check
				print("Record not found")
				return
	
	def search(self, value):
		theRecord = self.utilSearch(value, False, False)
		if not (theRecord is None):
			theRecord.prettyPrint()
		
	def update(self, value, data):
		# format the record to overwrite with
		formattedRecord = Record.new(self.recordSize, self.fieldSize, self.strKeys, value, data)
		recordInfo = self.utilSearch(value, True, False)
		if recordInfo["main"]:
			file = self.file
			# add two blocks for header??
			recordInfo["blockLoc"]+=2
		else:
			file = self.overflow
		with open(file, 'r+b') as f:
			# navigate to the record to be updated
			f.seek(self.blockSize*(recordInfo["blockLoc"]) + self.recordSize*recordInfo["recordLoc"])
			# write over the old record with new formatted one
			f.write(formattedRecord.bytes)
			
	def delete(self, value):
		recordInfo = self.utilSearch(value, True, False)
		if recordInfo["main"]:
			file = self.file
			# add two blocks for header??
			recordInfo["blockLoc"]+=2
		else:
			file = self.overflow
		with open(file, 'r+b') as f:
			# navigate to the record to be updated
			f.seek(self.blockSize*(recordInfo["blockLoc"]) + self.recordSize*recordInfo["recordLoc"] + self.fieldSize)
			# set the deletion bit to 1
			f.write(b'\x01')
	
	def undelete(self, value):
		recordInfo = self.utilSearch(value, True, True)
		if recordInfo["main"]:
			file = self.file
			# add two blocks for header??
			recordInfo["blockLoc"]+=2
		else:
			file = self.overflow
		with open(file, 'r+b') as f:
			# navigate to the record to be updated
			f.seek(self.blockSize*(recordInfo["blockLoc"]) + self.recordSize*recordInfo["recordLoc"] + self.fieldSize)
			# set the deletion bit to 0
			f.write(b'\x00')
	
	def displayHeader(self):
		print("header")
	
	def displayBlock(self, main, blockNum):
		if main:
			file = self.file
			blockNum+=2
			blockLabel = blockNum
		else:
			file = self.overflow
			blockLabel = "-->"
		
		with open(file, 'rb', buffering=self.blockSize) as f:
			# navigate to the given bucket
			# plus 2 to account for header
			f.seek(self.blockSize*blockNum)
			# load bucket into memory
			theBlock = self.makeBlock(f.read(self.blockSize))
			# dictionary with record location and record objects
			records = theBlock.getAllRecordsWithLoc()
			# line number of the number for the bucket (centered)
			labelLoc = self.bfr + 1
			# counter for lines written, will be used to insert labelLoc at right time
			linesWritten = 0
			if main:
				tabSize = 5
			else:
				tabSize = 10
			# loop through all possible locations
			for i in range(0, self.bfr):
				self.printTabOrBucketNum(tabSize, linesWritten, labelLoc, blockNum, blockLabel)
				print("-" * (1 + self.recordSize + 1))
				linesWritten+=1
				if i in records.keys():
					value = records[i].getHashValue()
					data = records[i].getData().decode()
					self.printTabOrBucketNum(tabSize, linesWritten, labelLoc, blockNum, blockLabel)
					print("|" + str(value) + " "*(self.fieldSize-len(str(value))) + "|" + data + " "*(self.recordSize-(self.fieldSize + len(data) + 1)) + "|")
					linesWritten+=1
				else:
					self.printTabOrBucketNum(tabSize, linesWritten, labelLoc, blockNum, blockLabel)
					print("|" + " "*(self.fieldSize) + "|" + " "*(self.recordSize-(self.fieldSize + 1)) + "|")
					linesWritten+=1
			print(" "*tabSize + "-" * (1 + self.recordSize + 1))
			# print overflow
			pointer = theBlock.getPointer() - 1:
			if pointer >= 0:
				self.displayBlock(False, pointer)
				
			
	def printTabOrBucketNum(self, tabSize, linesWritten, labelLoc, blockNum, blockLabel):
		if(linesWritten == labelLoc - 1):
			print(" "*math.ceil((tabSize-len(str(blockLabel)))/2) + str(blockLabel) + " "*math.floor((tabSize-len(str(blockLabel)))/2), end="")
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
