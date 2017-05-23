from Record import Record
from ExtendibleBlock import *
import math
from timeit import default_timer as timer
class ExtendibleHashedFile:

	def __init__(self, blockSize, recordSize, fieldSize, fileLoc, strKeys, readFileArgs):
		self.file = fileLoc
		self.blockSize = blockSize
		self.strKeys = strKeys
		# record size supplied by user should include the hash field size
		# 1 is added for the deletion marker
		self.recordSize = recordSize + 1
		self.fieldSize = fieldSize
		self.depthSize = 4
		self.bfr = math.floor((blockSize)/(self.recordSize))
		self.globalDepth = 0
		self.nextAvailableBucket = 3
		self.times = False
		self.workings = False
		self.numRecords = 0
		self.Directory = {}
		self.Directory[""] = 2
	#if not (readFileArgs is None):
			#self.m = readFileArgs["m"]
			#self.n = readFileArgs["n"]
			#self.numRecords = readFileArgs["numRecords"]
			#self.numRecordsDeleted = readFileArgs["numRecordsDeleted"]
		#else:
			#self.m = 1
			#self.n = 0
			#self.numRecords = 0
			#self.numRecordsDeleted = 0
		# truncates the file
		with open(self.file, 'wb') as f:
			f.write(b"some header")
			f.seek(self.blockSize*2)
			f.write(bytearray(self.blockSize))
			# set local depth to 0
			f.seek(self.blockSize*3 - self.depthSize)
			# update local depth value
			f.write((0).to_bytes(self.depthSize, byteorder='big'))
	
	@classmethod
	def fromExistingFile(cls, fileLoc):
		extraFileArgs = {}
		theDirectory = {}
		with open(fileLoc, 'r+b') as f:
			f.seek(0)
			extraFileArgs["globalDepth"] = int.from_bytes(f.read(3), byteorder='big')
			blockSize = int.from_bytes(f.read(3), byteorder='big')
			recordSize = int.from_bytes(f.read(3), byteorder='big')
			fieldSize = int.from_bytes(f.read(3), byteorder='big')
			if f.read(1) == b'\x01':
				strKeys = True
			else:
				strKeys = False
			f.seek(blockSize)
			extraFileArgs["numRecords"] = int.from_bytes(f.read(6), byteorder='big')
			extraFileArgs["numRecordsDeleted"] = int.from_bytes(f.read(3), byteorder='big')
			for pair in range(0, 2**extraFileArgs["globalDepth"]):
				intKey = int.from_bytes(f.read(1), byteorder='big')
				formattedKey = cls.getBinary(intKey, extraFileArgs["globalDepth"])
				value = int.from_bytes(f.read(1), byteorder='big')
				theDirectory[formattedKey] = value
		extraFileArgs["Directory"] = theDirectory
		return cls(blockSize, recordSize, fieldSize, fileLoc, strKeys, extraFileArgs)		
	def writeFirstHeaderBlock(self):
		with open(self.file, 'r+b') as f:
			f.seek(0)
			f.write(bytearray(self.blockSize))
			f.seek(0)
			f.write(self.n.to_bytes(3, byteorder='big'))
			f.write(self.m.to_bytes(3, byteorder='big'))
			f.write(self.blockSize.to_bytes(3, byteorder='big'))
			f.write(self.recordSize.to_bytes(3, byteorder='big'))
			f.write(self.fieldSize.to_bytes(3, byteorder='big'))
			if self.strKeys:
				f.write(b'\x01')
			else:
				f.write(b'\x00')
	def writeSecondHeaderBlock(self):
		with open(self.file, 'r+b') as f:
			f.seek(self.blockSize)
			f.write(bytearray(self.blockSize))
			f.seek(self.blockSize)
			f.write(self.numRecords.to_bytes(6, byteorder='big'))
			f.write(self.numRecordsDeleted.to_bytes(3, byteorder='big'))
			f.write(self.bfr.to_bytes(1, byteorder='big'))
			f.write(self.numRecords.to_bytes(6, byteorder='big'))
		
	
	def h1(self, value):
		return value % 32
		
	def getBinary(self, value):
		binary = "{0:b}".format(value)
		while len(binary) < 5:
			binary = "0" + binary
		return binary
			
	def getLeftmostBits(self, value, count):
		if count>0:
			return self.getBinary(self.h1(value))[:count]
		else:
			return ""
		
	def insert(self, value, record):
		start = timer()
		if self.workings:
			print("Search for key value first to ensure record does not already exist.")
		if not (self.utilSearch(value, False, False) is None):
			print("Record with that key already exists, cannot insert.")
			return	
		if self.workings:
			print("Begin insert.")
		
			
		#using the hash function
		bucket = self.getBucketPointer(value)
		
		if self.workings:
			print(str(value) + " maps to bucket " + str(bucket))
		
		#format the record to be inserted
		formattedRecord = Record.new(self.recordSize, self.fieldSize, False, value, record)
		#open the file as binary read and write
		with open(self.file, 'r+b', buffering=self.blockSize) as f:
			#navigate to the appropriate bucket
			f.seek(self.blockSize*(bucket))
			
			if self.workings:
				print("Navigate to bucket " + str(bucket))
			
			#check to see if data exits in this bucket
			theBlock = self.makeBlock(f.read(self.blockSize))
			space = theBlock.hasSpace()
			if space>=0:
				if self.workings:
					print("Space " + str(space) + " is available in this bucket.")
				# spot was open, move pointer back
				f.seek(self.blockSize*(bucket) + self.recordSize*space)
				#slot data in there boiiiiii
				f.write(formattedRecord.bytes)
				if self.workings:
					print("The record was inserted at record number " + str(space) + " in bucket " + str(bucket) + ".")
			
			else:
				if self.workings:
					print("The bucket is full.  Initiate a split.")
				# clear out bucket on disk
				f.seek(self.blockSize*(bucket))
				f.write(bytearray(self.blockSize))
				self.split(f, theBlock, formattedRecord, value)
		end = timer()		
		if self.times:
			print("Insert time: " + str((end-start)*1000) + "ms")

	def split(self, mainFile, theBlock, theRecord, value):
		
		bucket = self.getBucketPointer(value)
		
		if self.workings:
			print("Bucket " + str() + " will be split.")
		
		# If there's collision split bucket
		if theBlock.getLocalDepth() == self.globalDepth:
			if self.workings:
				print("The local depth of bucket " + str(bucket) + " is equal to the global depth.")
				print("This means the directory must be expanded.")
			newDirectory = {}
			for entry in self.Directory.keys():
				str1 = entry + "0"
				str2 = entry + "1"
				newDirectory[str1] = self.Directory[entry]
				newDirectory[str2] = self.Directory[entry]
			self.globalDepth += 1
			self.Directory = newDirectory
			if self.workings:
				print("Here is the new directory: ")
				print(self.Directory)
	
		allRecords = theBlock.getAllRecords()
		allRecords.append(theRecord)
		
		if self.workings:
			print(str(len(allRecords)) + " record(s) will be rehashed." )
		
		orig=[]
		new=[]
		needsAnotherSplit = False
		curr = self.getLeftmostBits(value, theBlock.getLocalDepth()) + "0"
		next = self.getLeftmostBits(value, theBlock.getLocalDepth()) + "1"
		self.Directory[self.padVal(next)] = self.nextAvailableBucket
		for record in allRecords:
			#use the leftmost significant bits to determine which bucket
			whichBucket = self.getLeftmostBits(record.getHashValue(), theBlock.getLocalDepth() + 1)
			if whichBucket == curr:
				orig.append(record)
			if whichBucket == next:
				new.append(record)
		
		#to put records in thier appropraite bucket after hash functions
		count=0
		for record in orig:
			count+=1
			if count <= self.bfr:
				mainFile.seek(self.blockSize*(self.Directory[self.padVal(curr)]) + self.recordSize*(count - 1))
				mainFile.write(record.bytes)
			else:
				#print("overflow while splitting")
				needAnotherSplit=True
		if self.workings and count:
			print(str(count) + " records rehashed to bucket " + str(bucket))
		
		count=0
		for record in new:
			count+=1
			if count <= self.bfr:
				mainFile.seek(self.blockSize*(self.Directory[self.padVal(next)]) + self.recordSize*(count - 1))
				mainFile.write(record.bytes)
			else:
				#print("overflow while splitting")
				needAnotherSplit=True
		if self.workings and count:
			print(str(count) + " records rehashed to bucket " + str(self.nextAvailableBucket))
		
		self.nextAvailableBucket += 1
		newLocalDepth = theBlock.getLocalDepth() + 1
		mainFile.seek(self.blockSize*(self.Directory[self.padVal(curr)] + 1) - self.depthSize)
		mainFile.write(newLocalDepth.to_bytes(self.depthSize, byteorder='big'))
		mainFile.seek(self.blockSize*(self.Directory[self.padVal(next)] + 1) - self.depthSize)
		mainFile.write(newLocalDepth.to_bytes(self.depthSize, byteorder='big'))
		
		#print(self.Directory)
		
		if needsAnotherSplit:
			if len(orig) > len(new):
				value = curr
				aRecord = orig[len(orig)-1]
				mainFile.seek(self.blockSize*(self.Directory[self.padVal(curr)]))
				theBlock = self.makeBlock(mainFile.read(self.blockSize))
			else:
				value = next
				aRecord = new[len(new)-1]
				mainFile.seek(self.blockSize*(self.Directory[self.padVal(next)]))
				theBlock = self.makeBlock(mainFile.read(self.blockSize))
			self.split(f, theBlock, aRecord, value)
	
	def padVal(self, val):
		while len(val) < self.globalDepth:
			val = val + "0"
		return val
	
	def getBucketPointer(self, value):
		leftmost = self.getLeftmostBits(value, self.globalDepth)
		#print(leftmost)
		return self.Directory[self.padVal(leftmost)]
		
	def utilSearch(self, value, loc, searchDeleted):	
		bucket =self.getBucketPointer(value)
		#open the file as binary read
		with open(self.file, 'r+b', buffering=self.blockSize) as f:
			#navigate to the appropraite bucket
			f.seek(self.blockSize*(bucket))
			#load bucket into memory
			theBlock = self.makeBlock(f.read(self.blockSize))
			# currently only built to handle key values
			if searchDeleted and theBlock.containsRecordWithValueInclDeleted(value):
				if loc:
					blockLoc = bucket
					recordLoc = theBlock.getRecordWithValueLocInclDeleted(value)
					return {"blockLoc": blockLoc, "recordLoc": recordLoc}
				else:
					return theBlock.getRecordWithValueInclDeleted(value)
			elif (not searchDeleted) and theBlock.containsRecordWithValue(value):
				# load the record
				if loc:
					blockLoc = bucket
					recordLoc = theBlock.getRecordWithValueLoc(value)
					return {"blockLoc": blockLoc, "recordLoc": recordLoc}
				else:
					return theBlock.getRecordWithValue(value)
			else:
					return
		
	def search(self, value):
		start = timer()
		theRecord = self.utilSearch(value, False, False)
		if theRecord is None:
			print("Record not found")
		else:
			theRecord.prettyPrint()
		end = timer()
		if self.times:
			print("Search time: " + str((end-start)*1000) + "ms")	

	def update(self, value, data):
		start = timer()
        #format record
		formattedRecord = Record.new(self.recordSize, self.fieldSize, False, value, data)
		bucket = self.getBucketPointer(value)
		recordInfo = self.utilSearch(value, True, False)
		if recordInfo is None:
			print("Record not found.")
		else:
			# open the file as binary read and write
			with open(self.file, 'r+b', buffering=self.blockSize) as f:
				# navigate to the appropriate bucket
				# plus 2 is to account for the header
				f.seek(self.blockSize*(bucket))
				# load bucket into memory
				theBlock = self.makeBlock(f.read(self.blockSize))
				if theBlock.containsRecordWithValue(value):
					recLoc = theBlock.getRecordWithValueLoc(value)
					f.seek(self.blockSize*bucket + self.recordSize*recLoc)
					f.write(formattedRecord.bytes)
		end = timer()
		if self.times:
			print("Search time: " + str((end-start)*1000) + "ms")	
			
	def delete(self, value):
		start = timer()
		recordInfo = self.utilSearch(value, True, False)
		if recordInfo is None:
			print("Record not found")
		else:
			with open(self.file, 'r+b', buffering=self.blockSize) as f:
				# navigate to the record to be updated
				f.seek(self.blockSize*(recordInfo["blockLoc"]) + self.recordSize*recordInfo["recordLoc"] + self.fieldSize)
				# set the deletion bit to 1
				f.write(b'\x01')
		end = timer()
		if self.times:
			print("Delete time: " + str((end-start)*1000) + "ms")
		
			
	def undelete(self, value):
		start = timer()
		recordInfo = self.utilSearch(value, True, True)
		if recordInfo is None:
			print("Record not found")
		else:	
			with open(self.file, 'r+b', buffering=self.blockSize) as f:
				# navigate to the record to be updated
				f.seek(self.blockSize*(recordInfo["blockLoc"]) + self.recordSize*recordInfo["recordLoc"] + self.fieldSize)
				# set the deletion bit to 0
				f.write(b'\x00')
		end = timer()
		if self.times:
			print("Undelete time: " + str((end-start)*1000) + "ms")

	def setStatistics(self, times, workings):
		self.times = times
		self.workings = workings
	
	def displayBlock(self, bucket):
		with open(self.file, 'r+b', buffering=self.blockSize) as f:
			#navigate to the bucket
			f.seek(self.blockSize*(bucket))
			#load bucket into memeory
			theBlock = self.makeBlock(f.read(self.blockSize))
			#dictionary with record loaction and record objects
			records = theBlock.getAllRecordsWithLoc()
			# line number of the number for the bucket (centered)
			labelLoc = self.bfr + 1
			# counter for lines written, will be used to insert labelLoc at right time
			linesWritten = 0
			tabSize = 5
			blockLabel = bucket
			
			
			# loop through all possible locations
			for i in range(0, self.bfr):
				self.printTabOrBucketNum(tabSize, linesWritten, labelLoc, bucket, blockLabel)
				print("-" * (1 + self.recordSize + 1))
				linesWritten+=1
				if i in records.keys():
					value = records[i].getHashValue()
					data = records[i].getData().decode()
					self.printTabOrBucketNum(tabSize, linesWritten, labelLoc, bucket, blockLabel)
					print("|" + str(value) + " "*(self.fieldSize-len(str(value))) + "|" + data + " "*(self.recordSize-(self.fieldSize + len(data) + 1)) + "|")
					linesWritten+=1
				else:
					self.printTabOrBucketNum(tabSize, linesWritten, labelLoc, bucket, blockLabel)
					print("|" + " "*(self.fieldSize) + "|" + " "*(self.recordSize-(self.fieldSize + 1)) + "|")
					linesWritten+=1
		print(" "*tabSize + "-" * (1 + self.recordSize + 1))
	
	def printTabOrBucketNum(self, tabSize, linesWritten, labelLoc, bucket, blockLabel):
		if(linesWritten == labelLoc - 1):
			print(" "*math.ceil((tabSize-len(str(blockLabel)))/2) + str(blockLabel) + " "*math.floor((tabSize-len(str(blockLabel)))/2), end="")
		else:
			print(" "*tabSize, end="")	
	
	def display(self, withHeader):
		if withHeader:
			self.displayHeader()
		with open(self.file, 'r+b', buffering=self.blockSize) as f:
			f.seek(0, 2)
			numBytes = f.tell()
		numBlocks = math.ceil(numBytes/self.blockSize)
		print(numBlocks)
		for bucket in range(2, numBlocks):
			self.displayBlock(bucket)
	def readDirectoryFromHeader(self, globalDepth):
		theDirectory = {}
		with open(self.file, 'r+b') as f:
			f.seek(blockSize)
			if globalDepth == 0:
				f.seek(blockSize+1)
				theDirectory[''] = int.from_bytes(f.read(1), byteorder='big')
			else:
				for pair in range(0, 2**globalDepth):
					intKey = int.from_bytes(f.read(1), byteorder='big')
					formattedKey = self.getBinary(intKey, globalDepth)
					value = int.from_bytes(f.read(1), byteorder='big')
					theDirectory[formattedKey] = value
		return theDirectory		
	#def displayHeader(self):
		#print("n: " + str(self.n))
		#print("m: " + str(self.m))
		#print("Block size: " + str(self.blockSize))
		#print("Record size: " + str(self.recordSize))
		#print("Field size: " + str(self.fieldSize))
		#print("Uses strings for key values: " + str(self.strKeys))
		#print("Number of records: " + str(self.numRecords))
		#print("Number of records deleted: " + str(self.numRecordsDeleted))
		#print("BFR: " + str(self.bfr))
		#print("Distinct values: " + str(self.numRecords))		
			
	# takes two boolean arguments
	# time: print time to complete each operation
	# working: print navigation and other info
	# note: times will be artificially increased if working is on at the same time
	# recommend picking either one or the other
	def setStatistics(self, times, workings):
		self.times = times
		self.workings = workings
	
	def makeBlock(self, data):
		return ExtendibleBlock(self.blockSize, self.recordSize, self.fieldSize, self.bfr, self.depthSize, data)