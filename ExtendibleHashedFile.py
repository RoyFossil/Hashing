from Record import Record
from ExtendibleBlock import *
import math

class ExtendibleHashedFile:

	def __init__(self, blockSize, recordSize, fieldSize, fileLoc):
		self.file = fileLoc
		self.blockSize = blockSize
		# record size supplied by user should include the hash field size
		# 1 is added for the deletion marker
		self.recordSize = recordSize + 1
		self.fieldSize = fieldSize
		self.depthSize = 4
		self.bfr = math.floor((blockSize)/(self.recordSize))
		self.globalDepth = 0
		self.nextAvailableBucket = 3
	
		self.Directory = {}
		self.Directory[""] = 2
	
		# truncates the file
		with open(self.file, 'wb') as f:
			f.write(b"some heagfho[iserjiodfgfg")
			f.seek(self.blockSize*2)
			f.write(bytearray(self.blockSize))
			# set local depth to 0
			f.seek(self.blockSize*3 - self.depthSize)
			# update local depth value
			f.write((0).to_bytes(self.depthSize, byteorder='big'))
	
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
			print("Val: " + str(self.h1(value)) + " Count: " + str(count) + " LMB: " + self.getBinary(self.h1(value))[:count])
			return self.getBinary(self.h1(value))[:count]
		else:
			return ""
		
	def insert(self, value, record):
		#using the hash function
		bucket = self.getBucketPointer(value)
		print("Bucket: " + str(bucket))
		#format the record to be inserted
		formattedRecord = Record.new(self.recordSize, self.fieldSize, value, record)
		#open the file as binary read and write
		with open(self.file, 'r+b', buffering=self.blockSize) as f:
			#navigate to the appropriate bucket
			#plus 2 is to account for the header
			f.seek(self.blockSize*(bucket))
			#check to see if data exits in this bucket
			theBlock = self.makeBlock(f.read(self.blockSize))
			space = theBlock.hasSpace()
			if space>=0:
				# spot was open, move pointer back
				f.seek(self.blockSize*(bucket) + self.recordSize*space)
				#slot data in there boiiiiii
				f.write(formattedRecord.bytes)
			
			else:
				self.split(f, theBlock, formattedRecord, value)
	
	def split(self, mainFile, theBlock, theRecord, value):
		# If there's collision split bucket
		if theBlock.getLocalDepth() == self.globalDepth:
			newDirectory = {}
			for entry in self.Directory.keys():
				str1 = entry + "0"
				str2 = entry + "1"
				newDirectory[str1] = self.Directory[entry]
				newDirectory[str2] = self.Directory[entry]
			self.globalDepth += 1
			self.Directory = newDirectory
		print(self.Directory)
	
		allRecords = theBlock.getAllRecords()
		if not (theRecord is None):
			allRecords.append(theRecord)
		
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
				print("overflow while splitting")
				needAnotherSplit=True
				
		count=0
		for record in new:
			count+=1
			if count <= self.bfr:
				mainFile.seek(self.blockSize*(self.Directory[self.padVal(next)]) + self.recordSize*(count - 1))
				mainFile.write(record.bytes)
			else:
				print("overflow while splitting")
				needAnotherSplit=True
		
		self.nextAvailableBucket += 1
		newLocalDepth = theBlock.getLocalDepth() + 1
		mainFile.seek(self.blockSize*(self.Directory[self.padVal(curr)] + 1) - self.depthSize)
		mainFile.write(newLocalDepth.to_bytes(self.depthSize, byteorder='big'))
		mainFile.seek(self.blockSize*(self.Directory[self.padVal(next)] + 1) - self.depthSize)
		mainFile.write(newLocalDepth.to_bytes(self.depthSize, byteorder='big'))
		
		print(self.Directory)
		
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
		print(leftmost)
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
					print("Record not found")
		

		
	def search(self, value):
		theRecord = self.utilSearch(value, False, False)
		if not (theRecord is None):
			theRecord.prettyPrint()

	def update(self, value, data):
        #format record
		formattedRecord = Record.new(self.recordSize, self.fieldSize, value, data)
		
		bucket = self.getBucketPointer(value)
		
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
			
	def delete(self, value):
		recordInfo = self.utilSearch(value, True, False)
		with open(self.file, 'r+b', buffering=self.blockSize) as f:
			# navigate to the record to be updated
			f.seek(self.blockSize*(recordInfo["blockLoc"]) + self.recordSize*recordInfo["recordLoc"] + self.fieldSize)
			# set the deletion bit to 1
			f.write(b'\x01')
			
	def undelete(self, value):
		recordInfo = self.utilSearch(value, True, True)
		with open(self.file, 'r+b', buffering=self.blockSize) as f:
			# navigate to the record to be updated
			f.seek(self.blockSize*(recordInfo["blockLoc"]) + self.recordSize*recordInfo["recordLoc"] + self.fieldSize)
			# set the deletion bit to 0
			f.write(b'\x00')

	def displayHeader(self):
		print()
		print("globalDepth")
		print("Bucket: ")
		print("Block size: ")
		print("Record size: ")
		print("Field size: " )
		print("Number of records:")
		print("Number of records deleted:")
		print("BFR: ")
		print("Distinct values: ")
	
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
		for bucket in range(0, numBlocks-2):
			self.displayBlock(True, bucket)
			
	
	def makeBlock(self, data):
		return ExtendibleBlock(self.blockSize, self.recordSize, self.fieldSize, self.bfr, self.depthSize, data)