from Record import *
from Block import *
import math
from timeit import default_timer as timer

class StaticlyHashedFile:
	def __init__(self, blockSize, recordSize, fieldSize, fileSize, strKeys,readFileArgs,   fileLoc):
		self.file = fileLoc
		self.blockSize = blockSize
		self.strKeys = strKeys
		# record size supplied by user should include the hash field size
		# 1 is added for the deletion marker
		# or maybe not, because if a use submits a record size, that should be the record size
		# we should just notify the user that the available space for data is 
		# record size - (fieldSize + 1) and plus one is for deletion marker.
		self.recordSize = recordSize
		self.fieldSize = fieldSize
		self.fileSize = fileSize
		self.bfr = math.floor((blockSize)/recordSize)
		self.times = False
		self.workings = False
		self.maxnoOfEntries= (self.bfr*fileSize)
		self.noOfEntries=0
		if not (readFileArgs is None):
			print("read from exisitng file")
		else:
			# truncates the file
			with open(self.file, 'wb') as f:
				f.seek(self.blockSize*2)
				f.write(bytearray(self.blockSize))
				self.writeFirstHeaderBlock()
				
	@classmethod
	def fromExistingFile(cls, fileLoc):
		extraFileArgs = {}
		with open(fileLoc, 'r+b') as f:
			f.seek(0)
			# extraFileArgs["fileSize"] = int.from_bytes(f.read(3), byteorder='big')
			# extraFileArgs["m"] = int.from_bytes(f.read(3), byteorder='big')
			# fileSize = int.from_bytes(f.read(3), byteorder='big')
			blockSize = int.from_bytes(f.read(3), byteorder='big')
			recordSize = int.from_bytes(f.read(3), byteorder='big')
			fieldSize = int.from_bytes(f.read(3), byteorder='big')
			fileSize = int.from_bytes(f.read(3), byteorder='big')
			if f.read(1) == b'\x01':
				strKeys = True
			else:
				strKeys = False
			f.seek(blockSize)
			# extraFileArgs["numRecords"] = int.from_bytes(f.read(6), byteorder='big')
			# extraFileArgs["numRecordsDeleted"] = int.from_bytes(f.read(3), byteorder='big')
		return cls(blockSize, recordSize, fieldSize, fileSize, strKeys, extraFileArgs, fileLoc)
	
	def writeFirstHeaderBlock(self):
		with open(self.file, 'r+b') as f:
			f.seek(0)
			f.write(bytearray(self.blockSize))
			f.seek(0)
			# f.write(self.n.to_bytes(3, byteorder='big'))
			# f.write(self.m.to_bytes(3, byteorder='big'))
			# f.write(self.fileSize.to_bytes(3, byteorder='big'))
			f.write(self.blockSize.to_bytes(3, byteorder='big'))
			f.write(self.recordSize.to_bytes(3, byteorder='big'))
			f.write(self.fieldSize.to_bytes(3, byteorder='big'))
			f.write(self.fileSize.to_bytes(3, byteorder='big'))
			f.write(b'\x00')

	def setStatistics(self, times, workings):
		self.times = times
		self.workings = workings
		
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
		start = timer()
		if self.workings:
			print("Search for key value first to ensure record does not already exist.")
			
		if not (self.utilSearch(value, False, False) is None):
			print("Record with that key already exists, cannot insert.")
			return
			
		if self.workings:
			print("Begin insert.")
			
		if self.noOfEntries < self.maxnoOfEntries:
			# used to accept strings
			intValue = self.formatValue(value)
			# pass value to first hash function
			bucket = (self.h1(intValue)) + 2
			if self.workings:
				print(str(value) + " maps to bucket " + str(bucket))
			formattedRecord = Record.new(self.recordSize, self.fieldSize, False, value, record)
			with open(self.file, 'r+b') as f:
				# navigate to the appropriate bucket
				# plus 2 is to account for the header
				numChecked = 0
				while numChecked < self.fileSize:
					f.seek(self.blockSize*(bucket))
					# check to see if data exists in this bucket
					if self.workings:
						print("Navigate to bucket " + str(bucket))
					theBlock = self.makeBlock(f.read(self.blockSize))
					space = theBlock.hasSpace()
					if space>=0:
						if self.workings:
							print("Space " + str(space) + " is available in this bucket.")
						f.seek(self.blockSize*(bucket) + self.recordSize*space)
						f.write(formattedRecord.bytes)
						self.noOfEntries+=1
						break
					else:
						numChecked+=1
						# there has been a collision. handle it
						if self.workings:
							print("The bucket is full, write the record to next available one.")
						print("move to the next available space")
						print("overflow here" +  str(formattedRecord.getHashValue()) + " did it.")
						# check to see if data exists in the next avilable bucket
						bucket+=1
						if bucket >= self.fileSize:
							bucket = 2
						# else:	
							# print("you're fine")
								
		else:
			print("nahhhh dude, file's full")
		end = timer()
		if self.times:
			print("Insert time: " + str((end-start)*1000) + "ms")	
			
	def utilSearch(self, value, loc, searchDeleted):	
		# used for accepting strings
		intValue = self.formatValue(value)
		bucket = self.h1(intValue) + 2 
		if self.workings:
			print(str(value) + " maps to bucket " + str(bucket))
		#open the file as binary read
		with open(self.file, 'rb') as f:
			#navigate to the appropraite bucket
			f.seek(self.blockSize*(bucket))
			if self.workings:
				print("Navigate to bucket " + str(bucket))
			# load bucket into memory
			#load bucket into memory
			theBlock = self.makeBlock(f.read(self.blockSize))
			# currently only built to handle key values
			if searchDeleted and theBlock.containsRecordWithValueInclDeleted(value):
				if self.workings:
					print("Record found in bucket " + str(bucket))
				if loc:
					blockLoc = bucket
					recordLoc = theBlock.getRecordWithValueLocInclDeleted(value)
					return {"blockLoc": blockLoc, "recordLoc": recordLoc}
				else:
					return theBlock.getRecordWithValueInclDeleted(value)
			elif (not searchDeleted) and theBlock.containsRecordWithValue(value):
				if self.workings:
					print("Record found in bucket " + str(bucket))
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
		if not (theRecord is None):
			theRecord.prettyPrint()
		else:
			print("Record not found")
		end = timer()
		if self.times:
			print("Search time: " + str((end-start)*1000) + "ms")
				
						
	def update(self, value, data):
		start = timer()
		formattedRecord = Record.new(self.recordSize, self.fieldSize, self.strKeys, value, data)
		recordInfo = self.utilSearch(value, True, False)
		file = self.file
		if not (recordInfo is None):
			with open(file, 'r+b') as f:
				# navigate to the record to be updated
				f.seek(self.blockSize*(recordInfo["blockLoc"]) + self.recordSize*recordInfo["recordLoc"])
				# write over the old record with new formatted one
				f.write(formattedRecord.bytes)
				print("record updated")
		else:
				print("record not found")
		end = timer()
		if self.times:
			print("update time: " + str((end-start)*1000) + "ms")
	
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
				print("record deleted")
		end = timer()
		if self.times:
			print("Delete time: " + str((end-start)*1000) + "ms")
			
	def undelete(self, value):
		start = timer()
		recordInfo = self.utilSearch(value, True, True)
		if recordInfo is None:
			print("Record not found")
		else:	
			with open(self.file, 'r+b') as f:
				# navigate to the record to be updated
				f.seek(self.blockSize*(recordInfo["blockLoc"]) + self.recordSize*recordInfo["recordLoc"] + self.fieldSize)
				# set the deletion bit to 0
				f.write(b'\x00')
				print("record undeleted")
		end = timer()
		if self.times:
			print("Undelete time: " + str((end-start)*1000) + "ms")
	
	def displayHeader(self):
		print("Block size: " + str(self.blockSize))
		print("Record size: " + str(self.recordSize))
		print("Field size: " + str(self.fieldSize))
		print("Uses strings for key values: " + str(self.strKeys))
		# print("Number of records: " + str(self.numRecords))
		# print("Number of records deleted: " + str(self.numRecordsDeleted))
		print("BFR: " + str(self.bfr))
		
	def displayBlock(self, blockNum):
		file = self.file
		blockNum+=2
		blockLabel = blockNum - 2
		with open(file, 'rb') as f:
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
			# loop through all possible locations
			for i in range(0, self.bfr):
				self.printTabOrBucketNum(linesWritten, labelLoc, blockNum, blockLabel)
				print("-" * (1 + self.recordSize + 1))
				linesWritten+=1
				if i in records.keys():
					value = records[i].getHashValue()
					data = records[i].getData().decode()
					self.printTabOrBucketNum(linesWritten, labelLoc, blockNum, blockLabel)
					print("|" + str(value) + " "*(self.fieldSize-len(str(value))) + "|" + data + " "*(self.recordSize-(self.fieldSize + len(data) + 1)) + "|")
					linesWritten+=1
				else:
					self.printTabOrBucketNum(linesWritten, labelLoc, blockNum, blockLabel)
					print("|" + " "*(self.fieldSize) + "|" + " "*(self.recordSize-(self.fieldSize + 1)) + "|")
					linesWritten+=1
					print(1 + self.recordSize + 1)

	def printTabOrBucketNum(self, linesWritten, labelLoc, blockNum, blockLabel):
		if(linesWritten == labelLoc - 1):
			print(" "*math.ceil((len(str(blockLabel)))/2) + str(blockLabel) + " "*math.floor((len(str(blockLabel)))/2), end="")
		else:
			print(end="")
			
	def display(self, withHeader):
		if withHeader:
			self.displayHeader()
		with open(self.file, 'rb') as f:
			f.seek(0, 2)
			numBytes = f.tell()
			numBlocks = math.ceil(numBytes/self.blockSize)
		for blockNum in range(0, numBlocks-2):
			self.displayBlock(blockNum)
			
	def makeBlock(self, data):
		return Block(self.blockSize, self.recordSize, self.fieldSize, self.bfr, self.strKeys, data)			
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	