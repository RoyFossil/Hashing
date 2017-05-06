from Record import *
from Block import *
import math

class StaticlyHashedFile:
	def __init__(self, blockSize, recordSize, fieldSize, fileSize, fileLoc):
		self.file = fileLoc
		self.blockSize = blockSize
		# record size supplied by user should include the hash field size
		# 1 is added for the deletion marker
		# or maybe not, because if a use submits a record size, that should be the record size
		# we should just notify the user that the available space for data is 
		# record size - (fieldSize + 1) and plus one is for deletion marker.
		self.recordSize = recordSize
		self.fieldSize = fieldSize
		self.fileSize = fileSize
		self.bfr = math.floor((blockSize)/recordSize)
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
			bucket = (self.h1(intValue)) + 2
			formattedRecord = Record.new(self.recordSize, self.fieldSize, False, value, record)
			with open(self.file, 'r+b') as f:
				# navigate to the appropriate bucket
				# plus 2 is to account for the header
				numChecked = 0
				while numChecked < self.fileSize:
					f.seek(self.blockSize*(bucket))
					# check to see if data exists in this bucket
					theBlock = self.makeBlock(f.read(self.blockSize))
					space = theBlock.hasSpace()
					if space>=0:
						f.seek(self.blockSize*(bucket) + self.recordSize*space)
						f.write(formattedRecord.bytes)
						self.noOfEntries+=1
						break
					else:
						numChecked+=1
						# there has been a collision. handle it
						print("move to the next available space")
						# check to see if data exists in the next avilable bucket
						bucket+=1
						if bucket >= self.fileSize:
							bucket = 2
						# else:	
							# print("you're fine")
								
		else:
			print("nahhhh dude, file's full")
			
	def search(self, value):
		theRecord = self.utilSearch(value, False, False)
		if not (theRecord is None):
			theRecord.prettyPrint()
		
	# def search(self, value):	
		
		# intValue = self.formatValue(value)
		# bucket = (self.h1(intValue)) + 2
		# with open(self.file, 'rb') as f:
			# # navigate to the appropriate bucket
			# # plus 2 is to account for the header
			# numChecked = 0
			# while numChecked < self.fileSize:
				# f.seek(self.blockSize*(bucket))
				# # load bucket into memory
				# theBlock = self.makeBlock(f.read(self.blockSize))
				# # currently only built to handle key values
				# if theBlock.containsRecordWithValue(value):
					# theRecord = theBlock.getRecordWithValue(value)
					# theRecord.prettyPrint()
					# break
				# else:
					# numChecked+=1
						# # there has been a collision. handle it
					# print("move to the next available space")
						# # check to see if data exists in the next avilable bucket
					# bucket+=1
					# if bucket >= self.fileSize:
						# bucket = 2
					# else:	
						# print("GO BACKKK")	
						
						
	def utilSearch(self, value , loc, searchDeleted):
		# pass value to first hash function
		bucket = self.h1(value) + 2 
		# open the file as binary read
		with open(self.file, 'rb') as f:	
			numChecked = 0
			while numChecked < self.fileSize:
				# navigate to the appropriate bucket
				# plus 2 is to account for the header
				f.seek(self.blockSize*(bucket))				
				# load bucket into memory
				theBlock = self.makeBlock(f.read(self.blockSize))
				# currently only built to handle key values
				if searchDeleted and theBlock.containsRecordWithValueInclDeleted(value):
				# load the record
					if loc:
						main = True
						blockLoc = bucket
						recordLoc = theBlock.containsRecordWithValueInclDeleted(value)
						return {"blockLoc": blockLoc, "recordLoc": recordLoc}
						
					else:
						return theBlock.getRecordWithValueWithValueInclDeleted(value)	
				elif (not searchDeleted) and theBlock.containsRecordWithValueInclDeleted(value):
					if loc:
						main = True
						blockLoc = bucket
						recordLoc = theBlock.getRecordWithValueLocInclDeleted(value)
						return {"blockLoc": blockLoc, "recordLoc": recordLoc}	
					else:
						return theBlock.getRecordWithValue(value)
					
				else:
					numChecked+=1
					print("check the next one")
					# check to see if data exists in the next avilable bucket
					bucket+=1
					if bucket >= self.fileSize:
						bucket = 2
						if loc:
							main = True
							blockLoc = bucket
							recordLoc = theBlock.containsRecordWithValueInclDeleted(value)
							return {"blockLoc": blockLoc, "recordLoc": recordLoc}
							
						else:
							theBlock.containsRecordWithValueInclDeleted(value)
					elif (not searchDeleted) and theBlock.containsRecordWithValueInclDeleted(value):
						if loc:
							main = False
							blockLoc = bucket
							recordLoc = theBlock.getRecordWithValueLocInclDeleted(value)
							return {"blockLoc": blockLoc, "recordLoc": recordLoc}	
						else:
							return theBlock.getRecordWithValue(value)
						
	def update(self, value, data):
		recordInfo = self.utilSearch(value, True)
		file = self.file
		with open(file, 'r+b') as f:
			# navigate to the record to be updated
			f.seek(self.blockSize*(recordInfo["blockLoc"]) + self.recordSize*recordInfo["recordLoc"])
			# write over the old record with new formatted one
			f.write(formattedRecord.bytes)
			print("ARE YOU GOING IN HERE:UPDATE")
	
	def delete(self,value):
		recordInfo = self.utilSearch(value, True, False)
		file = self.file
		with open(file, 'r+b') as f:
			# navigate to the record to be updated
			f.seek(self.blockSize*(recordInfo["blockLoc"]) + self.recordSize*recordInfo["recordLoc"] + self.fieldSize)
			# set the deletion bit to 1
			f.write(b'\x01')
			print("ARE YOU GOING IN HERE:DELETE")
			
	def undelete(self, value):
		recordInfo = self.utilSearch(value, True, True)
		file = self.file
		with open(file, 'r+b') as f:
			# navigate to the record to be updated
			f.seek(self.blockSize*(recordInfo["blockLoc"]) + self.recordSize*recordInfo["recordLoc"] + self.fieldSize)
			# set the deletion bit to 0
			f.write(b'\x00')
			print("ARE YOU GOING IN HERE:UNDELETE")	
				
	def makeBlock(self, data):
		return Block(self.blockSize, self.recordSize, self.fieldSize, self.bfr, data)			
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	