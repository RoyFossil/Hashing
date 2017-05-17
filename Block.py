from Record import *

class Block: 
	
	def __init__(self, size, recordSize, fieldSize, bfr, strKeys, data):
		self.size = size
		# size of entire record (including hashing field)
		self.recordSize = recordSize
		self.fieldSize = fieldSize
		self.bfr = bfr
		self.strKeys = strKeys
		self.data = data
	
	# returns the index of the available space within a block, -1 if full
	def hasSpace(self):
		for recNum in range(0, self.bfr):
			aRecord = self.makeRecord(self.data[recNum*self.recordSize:(recNum+1)*self.recordSize])
			if aRecord.isEmpty():
				#print("New Rec num:" + str(recNum))
				return recNum
		return -1
	
	# only to be used inside this class
	# makes creating records easier and more clear
	def makeRecord(self, data):
		return Record(self.recordSize, self.fieldSize, self.strKeys, data)
		
	# return pointer value
	def getPointer(self):
		return int.from_bytes(self.data[(-1*self.pointerSize):], byteorder='big')
	
	# returns boolean value
	def isEmpty(self):
		for recNum in range(0, self.bfr):
			aRecord = self.makeRecord(self.data[recNum*self.recordSize:(recNum+1)*self.recordSize])
			if not aRecord.isEmpty():
				return False
		return True
	
	# returns an array of record objects
	def getAllRecords(self):
		records = []
		for recNum in range(0, self.bfr):
			aRecord = self.makeRecord(self.data[recNum*self.recordSize:(recNum+1)*self.recordSize])
			if not aRecord.isEmpty():
				records.append(aRecord)
		return records
	
	# returns a dictionary of locations and record objects
	def getAllRecordsWithLoc(self):
		records = {}
		i = 0
		for recNum in range(0, self.bfr):
			aRecord = self.makeRecord(self.data[recNum*self.recordSize:(recNum+1)*self.recordSize])
			if not aRecord.isEmpty():
				records[i] = aRecord
			i += 1
		return records
	
	def getAllRecordsInclDeleted(self):
		records = []
		for recNum in range(0, self.bfr):
			aRecord = self.makeRecord(self.data[recNum*self.recordSize:(recNum+1)*self.recordSize])
			if aRecord.getHashValueEvenIfDeleted():
				records.append(aRecord)
		return records
	
	# returns a record object 
	def getRecordWithValue(self, value):
		records = self.getAllRecords()
		for record in records:
			if record.getHashValue() == value:
				return record
	
	def getRecordWithValueLoc(self, value):
		records = self.getAllRecords()
		i = 0
		for record in records:
			if record.getHashValue() == value:
				return i
			else:
				i += 1
	
	def getRecordWithValueLocInclDeleted(self, value):
		records = self.getAllRecordsInclDeleted()
		i = 0
		for record in records:
			if record.getHashValueEvenIfDeleted() == value:
				return i
			else:
				i += 1
	
	def containsRecordWithValue(self, value):
		records = self.getAllRecords()
		for record in records:
			if record.getHashValue() == value:
				return True
		return False
	
	def containsRecordWithValueInclDeleted(self, value):
		records = self.getAllRecordsInclDeleted()
		for record in records:
			if record.getHashValueEvenIfDeleted() == value:
				return True
		return False