class Record: 
	
	def __init__(self, size, recordSize, fieldSize, bfr, data):
		self.size = size
		# self.pointerSize = pointerSize
		# size of entire record (including hashing field)
		self.recordSize = recordSize
		self.fieldSize = fieldSize
		self.bfr = bfr
	
	def hasSpace(self):
		for recNum in range(0, self.bfr):
			aRecord = self.makeRecord(self.data[recNum*self.recordSize:(recNum+1)*self.recordSize])
		if aRecord.isEmpty():
				#print("New Rec num:" + str(recNum))
			return recNum
			return -1
		
	def makeRecord(self, data):
		return Record(self.recordSize, self.fieldSize, data)
		
		# returns an array of record objects
	def getAllRecords(self):
		records = []
	for recNum in range(0, self.bfr):
		aRecord = self.makeRecord(self.data[recNum*self.recordSize:(recNum+1)*self.recordSize])
	if not aRecord.isEmpty():
		records.append(aRecord)
		return records
	
	