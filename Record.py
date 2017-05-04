class Record: 
	
	def __init__(self, size, fieldSize, bytes):
		self.size = size
		self.fieldSize = fieldSize
		self.bytes = bytes
		#print(bytes)
	
	@classmethod
	def new(cls, size, fieldSize, value, record):
		bytes = value.to_bytes(fieldSize, byteorder='big') + b'' + record.encode('UTF-8')
		return cls(size, fieldSize, bytes)
	
	def getHashValue(self):
		#print("Hash key: " + str(int.from_bytes(self.bytes[0:self.fieldSize], byteorder='big')))
		return int.from_bytes(self.bytes[0:self.fieldSize], byteorder='big')
		
	def isEmpty(self):
		return not self.getHashValue()
		
	def getData(self):
		return self.bytes[self.fieldSize:]