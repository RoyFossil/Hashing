class Record: 
	
	def __init__(self, size, fieldSize, strKeys, bytes):
		self.size = size
		self.fieldSize = fieldSize
		self.bytes = bytes
		self.strKeys = strKeys
		#print(bytes)
	
	@classmethod
	def new(cls, size, fieldSize, strKeys, value, record):
		if type(value) is int:
			bytes = value.to_bytes(fieldSize, byteorder='big') + b'\x00' + record.encode('UTF-8')
		elif type(value) is str:
			bytes = value.encode('UTF-8') + b'\x00'*(fieldSize-len(value)) + b'\x00' + record.encode('UTF-8')
		bytes = bytes + bytearray(size - len(bytes))
		return cls(size, fieldSize, strKeys, bytes)
	
	def sumChars(self, string):
		sum=0
		for c in string:
			sum+=ord(c)
		return sum
	
	#returns the hash value that was originally sent in
	#eg. will return a string or an int.
	def getHashValue(self):
		if not self.isDeleted():
			if self.strKeys:
				return (self.bytes[0:self.fieldSize]).rstrip(b'\x00').decode()
			else:
				return int.from_bytes(self.bytes[0:self.fieldSize], byteorder='big')
	
	def getHashValueInt(self):
		if not self.isDeleted():
			if self.strKeys:
				return self.sumChars(self.getHashValue())
			else:
				return int.from_bytes(self.bytes[0:self.fieldSize], byteorder='big')
	
	def getHashValueEvenIfDeleted(self):
		if self.strKeys:
			return self.sumChars(self.getHashValue())
		else:
			return int.from_bytes(self.bytes[0:self.fieldSize], byteorder='big')
		
	def isEmpty(self):
		return not self.getHashValueInt()
		
	def getData(self):
		return self.bytes[self.fieldSize + 1:]
	
	def prettyPrint(self):
		print("Key: " + str(self.getHashValue()) + " Data: " + self.getData().decode())
	
	def isDeleted(self):
		if(self.bytes[self.fieldSize:self.fieldSize+1] == b'\x01'):
			return True
		else:
			return False