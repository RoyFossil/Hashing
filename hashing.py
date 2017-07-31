from LinearlyHashedFile import *
from StaticlyHashedFile import *
from ExtendibleHashedFile import *
import random

def initStatic():
	while True:
		choice = input("Would you like to create a (N)ew file or use and (E)xisting one?  ")
		if choice == 'E' or choice == 'e':
			path = input("Enter the file location:  ")
			return StaticlyHashedFile.fromExistingFile(path)
		elif choice == 'N' or choice == 'n':
			print("Please enter all sizes in bytes.")
			blockSize = int(input("Enter the block size:  "))
			recordSize = int(input("Enter the record size:  "))
			while recordSize > blockSize:	
				print("Record size must be smaller than block size")
				recordSize = int(input("Enter the record size:  "))
			fieldSize = int(input("Enter the size of the field that will be used for hashing: "))
			while fieldSize > recordSize:	
				print("Field size must be smaller than record size")
				fieldSize = int(input("Enter the field size:  "))
			fileSize = int(input("Enter the size of the file in number of blocks:  "))
			path = input("Enter the path and name of the file:  ")
			while True:
				choice = input("Will your hashing field be of type string? (Y/N) ")
				if choice == 'Y' or choice == 'y':
					strKeys = True
					break
				elif choice == 'N' or choice == 'n':
					strKeys = False
					break
				else:
					print("Please make a valid selection (Y or N)")
			return StaticlyHashedFile(blockSize, recordSize, fieldSize, fileSize, strKeys, None, path)

def initExtendible():
	while True:
		choice = input("Would you like to create a (N)ew file, use and (E)xisting one, or build from a text (F)ile?  ")
		if choice == 'E' or choice == 'e':
			path = input("Enter the file location:  ")
			return ExtendibleHashedFile.fromExistingFile(path)
		elif choice == 'N' or choice == 'n':
			print("Please enter all sizes in bytes.")
			blockSize = int(input("Enter the block size (note that 1 byte will be added for local depth storage):  "))
			recordSize = int(input("Enter the record size:  "))
			while recordSize > blockSize:	
				print("Record size must be smaller than block size")
				recordSize = int(input("Enter the record size:  "))
			fieldSize = int(input("Enter the size of the field that will be used for hashing: "))
			while fieldSize > recordSize:	
				print("Field size must be smaller than record size")
				fieldSize = int(input("Enter the field size:  "))
			path = input("Enter the path and name of the file:  ")
			while True:
				choice = input("Will your hashing field be of type string? (Y/N) ")
				if choice == 'Y' or choice == 'y':
					strKeys = True
					break
				elif choice == 'N' or choice == 'n':
					strKeys = False
					break
				else:
					print("Please make a valid selection (Y or N)")
			while True:
				choice = input("Will your hashing field be a key field? (Y/N) ")
				if choice == 'Y' or choice == 'y':
					nonKey = False
					break
				elif choice == 'N' or choice == 'n':
					nonKey = True
					break
				else:
					print("Please make a valid selection (Y or N)")
			return ExtendibleHashedFile(blockSize, recordSize, fieldSize, path, strKeys, nonKey, None)
		elif choice == 'F' or choice == 'f':
			print("The text file must be correctly formatted and include all information in the correct order.")
			fileLayout = input("Would you like to see an example of the file layout? (Y)es or any other input for no ")
			if fileLayout == 'Y' or fileLayout == 'y':
				displayTextFileLayout();
			path = input("Enter the location of the text file:  ")
			return createExtendibleFromTxt(path)
		else:
			print("Please make a valid selection (N, E, or F)")

def initLinear():
	while True:
		choice = input("Would you like to create a (N)ew file or use and (E)xisting one?  ")
		if choice == 'E' or choice == 'e':
			path = input("Enter the file location:  ")
			return LinearlyHashedFile.fromExistingFile(path)
		elif choice == 'N' or choice == 'n':
			print("Please enter all sizes in bytes.")
			blockSize = int(input("Enter the block size:  "))
			recordSize = int(input("Enter the record size:  "))
			while recordSize > blockSize:	
				print("Record size must be smaller than block size")
				recordSize = int(input("Enter the record size:  "))
			fieldSize = int(input("Enter the size of the field that will be used for hashing: "))
			while fieldSize > recordSize:	
				print("Field size must be smaller than record size")
				fieldSize = int(input("Enter the field size:  "))
			path = input("Enter the path and name of the file:  ")
			while True:
				choice = input("Will your hashing field be of type string? (Y/N) ")
				if choice == 'Y' or choice == 'y':
					strKeys = True
					break
				elif choice == 'N' or choice == 'n':
					strKeys = False
					break
				else:
					print("Please make a valid selection (Y or N)")
			return LinearlyHashedFile(blockSize, recordSize, fieldSize, path, strKeys, None)
		else:
			print("Please make a valid selection (N or E)")

def menu(file, type):
	while True:
		print("\n" + str(type) + " Hashing Menu: ")
		print("   1: Insert")
		print("   2: Search")
		print("   3: Update")
		print("   4: Delete")
		print("   5: Undelete")
		print("   6: Display")
		print("   7: Print statistics")
		print("   8: Quit")
		choice = input("")
		print("")
		if choice == '1':
			keyVal = input("Enter the key value for the record to be inserted:  ")
			data = input("Enter the data to be stored with that value: ")
			if not hasattr(file, "strKeys") or not file.strKeys:
				keyVal = int(keyVal)
			file.insert(keyVal, data)
		elif choice == '2':
			keyVal = input("Enter the key value for the record to be search:  ")
			if not hasattr(file, "strKeys") or not file.strKeys:
				keyVal = int(keyVal)
			file.search(keyVal)
		elif choice == '3':
			keyVal = input("Enter the key value for the record to be updated:  ")
			data = input("Enter the data to be stored with that value: ")
			if not hasattr(file, "strKeys") or not file.strKeys:
				keyVal = int(keyVal)
			file.update(keyVal, data)
		elif choice == '4':
			keyVal = input("Enter the key value for the record to be deleted:  ")
			if not hasattr(file, "strKeys") or not file.strKeys:
				keyVal = int(keyVal)
			file.delete(keyVal)
		elif choice == '5':
			keyVal = input("Enter the key value for the record to be undeleted:  ")
			if not hasattr(file, "strKeys") or not file.strKeys:
				keyVal = int(keyVal)
			file.undelete(keyVal)
		elif choice == '6':
			if type == "Extendible":
				extendibleDisplay(file)
			else:
				while True:
					withHeader = input("Would you like to display the file header? (Y/N) ")
					if withHeader == 'Y' or withHeader == 'y':
						withHeader = True
						break
					elif withHeader == 'N' or withHeader == 'n':
						withHeader = False
						break
					else:
						print("Please make a valid selection (Y or N)")
				file.display(withHeader)
		elif choice == '7':
			print("There are two options for printing statistics.")
			print("Print times will display the amount of time a function takes to execute.")
			print("Print workings will display information about navigating through the file.")
			print("It is not recommended to use both simultaneously as printing to the console adds time to the functions.")
			while True:
				times = input("Would you like to print times? (Y/N) ")
				if times == 'Y' or times == 'y':
					times = True
					break
				elif times == 'N' or times == 'n':
					times = False
					break
				else:
					print("Please make a valid selection (Y or N)")
			while True:
				workings = input("Would you like to print workings? (Y/N) ")
				if workings == 'Y' or workings == 'y':
					workings = True
					break
				elif workings == 'N' or workings == 'n':
					workings = False
					break
				else:
					print("Please make a valid selection (Y or N)")
			file.setStatistics(times, workings)
		elif choice == '8':
			break
		else:
			print("Please make a valid selection (1-8)")
	print("")

def chooseScheme():
	while True:
		print("Choose a hashing scheme:")
		print("   (S)tatic Hashing")
		print("   (E)xtendible Hashing")
		print("   (L)inear Hashing")
		print("    or (Q)uit")
		choice = input("")
		if choice == 'S' or choice == 's':
			return {"file": initStatic(), "type": "Static"}
		elif choice == 'E' or choice == 'e':
			return {"file": initExtendible(), "type": "Extendible"}
		elif choice == 'L' or choice == 'l':
			return {"file": initLinear(), "type": "Linear"}
		elif choice == 'Q' or choice == 'q':
			return
		else:
			print("Please make a valid selection (S, E, or L)")

def extendibleDisplay(file):
	while True:
		print("Choose a display option:")
		print("     1: One specific block")
		print("     2: Range of blocks")
		print("     3: Whole File")
		print("     4: Directory")
		choice = input("")
		if choice == '1':
			print("Input the block number to display.")
			print("Recall that blocks 0 and 1 are reserved for file metadata")
			blockNo = int(input(""))
			if blockNo == 0 or blockNo == 1:
				file.printFirstHeaderBlock()
			else:
				print("")
				file.displayBlock(blockNo)
			return
		elif choice == '2':
			print("Recall that blocks 0 and 1 are reserved for metadata")
			startBlockNo = int(input("Input the starting block number to display:  "))
			endBlockNo = int(input("Input the ending block number to display:  "))
			file.displayRange(startBlockNo, endBlockNo)
			return
		elif choice == '3':
			while True:
				withHeader = input("Would you like to display the file header? (Y/N) ")
				if withHeader == 'Y' or withHeader == 'y':
					withHeader = True
					break
				elif withHeader == 'N' or withHeader == 'n':
					withHeader = False
					break
				else:
					print("Please make a valid selection (Y or N)")
			file.display(withHeader)
			return
		elif choice == '4':
			file.printDirectory(true);
			return
		else:
			print("Please make a valid selection (1-4)")

def displayTextFileLayout():
	print("//Any lines starting with // are simply for describing the file layout and should be omitted.")
	print("//The first section of the file will contain the file metadata")
	print("//The data needed includes: Block size, record size, field size, string keys?, key field?, and file path")
	print("//These data values must come in that order.  An example is below")
	print("256")
	print("100")
	print("10")
	print("N")
	print("Y")
	print("C:/JO/test")
	print("//The next section of the file contains all of the record data")
	print("//The hashing field value will come first followed by the data to be stored with it")
	print("//The default separator is a single space")
	print("123 test123")
	print("234 test234")
	print("345 test345")

def validLoc(loc):
	return True

def createExtendibleFromTxt(datFile):
	with open(datFile) as f:
		content = f.read().splitlines()
		blockSize = int(content[0])
		recordSize = int(content[1])
		fieldSize = int(content[2])
		if content[3] == "Y" or content[3] == "y":
			strKeys = True
		else:
			strKeys = False
		if content[4] == "N" or content[4] == "n":
			nonKey = True
		else:
			nonKey = False
		filePath = content[5]
		file = ExtendibleHashedFile(blockSize, recordSize, fieldSize, filePath, strKeys, nonKey, None)
		for index in range(6, len(content)):
			arrayified = content[index].split(" ")
			hashingValue = arrayified.pop(0)
			if not strKeys:
				hashingValue = int(hashingValue)
			data = " ".join(arrayified)
			file.insert(hashingValue, data)
		return file

def makeFile():
	with open("C:/RF/wut.txt", 'w') as f:
		f.write("1024\n100\n10\nN\nY\n")
		for data in range(1, 10000):
			rand = random.randint(1, 1000)
			f.write(str(rand) + " test" + str(rand) +"\n")
		
while True:
	#makeFile()
	fileAndType = chooseScheme()
	if(fileAndType):
		file = fileAndType["file"]
		menu(file, fileAndType["type"])
	else:
		break













	
