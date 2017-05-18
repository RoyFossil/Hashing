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
			return StaticlyHashedFile(blockSize, recordSize, fieldSize, fileSize, strKeys, path)

def initExtendible():
	while True:
		choice = input("Would you like to create a (N)ew file or use and (E)xisting one?  ")
		if choice == 'E' or choice == 'e':
			path = input("Enter the file location:  ")
			return ExtendibleHashedFile.fromExistingFile(path)
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
			return ExtendibleHashedFile(blockSize, recordSize, fieldSize, path, None)
		else:
			print("Please make a valid selection (N or E)")


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
		choice = input("")
		if choice == 'S' or choice == 's':
			return {"file": initStatic(), "type": "Static"}
		elif choice == 'E' or choice == 'e':
			return {"file": initExtendible(), "type": "Extendible"}
		elif choice == 'L' or choice == 'l':
			return {"file": initLinear(), "type": "Linear"}
		else:
			print("Please make a valid selection (S, E, or L)")

while True:
	fileAndType = chooseScheme()
	file = fileAndType["file"]
	menu(file, fileAndType["type"])













	