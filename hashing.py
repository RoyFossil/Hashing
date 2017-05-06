from StaticlyHashedFile import *
file=StaticlyHashedFile(256,100,10,10,'C:/LN/test1')



def staticMenu(file):
	return
	
# def initStatic():
	# while True:
		# if choice == 'E' or choice == 'e':
			# file=StaticlyHashedFile(256,100,10,10,'C:/LN/test1')
		# elif choice =='N' or choice == 'n':
			# print("Please enter all sizes in bytes.")
			# blockSize = int(input("Enter the block size:  "))
			# recordSize = int(input("Enter the record size:  "))
			# while recordSize > blockSize:	
				# print("Record size must be smaller than block size")
				# recordSize = int(input("Enter the record size:  "))
			# fieldSize = int(input("Enter the size of the field that will be used for hashing: "))
			# while fieldSize > recordSize:	
				# print("Field size must be smaller than record size")
				# fieldSize = int(input("Enter the field size:  "))
			# path = input("Enter the path and name of the file:  ")
			# while True:
				# choice = input("Will your hashing field be of type string? (Y/N) ")
				# print("Default NO -- N")
				# if choice == 'Y' or choice == 'y':
					# strKeys = True
					# break
				# elif choice == 'N' or choice == 'n':
					# strKeys = False
					# break
				# else:
					# print("Please make a valid selection (Y or N)")	
			# return StaticlyHashedFile(blockSize, recordSize, fieldSize, path, None)
		# else:
			# print("Please make a valid selection (N or E)")
			
def staticMenu(file):
	
	while True:
		print("Static Hashing Menu:")
		print("   1: Insert")
		print("   2: Search")
		print("   3: Update")
		print("   4: Delete")
		print("   5: Undelete")
		print("   6: Display")
		print("   7: Quit")
		if choice == '1':
			keyVal = input("Enter the key value for the record to be inserted:  ")
			data = input("Enter the data to be stored with that value: ")
			keyVal = int(keyVal)
			file.insert(keyVal, data)
		elif choice == '2':
			keyVal = input("Enter the key value for the record to be search:  ")
			keyVal = int(keyVal)
			file.search(keyVal)
		elif choice == '3':
			keyVal = input("Enter the key value for the record to be updated:  ")
			data = input("Enter the data to be stored with that value: ")
			keyVal = int(keyVal)
			file.update(keyVal, data)
		elif choice == '4':
			keyVal = input("Enter the key value for the record to be deleted:  ")
			file.delete(keyVal)
		elif choice == '5':
			keyVal = input("Enter the key value for the record to be undeleted:  ")
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
			break
		else:
			print("Please make a valid selection (1-7)")
	
		
# file.numOfBlocksare()
# file.insert(60, "test val holy shit holy shit holy shittttt")






















 
# for i in range(1,30):
	# file.insert(int(i), "test val")		
# file.search(10)
# file.delete(20)
# file.search(20)
# file.undelete(20)
# file.displayHeader()
# file.displayBlock(i)

# file.display(False)

# newFile = StaticlyHashedFile.fromExistingFile('C:/LN/test1')
# newFile.display(True)









# lst = [12,34,200,255,203]

# strfile = 'C://RF/test.txt'
# buffer = bytes(lst)

# print(buffer)

# with open(strfile,'bw') as f:
    # f.write(buffer)

# print('File written, reading it back')

# with open(strfile,'br') as f:
    # buffer = f.read(16)
    # print("Length of buffer is %d" % len(buffer))

    # for i in buffer:
        # print(int(i))
