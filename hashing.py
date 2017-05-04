from LinearlyHashedFile import *

file = LinearlyHashedFile(256, 100, 10, 'C:/RF/test1', True)

# hashValue = input("Enter a hash value: ")
# recordValue = input("Enter a string to store with that hash value: ")

# file.insert(int(hashValue), recordValue)

for i in range(1,10000):
	file.insert("test" +str(i), "test val")
	file.search(str(i))



# for i in range(50, 500):
	# print("n: " + str(file.n) + "   m: " + str(file.m))
	# file.insert(i, str(i) + "th record3333")


# for i in range(60, 70):
	# file.search(i)
	# file.delete(i)
	# file.search(i)
	# file.undelete(i)
	# file.search(i)
	# print(" ")

	
# file.display(False)


# lst = [12,34,200,255]

# strfile = 'C:/RF/test.txt'
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
