from ExtendibleHashedFile import *

file = ExtendibleHashedFile(256, 100, 10, 'test1')

# while True:
	# hashValue=input('Enter a hash value: ')

	# file.insert(int(hashValue), "this is a record")
	

i=500
while i < 600:
	file.insert(i, str(i) + "test flippin value")
	print("just inserted")
	file.search(i)
	file.delete(i)
	print("just deleted")
	file.search(i)
	file.undelete(i)
	print("just undeleted")
	file.search(i)
	file.update(i, "new updated data")
	print("just updated")
	file.search(i)
	i+=500
#file.update(500, "updated val")
#file.search(500)
#file.displayBlock(2)
file.delete(500)
#file.search(500)
#file.undelete(500)
#file.search(500)
# for i in range(60, 70):
	# file.update(i, str(i) + "th UPDATED record")

# file.update(88, "i'm sure this wont break")


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
