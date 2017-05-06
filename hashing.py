from StaticlyHashedFile import *
file=StaticlyHashedFile(256,100,10,10,'C:/LN/test1')


# file.numOfBlocksare()
# file.insert(60, "test val holy shit holy shit holy shittttt")
 
for i in range(1,30):
	file.insert(int(i), "test val")		
	file.search(10)
	file.delete(10)
	file.search(10)
	file.undelete(10)










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
