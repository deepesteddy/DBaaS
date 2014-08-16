#!python
import os
import math
import traceback
import sys
#import cPickle
import re

inp_dir = '/dev/in'

stdout = '/dev/stdout'

total = 0
#max_count = 0

node_name = os.environ['SCRIPT_NAME']

#reducer_count = int(os.environ['COUNT'])    #reducer count
#count = int(sys.argv[1])
#indexOfDash = node_name.index("-")  #locate index of '-', node ID follows
#reducerID = int(node_name[(indexOfDash + 1):])

     
input_file_names = []
fileHandleList = []
#a list of the value at the head of every channel
listOfChannelTupples = []
listOfKeys = []

key_col_index = int(os.environ['KEY_COL_INDEX'])

data = []

#DISCOVER LIST OF CHANNELS and OPEN A FILE HANDLE TO EACH
#returns populated input_file_names, fileHandleList
def openChannels(files, handles):
  
	for inp_file in os.listdir(inp_dir):
		fp = open(os.path.join(inp_dir, inp_file), 'rb', buffering=(2<<18) + 8)
		handles.append(fp)
		files.append(inp_file)
        
        
#INTIALIZE CHANNEL VALUES
def intialize(handles, tupples, keys):
	for fp in handles:
		#value = fp.readline()
		#tupples.append(value)
        #keys.append(value[key_col_index])
		sys.stderr.write('CHANNEL INITIALIZATION: ' + str(fp) + '\n')
        
		#tupple = cPickle.load(fp)
		tupple = fp.readline()
        
		#sys.stderr.write(', '.join(tuple) + '\n')
		sys.stderr.write("tupple: " + str(tupple))
        
		tupples.append(tupple)						#store tupple
		key =  re.split(',',tupple)[key_col_index]
		sys.stderr.write("key: " + key + '\n')
		sys.stderr.flush()
		#keys.append(tupple[key_col_index])			#store key
		keys.append(key)							#store key
        
        
#DEBUG
#list values in handles, tupples, keys for corresponding to the current state of the input channels
def channelState(handles, tupples, keys):
	sys.stderr.write('Channel State ************\n')
	i = 0
	for fp in handles:
		#sys.stderr.write('Channel' + str(fp) + ': ' + ', '.join(tupples[i]) + '\n')
		sys.stderr.write('Channel' + str(fp) + '\n')
		sys.stderr.write("tupple: " + str(tupples[i]))
		sys.stderr.write('key: ' + keys[i] + '\n')
		sys.stderr.flush()
		i += 1
        
    
#DEBUG
#list inp_files for this reducer/name node      
def listInputFiles(files):
	for file in files:
		sys.stderr.write(node_name + " connected to: " + file + '\n')
		sys.stderr.flush()
        

def keyCmp(x,y):
	result = 0
	if x.isdigit() and y.isdigit():
		number1=int(x)
		number2=int(y)
		result = cmp(number1,number2)
	else:
		result = cmp(x,y)  
      
	return result
  
  
class EmptyChannels(Exception):
	def __init__(self, value):
		self.value = value
	def __str__(self):
		return repr(self.value)


channelIndexLastMin = 0	#channel holding min	
#SEARCH ALL CHANNELS fpr TUPPLE w/ minimum KEY, READ ANOTHER VALUE INTO CHANNEL THAT HELD MIN KEY.
#returns tupple w/ minimum key
def joinChannelsOnKey(handles, tupples, keys):
 
  
  	#if tupples is empty, all input has been read
	if len(tupples) == 0 :
		sys.stderr.write("************Raised EmptyChannels Exception" + '\n')
		sys.stderr.flush()
		#sys.exit("**************Channels are empty***************")
		raise EmptyChannels("all input channels empty\n")

    
    
	global channelIndexLastMin
 	#locate the min and its index in the list of keys       
	min = keys[0]       #initialize min to first key		
	i = 0 				#loop current min index
	channel = 0			#loop counter
	for key in keys[1:]:
      
		channel += 1
      	
		if keyCmp(min,key) > 0:
			min = key
			i = channel								#remember which channel min value came from
		elif keyCmp(min,key) == 0:
			if  channelIndexLastMin  <> channel:   	#alternate channel when equal keys
				min = key
				i = channel							#remember which channel min value came from	   
                
     
	channelIndexLastMin = i   						#reset last index 

    #save tupple to return associated w/ min key
	min_tupple = tupples[i]
    
	joined = []
    #JOIN any other tupples with same key value with the ith tuple
	j = 0
	for key in keys:
		if (min == key) and (channelIndexLastMin <> j):
			joined.append(min_tupple + " : " + tupples[j])
		j += 1
    
           
      
	#now read in a new value into the channel that held the min, remember a channel's value is 
    #actually a tupple/row
	try:  
		tupples[i] = handles[i].readline()		#read from ith channel
		#tupples[i] = cPickle.load(handles[i])
        
		#sys.exit("getMinValue: step 3 completed")
        
		if not tupples[i]:				#this channel is exhausted
			sys.stderr.write("************* Channel EOF:  " + str(handles[i]) + " ***********************\n")
			handles[i].close()
			del handles[i]
			del tupples[i]
			del keys[i]
		else:					#update list of keys at index 
			keys[i] =  re.split(',',tupples[i])[key_col_index]
			#keys[i] = (tupples[i])[key_col_index]
            
		#sys.exit("getMinValue: step 4 completed")
        
	except: # catch *all* exceptions  including EOF
		exc_type, exc_value, exc_traceback = sys.exc_info()
		traceback.print_exception(exc_type, exc_value, exc_traceback)
		sys.exit("*****************ERROR LOADING TUPPLE or CLOSING CHANNEL*********")
        
	#minimum tupple    
	return	joined
 

  
#                        MAIN EXECUTION SEQUENCE AND LOOP
#get channels  
openChannels(input_file_names, fileHandleList)

listInputFiles(input_file_names)   #debug

#init
intialize(fileHandleList, listOfChannelTupples, listOfKeys)

#debug
channelState(fileHandleList, listOfChannelTupples, listOfKeys)


#read channels, extracting and writing tupple with min value to output
count = 0
flag = True

while flag:
  
  	
	try:
      
		joinList = joinChannelsOnKey(fileHandleList, listOfChannelTupples, listOfKeys)
        
		for joinedTupple in joinList:
			if ((count % 1000) == 0):
				sys.stderr.write(joinedTupple)
		
		#if ((count % 100) == 0) :
		#channelState(fileHandleList, listOfChannelTupples, listOfKeys)
		if len(joinList) > 0:  
			count += 1
	except EmptyChannels as e:
		sys.stderr.write (e.value)
		sys.stderr.flush()
		flag = False
	except: # catch *all* exceptions
		exc_type, exc_value, exc_traceback = sys.exc_info()
		traceback.print_exception(exc_type, exc_value, exc_traceback)
		flag = False
        
#end while      
	
total = count    
sys.stderr.write("********************Total tupples sequenced: " + str(count))
sys.stderr.flush()    


'''
for inp_file in os.listdir(inp_dir):
  
	index = inp_file.index("-")
	mapperID = int(inp_file[(index + 1): ])
    
	#debug
	#sys.stderr.write("reducerID: " + str(reducerID) + ", mapperID: " + str(mapperID) + '\n')
    
	remainder = mapperID % reducer_count           		
	# check if mapper is connected to this reducer               
	if ( (remainder == reducerID) or (remainder == 0 and reducerID == reducer_count)  ) :
        
		with open(os.path.join(inp_dir, inp_file), 'rb', -1) as fp:
			line_count = 0
        
			try:  
          
			#while True:
			#	line = fp.readline()
			#	if not line:
			#		break
				#count, filename = line.split()
              
				for line in fp:
					line_count += 1       # count the number of lines in each input file
					if line_count < 10:
						sys.stderr.write(str(line))
		
			except: # catch *all* exceptions
				exc_type, exc_value, exc_traceback = sys.exc_info()
				traceback.print_exception(exc_type, exc_value, exc_traceback)
			finally:
				fp.close()
				if line_count > max_count:
					max_count = line_count
				data.append((line_count, inp_file))
				total += line_count



for line_count, inp_file in data:
	print '%d  %s' % (line_count, inp_file)
    
'''    
    
print '%d  %s' % (total, 'total for: ' + node_name + '\n')
