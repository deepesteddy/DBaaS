#!python
# -*- coding: utf-8 -*-
import os
import csv
import operator
import sys
import traceback
import cPickle
import io

#THIS VERSION
#READING PARAMETER INTO PYTHON SCRIPT FROM JSON FILE USING:  
#	
#		Python fragment
#		os.environ['someParameter']
#
#
#		JSON fragment
#		"exec": {
#            "path": "file://python:python",
#			 "env": {
#				"someParameter": 4
#			  }
#        }
#
#
#ALSO CONSTRUCT WHICH SEQUENCER TO CONNECT TO USING:	os.environ['SCRIPT_NAME']
#AND A MODULO AND STRING FUNCTIONS TO FORM THE PATH TO A SPECIFIC SEQUENCER USING THE CONNECTION IDS
#THAT ARE GUARANTEED TO BE PROVIDED BY THE ZEROVM NETWORK LAYER. SEE THE JSON SERVLET DOC

key_col_index = int(os.environ['KEY_COL_INDEX'])

def csv_to_list(csv_file, delimiter=','):
	""" 
	Reads in a CSV file and returns the contents as list,
	where every row is stored as a sublist, and each element
	in the sublist represents 1 cell in the table.
  
	"""
	#sys.stderr.write(csv_file)
	with open(csv_file, 'rb', buffering=(2<<18) + 8) as csv_con:
		reader = csv.reader(csv_con, delimiter=delimiter)
		csv_list = list(reader)
		csv_con.close()
		return csv_list
      
	#cmp=lambda x,y: cmp(x.lower(), y.lower())
    
def sortCmp(x,y): 
	if x.isdigit() and y.isdigit():
		x=str(x)
		y=str(y)
	return cmp(x,y)
      
def sort_by_column(csv_cont, col, reverse=False):
	""" 
	Sorts CSV contents by column name (if col argument is type <str>) 
	or column index (if col argument is type <int>). 
    
	"""
	header = csv_cont[0]
	body = csv_cont[1:]

	if isinstance(col, str):  
		col_index = header.index(col)
	else:
		col_index = col
    
	#MUCH SLOWER, but does not need:  import operator, not supported in ZeroVM Cloud
	#body = sorted(body, key =  lambda x :  x[col_index], reverse=reverse)
    #cmp=lambda x,y: cmp(x.lower(), y.lower())
	body = sorted(body, cmp=lambda x,y: sortCmp(x,y), key=operator.itemgetter(col_index), reverse=reverse)
	#body.insert(0, header)  #don't put back CSV header in sorted list, we are done with CSV file writes

	return body
  
#create the list from the .csv file, a list of lists, where every line is a list of csv formatted fields         
csv_list = csv_to_list('/dev/input', ",")

#sort the list by the column key
csv_list = sort_by_column(csv_list, key_col_index, reverse=False)  # list is sorted

 
    
#output to which sequencer?   get this node's name, strip off its ID number, i.e. for mapper-7 strip off 7
#then use node's ID number with modulo operator to decide which sequencer to connect to. Get COUNT in JSON file
#for number of sequencers. For example if "sequence_count":4  then   ID % 4 .  If in this example, ID is 7 then this
#node should be connected to 7 % 4 = 3 ,   sort_R-7  ---> sequence-3 
sequence = ""
sequence_count = int(os.environ['SEQ_COUNT'])
node_name = os.environ['SCRIPT_NAME']

if sequence_count > 1:
	indexOfDash = node_name.index("-")  #locate index of '-', node ID follows
	if (int(node_name[(indexOfDash + 1):]) % sequence_count) == 0:
		sequence =  "/dev/out/sequence_R-" + str(sequence_count)
	else:
		sequence = "/dev/out/sequence_R-" + str(int(node_name[(indexOfDash + 1):]) % sequence_count)
else:
  	sequence = "/dev/out/sequence_R"
    
sys.stderr.write("node_name is: " + node_name + ", connected to sequence: " + sequence + '\n')
sys.stderr.flush();
#print >> sys.stderr, node_name

with open(sequence, 'ab', buffering=(2<<18) + 8) as fp:
	try:  
		line_count = 0
		for row in csv_list:
			fp.write(str(row) + '\n')
			#sys.stderr.write(str(row) + '\n')
			#sys.stderr.flush()
			#cPickle.dump(row,fp)
			#line_count += 1
            
			#if line_count > 1:
			#	raise Exception("DEBUG")
            
			#if line_count < 10:
			#	sys.stderr.write(str(row) + '\n')
				#sys.stderr.write(', '.join(row) + '\n')
			#	sys.stderr.flush()
                
		sys.stderr.write("*********************EOF***************************")
		sys.stderr.flush()
			#print >> fp, '%s' % (line) 
	except: # catch *all* exceptions
		exc_type, exc_value, exc_traceback = sys.exc_info()
		traceback.print_exception(exc_type, exc_value, exc_traceback)
	finally:
		fp.flush()
		fp.close()
        
     
		#e = sys.exc_info()[0]
		#sys.stderr.write(str(e))
		#print >> sys.stderr, '%s' % (e)
    
    # Split off the swift prefix
    # Just show the container/file
	#path_info = os.environ['PATH_INFO']
	#shorter = '/'.join(path_info.split('/')[2:])
    # Pipe the output to the reducer:
	#print >>fp, '%d %s' % (len(csv_list), shorter)
