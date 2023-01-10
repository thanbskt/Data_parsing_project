# -*- coding: utf-8 -*-



# the first function takes as input the  lines of the file in a list format and 
# the address of the word we located
def task1 (line_index,lines_fun):
    # we construct a list
    Query = []
    # knowing the schema of the data we exctract the 6 lines we need 
    # ingoring the lines we dont want so we use 
    # we do this adding 4 and 10 in the range function
    for i in range(line_index+4,line_index+10):
                    #we strip every line from spaces
                    text = lines_fun[i].strip()
                    # we split every line in symbol : and we get the second part to get
                    # rid of the 'INFO :' string, then we split again from the rifht one time 
                    # to get the values and keep the keys even if they are more than one word
                    one_go = [name.strip() for name in text.split(':')[1].rsplit(' ',1)]
                    # we append them to our list
                    Query.append(one_go)
    # the first element of each list  (Query is a list of lists) are the keys 
    # and the second are the values      
    keys =  [el[0] for el in Query]    
    values = [el[1] for el in Query]
    # we construct a dictionary knowing the kesy and the values with dict function
    Query_Execution_Summary = dict(zip(keys, values))
    # we return the dictionary
    return Query_Execution_Summary
#again we take in the second function the address line and the lines list
def task2 (line_index,lines_fun):
    Task =[]
    # we locate and we ectract the proper lines adding to the index number
    for i in range(line_index+2,line_index+8):
        # we strip and split the lines as we did in task1
        text = lines_fun[i].strip()
        text = [name.strip().replace(",", "") for name in text.split(':')[1].rsplit(None,5)]                
        Task.append(text)  
    # we delete the second element because its full of lines and its useless            
    del Task[1]       
    # we save the values
    values = [el[0] for el in Task]  
    # we create a list to save the data of keys in float format         
    data = []
    Task_Execution_Summary = {}
    for i in range(1,len(Task)):
        data.append([float(i) for i in Task[i][1:]])
        # we save construct the dictionary with the data from the two lists, data and Task
        Task_Execution_Summary [values[i]] = dict(zip(Task[0][1:],data[i-1])) 
        # we return the dictionary          
    return Task_Execution_Summary

# the third function
def task3 (line_i,lines_fun):
    #we construct the list and keep the address line
    i =line_i
    keys = []
    values = []
    #we get the first two lines and we split them like in the previous functions to get the data we want
    
    text = lines[line_i].split(' : ')[1] 
    text2 = lines[line_i+1].split(' : ')[1] 
    # we keep the data while we have with tabs
    # we make use of the nested format of the data
    # this is repeated untile we get a word without tab 
    while True:
        text = lines[i].split(' : ')[1] 
        text2 = lines[i+1].split(' : ')[1]
        spaces = len(text) - len(text.lstrip())
        spaces2 = len(text2) - len(text2.lstrip())
        i+=1
        keys.append(text.split(':')[0].strip())

        values.append(text.split(':')[1].strip())
        if spaces>spaces2:
            break
    # we delete the first value because its the header name and its empty     
    del values[0]
    # we transform data to int and we construct the nested dictionary
    for k in range(0,len(values)):
        values[k] = int(values[k])
    nested_dict = {}    
    nested_dict = dict(zip(keys[1:],values[1:]  )) 
    #we return the dictionary and the address that the function stoped 
    return  nested_dict,keys[0],i
        
# string to search in file
# we search for these specific words that our data starts and save them into variables
word1 = 'Query Execution Summary'
word2 = 'Task Execution Summary'
word3 = 'org.apache.tez.common.counters.DAGCounter'
#we open the data file
with open('beeline_consent_query_stderr.txt') as fp:
    # read all lines in a list from the file
    lines = fp.readlines()
    for line in lines:        
        # check if one of our words is in the file, we expect it to be there
        #when we find each word we save the adress line of the word
        if (line.find(word1) != -1):
            # we call the function task1 that takes as argument the index(address) 
            # of the first word and returns a dictionary of the data in the file
            # every task has different structure of data so we use functions for the tree 
            # different task
            task1_dict = task1(lines.index(line),lines)
        if line.find(word2) != -1 :
            # we search for the second word and call the second function that returns 
            # a nested dictionary with the data of the second task
              task2_dict = task2(lines.index(line),lines)
        if (line.find(word3) != -1):
            # we search for the third word to check his location and run one time function 3
            # and we create the first dictionary of the nested bigger dicionary
            
            task3_dict = {}
            tempdict,x,line_index = task3(lines.index(line),lines)
            task3_dict[x]    =  tempdict
            # after the extraction of the first dictionary we repeat this process
            # and we use as start address the index of the line in the previous dictionary
            # we repeat this 16 times as we know the number of the data blocks in order 
            # to construct the final nested dictionary
            for i in range(1,17):
                tempdict,x,line_index = task3(line_index, lines)
                task3_dict[x]    =  tempdict
            
  



# we use json library to proper print the data of the dictionaries
import json
#we print the dictionaries
print (word1,':\n',json.dumps(task1_dict, indent=2))
print (word2,':\n',json.dumps(task2_dict, indent=2))
# here the data have many headers and we simply print the dictionary
print (json.dumps(task3_dict, indent=2))


#saving the three files of data in seperate files on for every task
f  = open('task1_data.txt', 'wt')
f .write(str(task1_dict))
f .close()
f  = open('task2_data.txt', 'wt')
f .write(str(task2_dict))
f .close()
f  = open('task3_data.txt', 'wt')
f .write(str(task3_dict))
f .close()