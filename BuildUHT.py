'''
BuildUHT.py

Contains all required functions to build the UHT (Unit History Table) from scratch from the transaction log
Also contains required functions to append new days to the bottom of an existing Unit History Table

--- History ---
26/02 Major update:

All relevant functions combined into a single class. 

'''

import pandas as pd #Everything else from pandas requires a pd.x
import os #For iterating through folders and getting the current location of this script file
import numpy as np
import datetime as dt
import time

#Get the path of where this actual script file is (regardless of where it is being run from)
thisfilepath = os.path.dirname(os.path.realpath(__file__)) + '\\'
print thisfilepath

#Define all of the custom functions used in the script file using "def". 
#Note that these pieces of code do not actually run when they are being "defined" -
#they are is simply convenient "buckets" to store individual pieces of code, 
#to get a defined output from a defined input. Without these functions, the code would
#be much less readable and very difficult to test.

def loadtransactionlog(inputpath, successonly = True, renamedict = {}, 
	droplist = ['Web Login','E-payment Reference','E-Payment Provider',
    'E-Payment Amount','Error Detail','Outgoing Message',
    'Unit Check Digit','Gateway Number',
    'Scratch- card Serial Number','Scratch-card PIN',
    'Timestamp','Unit Id']):
	'''
	loadtransactionlog(inputpath, successonly = True, droplist = [...], renamedict = {})
	Loads the transaction log by iterating through every csv file in inputpath
	Concatenates everything into single dataframe, indexed by timestamp
	Performs a basic cleanup of the data:
	- Remove duplicates
	- Clean up unit IDs
	- Remove unsuccessful transactions
	- Drop irrelevant columns
	
	Returns transactionlog dataframe

	---OPTIONS---
	successonly: If set to False, all unsuccessful transactions also also returned
	droplist: User can pass a different list of columns to drop, or [] if they do not want to drop any columns
	renamedict: User can pass a dictionary to rename the columns to something sensible
	'''

	#Load the transaction log, which has been downloaded by months from the server
	firstfile=True
	for filename in os.listdir(inputpath + 'ScratchcardTransactions'):
		if filename.endswith('.csv'): #Ignore random hidden files which can appear
			newtrans = pd.read_csv(inputpath + 'ScratchcardTransactions/' + filename,low_memory=False)
			if firstfile:
				translog=newtrans
				firstfile=False #Do concat for future files
			else:
				translog = pd.concat([translog, newtrans])

	translog.drop_duplicates(inplace=True)

	#For some reason, Unit ID sometimes gets a trailing .0. Strip this
	#Change the name at the same time (ID is Id in the new server for transactions only...)
	translog['Unit ID']=translog['Unit Id'].astype(str).apply(lambda x: x[:-2] if x[-2:] == '.0' else x)

	#Convert the timestamp, which is currently a string, to a datetime object
	#Make this faster: provide the format of the timestamp strings
	translog['timestamp']=pd.to_datetime(translog['Timestamp'])

	#Drop the irrelevant columns from the dataframe
	#Id is the old column
	#Removing Timestamp because we now have topupdate
	if successonly == True:
		#Remove all of the transactions that were not successful
		translog=translog[translog['Success']==1]
		droplist.append('Success')
		
	for dropcol in droplist:
		translog.drop(dropcol,axis=1,inplace=True)

	if renamedict:  #Empty dictionaries evaluate to false in Python
		#Rename each of the columns per renamedict
		translog.rename(columns=renamedict, inplace=True)
		
	#Set the index to timestamp (for resampling etc)
	translog.set_index('timestamp',inplace=True)
		
	return translog

def renamecolumns(translog, filepath):
	'''
	The names of the columns are very non standardised. It can make it extremely difficult 
	to remember what column name you want when coding because there are sometimes unnecessary 
	prefixes (eg "customer"), or non-capitalised names (Customer id).
	The following table standardises these names, removes spaces, etc.
	'''
	#Rename the columns according to the spreadsheet
	#How do I find the path that this particular file is stored in?
	replacedict = pd.read_excel(filepath + 'Operating Column Names.xlsx', sheetname='translog').set_index('As_loaded').to_dict()['Operating']
	return translog.rename(columns = replacedict)

def topupvalues(translog, filepath):
    '''
    topupvalues(translog)
    Create a new column for the value of the top up. This is stored as another sheet in Operating Column Names
    3 = 1 week
    4 = 4 weeks
    (5 = unlock, but I haven't put this as a value)
    '''
    valuedict = pd.read_excel(filepath + 'Operating Column Names.xlsx', sheetname='topupvalues').set_index('functioncode').to_dict()['value']
	
    #Every row where the function is no in this 
    translog['topupvalue']=None
    for key in valuedict:
    	translog.loc[translog['function_code']==key, 'topupvalue']=valuedict[key]

    return translog

def unitdailystatus(unittrans, startdate, enddate, installdate = "", includetopupvalues=False):
    '''
    unitdailystatus(unittrans, startdate, enddate, includetopupvalues=False)
    This function is applied to a single unit (hence unittrans)
    Returns a single-column dataframe indexed by date from startdate to enddate, with unitstatus for each day

    ---HOW---
    The dailyhistory dataframe is created by resampling unittrans to daily, hence recording number of transactions each day
    dailyhistory then gets a new column, called "unitstatus"
    unitstatus is the number of days credit remaining (+) or out of credit (-)

    18/02/16: Modified to record number of days credit remaining
    '''

    import datetime as dt

    #Resample the history to a sum of daily top up values (in case multiple top ups occurred on one day),
    #hence rename the column to dailytopuptotal
    #Set all values that are left as "NaN" (that is, days with no top up) to zero for calculations
    unittrans.topupvalue = unittrans.topupvalue.astype(float)
    dailyhistory = pd.DataFrame(unittrans.topupvalue.resample('d',how='sum'))
    dailyhistory.rename(columns = {'topupvalue':'dailytopuptotal'},inplace=True)
    dailyhistory.loc[dailyhistory.dailytopuptotal.isnull(),'dailytopuptotal'] = 0

    #If no install date specified, install date is taken as the first date with a transaction
    if installdate == "":
    	installdate = dailyhistory.index.min()
    #---SELENE--- 
    #How are we going to handle the strange situations where the recorded install date does not match the first top up date?

    #Resize the dataframe to all dates of interest by creating a blank data frame of the correct size and joining
    #This doesn't seem very efficient but I can't find a more reliable way to do it!
    index = pd.date_range(start = startdate, end = enddate, freq='d')
    dailyhistory = pd.DataFrame(index = index).join(dailyhistory, how = 'left')
    #Set the top-up values at the top and bottom of the dataframe to zero (so the maths works correctly)
    dailyhistory.loc[dailyhistory.dailytopuptotal.isnull(),'dailytopuptotal'] = 0

    #dailyhistory gets a new column now, called "unitstatus". 
    #This is the number of days credit remaining (+) or out of credit (-)
    #Set all days before the installdate to be value 'S' (stock)
    #Note the use of the .loc as recommended in the docs (rather than a slicer)
    dailyhistory.loc[dailyhistory.index < installdate,'unitstatus'] = 'S'

	#Determine the unlockdate, if it exists. Take the first unlock date (just in case there is more than one)
    if len(unittrans[unittrans.function_code == 5]) > 0:
        unlockdate = unittrans[unittrans.function_code == 5].index[0]
        unlockdate = dt.datetime(*unlockdate.timetuple()[:3])
        dailyhistory.loc[dailyhistory.index >= unlockdate,'unitstatus'] = 'U'
    else:
        #Set it a long time into the future so that all remaining rows are iterated over
        unlockdate = enddate + dt.timedelta(days = 365) 
	
    #Iterate through all days after the install date, keeping track of
    #the number of days that the customer is in credit. Start with zero credit assumed:
    dayscreditremaining = 0 #Counter
    continuousdaysooc = 0 	#Counter
    
    #This line is an iterator - it iterates through each row in dailyhistory (that is, each day, because of the 
    #resizing that we completed earlier). index is then used to 
    for index, row in dailyhistory[(dailyhistory.index >= installdate) & (dailyhistory.index < unlockdate)].iterrows():
        #Add any credit that was purchased by the customer today to the current credit total
        #Note that the dailytopuptotal column is measured in weeks of credit, hence * 7
        dayscreditremaining += row.dailytopuptotal * 7
        #Record today's amount of credit remaining in the dailyhistory table, then reduce dayscreditremaining by 1
        if dayscreditremaining > 0:
        	#The set_value method is ~100x faster than a .loc; useful as we are iterating through so many rows
            dailyhistory.set_value(index, 'unitstatus', dayscreditremaining)
            #Edge-case: If the current value is 1, this will stay at 0 until the unit is next topped up
            #so in future iterations, the else statement below will prevail
            dayscreditremaining += -1 
            #Ensure that the out of credit counter is reset, in case the unit just came back into credit
            continuousdaysooc = 0 
        else: #There is no credit remaining (ie zero or less)
        	# Record the number of days since the unit was last in credit and increase the number of days out of credit
			dailyhistory.set_value(index, 'unitstatus', -continuousdaysooc) #Note this could be zero also
			continuousdaysooc+=1

    #Now overwrite all the days after unlock, if the unit has been unlocked
    #I have had to rely on the first recorded successful unlock (some have multiple)
    #---ASSUMPTION---
    unlockdate = unittrans[unittrans.function_code==5].index.min()
    if unlockdate is not np.nan: #unit has been unlocked
        dailyhistory.loc[dailyhistory.index >= unlockdate,'unitstatus'] = 'U'

    if includetopupvalues == False: #Only return the unitstatus column, to save memory
        dailyhistory.drop('dailytopuptotal',1,inplace=True)

    return dailyhistory #So this is the complete history for this unit, from startdate to enddate.

def dayofdatetime(datetimeobject, nextday = False):
	'''
	Converts a datetime object into a new datetime object that is just the day portion (ie. midnight)
	This is different from datetime.date() because this returns a date object (not great for Pandas)
	if nextday is set to True, it returns the datetime at midnight on the following day
	The reason that we want all this is because we only want the unit history table to be operating in whole days
	'''
    
	import datetime as dt
	dayonly = dt.datetime(*datetimeobject.timetuple()[:3])
	if nextday:
		return dayonly + dt.timedelta(days=1)
	else:
		return dayonly
	

#Now run the actual script 

pd.options.mode.chained_assignment = None  #disable "chainedwithcopywarning": default='warn'

translog = loadtransactionlog(thisfilepath) #Note that it will look in the subfolder ScratchcardTransactions

translog = renamecolumns(translog, thisfilepath)

translog = topupvalues(translog, thisfilepath)

#You must always start with the start of the translog to get a reliable result.
#And you would generally run to the end of the transaction log, unless there was a really good reason not to...
startdate = translog.index.min()
startdate = dt.datetime(*startdate.timetuple()[:3]) #Convert timestamp to the actual date of the first day (midnight)
enddate = translog.index.max()
enddate = dt.datetime(*enddate.timetuple()[:3]) + dt.timedelta(days=1) #Convert timestamp to the next day (midnight)

#----ASSUMPTION---
#Create empty dataframe with index of all of the dates from startdate to enddate
unithistorytable = pd.DataFrame(index = pd.date_range(start = startdate, end = enddate, freq='D'))

#Create counter variable and determine total number of units for reporting on progress while running
unitscomplete = 0
totalunits = len(translog['unit_id'].unique())

starttime = time.time() #This is the start of the iteration which takes up the majority of the time for this function

#Group the transaction log by unit_id (a big operation - so only do the groupby once) and loop through each unit found
for unitID, unittrans in translog.groupby('unit_id'): #A very convenient pandas operator!
	#If there are no transactions, we assume the unit was never installed
	#and hence don't bother to put it in the unithistorytable
	if len(unittrans) > 0:
		#Calculate the unit history, then merge the result into the master with column name of the unit_id
		unithist = unitdailystatus(unittrans,startdate,enddate)
		unithistorytable = unithistorytable.join(unithist.rename(columns={'unitstatus':unitID}), how = 'left')
	
	#Record progress, and every hundredth unit, print out progress for the user
	unitscomplete += 1
	if unitscomplete%100 == 0:
		print 'Completed %r of %r units in %.1f seconds. Estimate %.1f minutes remaining'%(unitscomplete, totalunits,
			time.time() - starttime, (totalunits / unitscomplete - 1) * (time.time() - starttime) / 60)
			
print 'unithistorytable reading for viewing'