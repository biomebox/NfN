#!/usr/bin/env python3

# extracting the required rows and columns for classification calculations  

######### Importing required modules #########
import sys
import csv
import deepdiff
import re
import os
##############################################

##################################################################
########## function to extract the info out of two files ##########
def eachFile(fileName):
    projectIDList = []
    workflowIDList = []
    idDict = {}
    #print("\nchecking function")
    readingFile = open (fileName, 'r')
    csvreader = csv.reader(readingFile, delimiter=',')

    for row in csvreader:
        try:
            projectId = row[0].strip(' ')
            workflowId = row[1].strip(' ')
            classCount = row[9].strip(' ')
            try:
                projectId = int(projectId)
                projectIDList.append(projectId)
                workflowId = int(workflowId)
                workflowIDList.append(workflowId)
                if projectId not in idDict:
                    idDict[projectId] = {workflowId: classCount}
                elif projectId in idDict:
                    idDict[projectId][workflowId] = classCount
            except:
                pass
        except:
            pass
    readingFile.close()
    return projectIDList,workflowIDList,idDict
##################################################################

##################################################################
### using the log file to obtain the last two instances of run ###
inputFile = sys.argv[1]
with open (inputFile,'r') as logFile: 
    readingLogFile = logFile.readlines()
    readTimeAndDate = (readingLogFile[-3]).rstrip('\n')
    latestFilePath = (readingLogFile[-2]).rstrip('\n')
    try:
        previousFilePath = (readingLogFile[-5]).rstrip('\n')
    except:
        sys.exit("exiting")

#print("\nlength", len(readingLogFile))
print("latest: ", latestFilePath)
print("previous: ", previousFilePath)
###################################################################

##################################################################
###### obtaining variable for comparison between two files #######
fileName = str(latestFilePath)
lfProjectIds, lfWorkflowIds, lfIdDict = eachFile(fileName) #calling function eachFile
print("latestfile: ", len(set(lfProjectIds)),len(lfWorkflowIds), len(lfIdDict))
fileName = str(previousFilePath)
prProjectIds, prWorkflowIds, prIdDict = eachFile(fileName) #calling function eachFile
print("previousfile: ", len(set(prProjectIds)),len(prWorkflowIds), len(prIdDict))

##################################################################
###################### comparing two files #######################

difference = deepdiff.DeepDiff(prIdDict, lfIdDict)

########## extracting results based on deepdiff output ###########
prSum = 0
lfSum = 0
dictNeeded = {}

if 'values_changed' in difference:
    for changedObj in difference['values_changed']:
        #print("changed", changedObj)
        array = re.findall(r'[0-9]+', changedObj)
        proId = array[0]
        worId = array[-1]
        #print("extracted", proId, worId)
        if proId not in dictNeeded:
            dictNeeded[proId] = [worId]
        elif proId in dictNeeded:
            dictNeeded[proId].append(worId)

        prSum = prSum + int(difference['values_changed'][changedObj]['old_value'])
        lfSum = lfSum + int(difference['values_changed'][changedObj]['new_value'])

if 'dictionary_item_added' in difference:
    for addedobj in difference['dictionary_item_added']:
        #print("\nadded", addedobj)
        addArray = re.findall(r'[0-9]+', addedobj)
        if len(addArray) == 2:
            addProId = addArray[0]
            addWorId = addArray[-1]
            #print("extracted", addProId, addWorId)
            dictNeeded[addProId] = [addWorId]
            lfSum = lfSum + int(lfIdDict[int(addProId)][int(addWorId)])
        elif len(addArray) == 1:
            addProId = int(addArray[0])
            addWorId = list(b[addProId].keys())
            #print("check", addWorId)
            if addProId not in dictNeeded:
                dictNeeded[addProId] = addWorId
                if len(addWorId) > 1:
                    for ra in addWorId:
                        #print("ra", ra)
                        lfSum = lfSum + int(lfIdDict[int(addProId)][int(ra)])
                elif len(addWorId) == 1:
                    lfSum = lfSum + int(lfIdDict[int(addProId)][int(addWorId[0])])
#print(dictNeeded)
        
numberOfTranscripts = lfSum - prSum
workingDirectory = sys.argv[2]
transcriptsDoneFilePath = os.path.join(workingDirectory, "everyday_transcripts_done.txt")
transcriptsDoneFile = open(transcriptsDoneFilePath, 'a+')
transcriptsDoneFile.write("%s\tNo. of transcripts: %d\n"\
                          %(readTimeAndDate,numberOfTranscripts))
transcriptsDoneFile.close()
##########################################################################

##########################################################################
################### writing a more compact csv file ######################
#print(dictNeeded)

latestFileOpenAgain = open (latestFilePath, 'r')

conciseFilePath = os.path.join(os.path.dirname(latestFilePath), 'NfN_concise_file.csv')
conciseFileWrite = open(conciseFilePath, 'w')

lfCsvReader = csv.reader(latestFileOpenAgain, delimiter=',')
lfCsvWriter = csv.writer(conciseFileWrite)

for itr in range(0,9):
    topRow = next(lfCsvReader)
    lfCsvWriter.writerow(topRow)

header = ['Project_id', 'Workflow_id', 'Active', 'Default', 'Created date',\
         'Finished date', 'Subjects', 'Retirement', 'Retired_subjects', \
            'Class_count', 'Stats_visible', 'Stats_type', 'Class_completion', \
                'Retired_completion', 'Workflow name']

lfCsvWriter.writerow(header)

for lfRow in lfCsvReader:
    try:
        projectIdAgain = lfRow[0].strip(' ')
        workflowIdAgain = lfRow[1].strip(' ')
        try:
            projectIdAgainInt = int(projectIdAgain)
            workflowIdAgainInt = int(workflowIdAgain)
            #print(projectIdAgain, workflowIdAgain)
            for comparisonItem in dictNeeded:
                if comparisonItem == projectIdAgain:
                    if workflowIdAgain in dictNeeded[comparisonItem]:
                        #print(comparisonItem, workflowIdAgain)
                        lfCsvWriter.writerow(lfRow)
        except:
            pass
    except:
        pass

for extraItr in (lfSum, prSum, numberOfTranscripts):
    lineToWrite = [' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', \
            extraItr, ' ', ' ', ' ', ' ', ' ']

    lfCsvWriter.writerow(lineToWrite)

latestFileOpenAgain.close()
conciseFileWrite.close()
