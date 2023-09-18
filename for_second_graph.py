#!/usr/bin/env python3

# extracting the required rows and columns for classification calculations  

######### Importing required modules #########
import sys
import csv
import os
##############################################

##################################################################
##### function to extract the info out of regular files ##########
def eachFile(fileName):
    dictListCombo = {}
    #print("\nchecking function")
    readingFile = open (fileName, 'r')
    csvreader = csv.reader(readingFile, delimiter=',')

    for row in csvreader:
        try:
            projectId = row[0].strip(' ')
            workflowId = row[1].strip(' ')
            classCount = row[9].strip(' ')
            expName = row[-1].strip(' ')
            try:
                intProjectId = int(projectId)
                intWorkflowId = int(workflowId)
                comboId = projectId + "_"  + workflowId
                dictListCombo[comboId] = [classCount , expName]
            except:
                pass
        except:
            pass
    readingFile.close()
    return(dictListCombo)

##################################################################
### using the log file to obtain the last two instances of run ###
###### obtaining variable for comparison between two files #######
workingDirectory = sys.argv[1]
dateTime = sys.argv[2] #for new column

inputFilePath = os.path.join(workingDirectory, "stats_script_run_log.out") #hardcoded
bigOutFilePath = os.path.join(workingDirectory, "everyday_expeditions_detailed.csv") #hardcoded
checkFile = os.path.isfile(bigOutFilePath)

copyOfBigOutFilePath = os.path.join(workingDirectory, "temp.csv") #hardcoded


logFile = open (inputFilePath,'r')
readingLogFile = logFile.readlines()
latestFilePath = (readingLogFile[-2]).rstrip('\n')
lfDict = eachFile(latestFilePath) #calling function for latestfile
print("latest file: ", len(lfDict))
    
if checkFile == False:
    fileName = str(latestFilePath)
    ### setting up writefile ###
    bigOutFile = open(bigOutFilePath, 'w')
    simpleBigOutFileWriter = csv.writer(bigOutFile)
    simpleHeader =  ['Project_id', 'Workflow_id', 'Workflow_name', 'Totals']
    simpleBigOutFileWriter.writerow(simpleHeader)
    ### this is the first file of rolling totals ###
    ### so dictionary from the latest file is all we need ###
    for simpleKeys in lfDict:
        simpleIdCombo = simpleKeys.split("_")
        simpleRow = [simpleIdCombo[0], simpleIdCombo[-1], \
                        lfDict[simpleKeys][-1], lfDict[simpleKeys][0]]
        #print(simpleRow)
        simpleBigOutFileWriter.writerow(simpleRow)
    bigOutFile.close()

elif checkFile == True:
    previousFilePath = (readingLogFile[-5]).rstrip('\n')
    previousFile = str(previousFilePath)
    prDict = eachFile(previousFile) #calling function eachFile
    print("previousfile: ", len(prDict))
        
    ctr = 0
    new_ctr = 0
    newToWriteDict = {}
    ### newToWriteDict structure
    ### {idcombo: [value changed, latestValue, name]}

    for items in lfDict:
        if items in prDict:
            ctr += 1
            newValue = int(lfDict[items][0])
            oldValue = int(prDict[items][0])
            valueDiff = newValue - oldValue
            newToWriteDict[items] = [str(valueDiff), lfDict[items][0], lfDict[items][-1]]
            #if valueDiff > 0:
                #print(items, newValue, oldValue, valueDiff)
        elif items not in prDict:
            new_ctr += 1
            newValue =  int(lfDict[items][0])
            oldValue = 0
            valueDiff = newValue - oldValue
            newToWriteDict[items] = [str(valueDiff), lfDict[items][0], lfDict[items][-1]]
            print("ctr",ctr, "newly added", new_ctr, items)

    #bigOutFileDict = readBigCsvOutfile(bigOutFilePath)
    #consider last row as totals
    bigOutFileCopy = open(copyOfBigOutFilePath, 'r')
    bigOutFileCopyReader = csv.reader(bigOutFileCopy, delimiter=',')
    bigHeader = next(bigOutFileCopyReader) #used later
    csvLen = len(bigHeader) #used later
    print("big csv len", csvLen) 

    bigOutFileCopyIds = []

    for eachrow in bigOutFileCopyReader:
        try:
            proIdBigOutFileCopy = eachrow[0].strip(' ')
            workIdBigOutFileCopy = eachrow[1].strip(' ')
            try:
                intProjectId = int(proIdBigOutFileCopy)
                intWorkflowId = int(workIdBigOutFileCopy)
                comboId = proIdBigOutFileCopy + "_"  + workIdBigOutFileCopy
                bigOutFileCopyIds.append(comboId)
            except:
                pass
        except:
            pass
    bigOutFileCopy.close()
    print(len(bigOutFileCopyIds))

    #open new write file
    newBigOutFile = open(bigOutFilePath, 'w')
    newBigOutFileWriter = csv.writer(newBigOutFile)
    #write new header
    newHeader = bigHeader[:-1]
    newHeader.extend([str(dateTime), "Totals"])
    newBigOutFileWriter.writerow(newHeader)
    print("new big header", newHeader)

    for itr in newToWriteDict:
        buildNewRow = []
        if itr in bigOutFileCopyIds:
            with open(copyOfBigOutFilePath, 'r') as bigOutFileCopyAgain:
                bigOutFileCopyReaderAgain = csv.reader(bigOutFileCopyAgain, delimiter=',')
                next(bigOutFileCopyReaderAgain) #skip header
                for rowAgain in bigOutFileCopyReaderAgain:  
                    proIdAgain = rowAgain[0].strip(' ')
                    worIdAgain = rowAgain[1].strip(' ')
                    comboIdAgain = proIdAgain + '_' + worIdAgain
                    if itr == comboIdAgain:
                        buildNewRow = rowAgain[0:-1]
                        buildNewRow.extend([newToWriteDict[itr][0], newToWriteDict[itr][1]])
                        #print(buildNewRow)
                        newBigOutFileWriter.writerow(buildNewRow)
                        
        elif itr not in bigOutFileCopyIds:
            itrArray = itr.split("_")
            buildNewRow.extend([itrArray[0], itrArray[1], newToWriteDict[itr][-1]])
            noOfRunTimes = csvLen - 4
            if noOfRunTimes > 0:
                for noOfZeroes in range(0, noOfRunTimes):
                    buildNewRow.append('NA')
            buildNewRow.append(newToWriteDict[itr][1])
            print("new workflow added", buildNewRow)
            newBigOutFileWriter.writerow(buildNewRow)

    newBigOutFile.close()
logFile.close()

