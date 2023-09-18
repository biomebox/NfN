#!/usr/bin/bash

### script name is: consolidated_stats_script.sh

###### grab UTC/EST time for filenaming and timed automated script execution ######
timeFormatted=$(TZ="UTC" date +%F_%H-%M) 
currentMonth=$(TZ="UTC" date +%B)


###### making directories and organizing outputs based on each month and day ######
workingDirectory=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
outputDirectory=$workingDirectory/"everyday_updates"/"month_$currentMonth"/$timeFormatted
mkdir -p $outputDirectory


###### logging run times of this script in the logfile --> stats_script_run_log.out ######
printf "Current month: $currentMonth \tData collected at: $timeFormatted" | tee -a $workingDirectory/"stats_script_run_log.out"


###### run two python scripts for obtaining the NfN data each day ######
outFilename=$outputDirectory/"NfN_current_active_workflows.csv"
printf "\n$outFilename\n\n" | tee -a $workingDirectory/"stats_script_run_log.out"

# main script from PMason
python3 $workingDirectory/"org_workflow_stats_process_bar.py" -s $outFilename 
# secondary scripts from ChandraS
python3 $workingDirectory/"extracting_required_daily_info.py" $workingDirectory/"stats_script_run_log.out" $workingDirectory
everyExpFile=$workingDirectory/"everyday_expeditions_detailed.csv"
tempFile=$workingDirectory/"temp_everyday_expeditions_detailed.csv"
if test -f "$everyExpFile"; then
   cp $everyExpFile $tempFile
fi
python3 $workingDirectory/"for_second_graph.py" $workingDirectory $timeFormatted
if test -f "$tempFile"; then
   rm $tempFile
fi

###### run R script to obtain visualization of the NfN data each day ######
# R vizualization script from AveryG
# Rscript $workingDirectory/<<name of R script.R>> ?should supply name of output directory?


