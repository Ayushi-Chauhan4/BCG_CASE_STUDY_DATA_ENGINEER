# BCG_Case_Study_Car_Crash for DATA ENGINEERS

**Case Study:** Vehicle Accidents across US for brief amount of time.

**Dataset:** Data Set folder has 6 csv files. Please refer to the data dictionary to understand the dataset. 

**INSTALLATION**

Clone the repo and follow these steps:                 
1 - Extract the Data.zip file in `BCG_DE_CASE_STUDY` folder/directory                
2 - Go to the Project Directory: `$ cd BCG_DE_CASE_STUDY`                                 
3 - On terminal, run `python setup.py bdist_egg` to create `.egg` file. [RUN in terminal: pip install setuptools, pip install build - if not installed]             
4 - Run `spark-submit --master "local[*]" --py-files dist/CarCrashCaseStudy-0.0.1-py3.12.egg main.py` and get the output in Terminal as well as in Output Folder. [Edit your version of Python]    

The application solves below given Questions-

**Analytics:** 
1.	Analytics 1: Find the number of crashes (accidents) in which number of males killed are greater than 2?
2.	Analysis 2: How many two wheelers are booked for crashes? 
3.	Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
4.	Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run? 
5.	Analysis 5: Which state has highest number of accidents in which females are not involved? 
6.	Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
7.	Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
8.	Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
9.	Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
10.	Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)
                
