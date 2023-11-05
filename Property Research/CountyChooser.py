#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 27 09:50:59 2023

@author: rahelmizrahi
"""
import csv
import re
from decimal import Decimal, getcontext
import pprint
from numpy import diff
getcontext().prec = 3

HEADER_ROW=1
CURR_YEAR = 2022
class County:
    def __init__(self, name, State, yearsList:list, populationData:list):
        self.countyName = name
        self.percentChangeByYear = {}
        self.state = State
        self.populationData = {}
        self.numDataPoints = len(yearsList)
        self.showsGrowthTrend = None
        self.migrationFrom = None
        self.shows = None
        self.populate( yearsList,  populationData)
        self.populatePercentChange()
        self.updatePopulationChangeMetrics2()
    def populate(self, yearsList,  populationData):
        idx = 0
        for year in yearsList:
            self.populationData[year] = populationData[idx]
            idx += 1
            
    def populate2(self, rowInSpreadsheet):
        pass
    def populatePercentChange(self):
        years = list(self.populationData.keys())
        popSize = list(self.populationData.values())
        N = self.numDataPoints
        yearCurr = 0
        yearNext = 0
        for i in range(N):
            yearCurr = years[i]
            if i < N - 1:
                    yearNext = years[i+1]
                    percentChangeByYearKeyName = f"{str(yearCurr)} - {str(yearNext)}"
                    popCur = popSize[i]
                    popNext = popSize[i+1]
                    self.percentChangeByYear[percentChangeByYearKeyName]= self.calcPercentChange(popCur, popNext)
           
    def calcPercentChange(self, orig, new):
        x = ((new - orig) / orig) *100
        return round(x,2)
        

    
    def updatePopulationChangeMetrics2(self):
        percentChanges = list(self.percentChangeByYear.values())
        self.migrationFrom = any([ii < 0 for ii in  percentChanges])
        the_diff = list(diff(percentChanges))
        self.showsGrowthTrend = all([ii > 0 for ii in the_diff])
        
    
    def updatePopulationChangeMetrics(self):
        percentChanges = list(self.percentChangeByYear.values())
        numIters = len(percentChanges)
        assert(numIters == self.numDataPoints-1)
        
        #initialize the metric variables
        if percentChanges[0] < 0:
            self.migrationFrom = True
        else:
            self.migrationFrom = False
        
        if percentChanges[1] > percentChanges[0]:
            self.showsGrowthTrend = True
        else:
            self.showsGrowthTrend = False
        
        for i in range(1, numIters):
            if i < numIters - 1:
                if percentChanges[i] <= percentChanges[i+1]:
                    showsGrowthTrendTemp  = True
                else: 
                    showsGrowthTrendTemp = False
                self.showsGrowthTrend = self.showsGrowthTrend and showsGrowthTrendTemp
            
            if percentChanges[i] > 0:
                migrationFromTemp = False
            else:
                migrationFromTemp  = True
            
            self.migrationFrom = self.migrationFrom  and migrationFromTemp
       
   
   
        
'''
numPercentChangeYears: will compute %change in population, for the last 
numPercentChangeYears in population columns. 
so if you have population data for 10 years, and numPercentChangeYears is 3, then prgram 
computes %change in population for the 3 most recent years.
'''
class ParseData:
    
    def __init__(self, filePath, numPercentChangeYears=None):
        self.filePath = filePath
        self.columnNames=[]
        self.rawData = []
        self.populationByYear={} #{year: col number}
        self.percentChangeByYear={}
        self.counties = []
        if numPercentChangeYears is not None:
            self.numPercentChangeYears = numPercentChangeYears
        else:
            self.numPercentChangeYears = 2
        self.numCounties = 0
    def readData(self):
        with open(self.filePath, newline='') as f:
            reader = csv.reader(f)
            data = list(reader) #each line in the spreadheet is list, and data is a list of lists
        self.columnNames = data[HEADER_ROW]
        self.rawData = data[1:]
        self.numCounties = len(data) - HEADER_ROW - 1
    def updateByYearFields(self): #updates self.PopulationByYear and self.percentChangeByYear
        yearCurr = 0
        yearNext = 0
        for i in range(len(self.columnNames)):
            curListElement  = self.columnNames[i]
            if re.match('population', curListElement, re.IGNORECASE):
                yearCurr = int(re.split(',', curListElement)[1].strip()) #self.columnNames[i] is of the form "population, 2020"
                self.populationByYear[yearCurr] = i

    def convertToInt(self,stringNum):
            return int(re.sub(r',', '', stringNum))
    def updateCounties(self):
        # i is a row in the csv.
        #it is of the form: | county, state | pop2020 | pop2021 | pop2022 |
        for i in self.rawData[2:]: # is of the form county, state
    
                countyAndState = re.split(',\s*', i[0]) #now its a list
                if len(countyAndState) == 2:
                    countyName = countyAndState[0].strip()
                    state = countyAndState[1].strip()
                    populationData = i[1:] 
                    populationData = [self.convertToInt(i) for i in populationData]
                    list(self.populationByYear.keys())
                    self.counties.append( County(countyName, state, list(self.populationByYear.keys()), populationData))
           
    def getGoodCounties(self):
        goodCounties = []
        for c in self.counties:
            if c.populationData[CURR_YEAR] >= 30000:
                if (c.showsGrowthTrend == True) and ((c.migrationFrom)==False):
                    goodCounties.append(c)
            goodCounties.sort(key=lambda c: c.percentChangeByYear['2021 - 2022'], reverse=True)
            #orig_list.sort(key=lambda x: x.count, reverse=True)
    
        return goodCounties
   
def Print(Input):
    pprint.pprint(Input)
if __name__ == '__main__':
    fileName = "countyData.csv"
    parser = ParseData(fileName)
    parser.readData()
    parser.updateByYearFields()
    parser.updateCounties()
    
    goodCounties = parser.getGoodCounties()
    goodCounties[0].updatePopulationChangeMetrics()
    for i in range(0,10):
        print(f"{goodCounties[i].countyName},{goodCounties[i].state}")
        print(goodCounties[i].percentChangeByYear)
    fields = ['County Name', 'State', '% change (2020 - 2021)', '% change (2021- 2022)', 'distance from nearest metropolitan area', 'metropolitan area name', 'metropolitan area population', 'parcels of land sold in past 1 year', 'parcels of land sold in past 2 years']
    
    
   
    rows = zip([c.countyName for c in goodCounties],
               [c.state for c in goodCounties],
               [list(goodCounties[i].percentChangeByYear.values())[0] for i  in range(0,len(goodCounties))],
              [list(goodCounties[i].percentChangeByYear.values())[1] for i  in range(0,len(goodCounties))])
    with open('data.csv', 'w') as f:  
        writer = csv.writer(f)
        writer.writerow(fields)
        for row in rows:
            writer.writerow(row)
            
        
        
        
        
    