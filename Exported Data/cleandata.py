# -*- coding: utf-8 -*-


import pandas as p
from pathlib import PurePath
import re
from pprint import pprint
import os
import sys
import chardet
from copy import deepcopy

def check_encoding(path):
    rawdata = open(path, 'rb').read()
    result = chardet.detect(rawdata)
    Print(result)

class CleanData_runner:
    def __init__(self, Input, Output = None, recurse = False):
        self.Input = PurePath(Input)
        self.Output = self._newOutput(Output)
        self.recurse = recurse

    def run(self):
        if os.path.isfile(self.Input):
            CD = cleanData2(self.Input, self.Output)
            CD.run()
        else:
            for root, folder, file in os.walk(self.Input):
                for ii in file:
                    if ii.endswith("csv"):
                        try:
                            path = PurePath(root, ii)
                            CD = cleanData2(path, self.Output)
                            CD.run()
                        except Exception as e:
                            print(e)
                
                if not self.recurse:
                    break
                
    
    def _newOutput(self, Output):
        if not bool(Output):
            if os.path.isfile(self.Input):
                Output = self.Input.parent.parent / "Cleaned"
            else:
                Output = self.Input.parent / "Cleaned"
        else:
            Output = PurePath(Output)
            
        if not os.path.exists(Output):
            os.mkdir(Output)
        
        if not os.path.exists(Output / "Dropped"):
            os.mkdir(Output / "Dropped")
        
        return Output
    

class cleanData2:
    def __init__(self, Input, Output = None):
        if isinstance(Input, PurePath):
            self.Input = Input
        else:
            self.Input = PurePath(Input)
        
        if isinstance(Output, PurePath):
            self.Output = Output
        else:
            self.Output = self._newOutput(Output)
            
        self.raw_data = None
        self.cleaned_data = {
            "Type" : [],
            "First Name": [],
            "Last Name": [],
            "2nd Owner’s First Name" : [],
            "2nd Owner’s Last Name" : [],
            "Company" : [],
            "Address" : [],
            "City" : [],
            "State" : [],
            "Zip" : [],
            "APN" : [],
            "Property Address" : [],
            "Property City" : [],
            "Property County": [],
            "Property State" : [],
            "Property Zip" : [],
            "Property Size" : [],
            "Zoning": [],
            "Short Legal Description": [],
            "Assessed Value" : [],
            "Back Taxes" : [],
            "__Full Name__" : [],
            "__Full_Situs__" : [],
            "__Full_Mail__" : [],
            
            }
        
        self.map = {
            "OWNER 1 FIRST NAME" : "First Name",
            "OWNER 1 LAST NAME" : "Last Name",
            "OWNER 2 FIRST NAME" : "2nd Owner’s First Name",
            "OWNER 2 LAST NAME" : "2nd Owner’s Last Name",
            "MAILING STREET ADDRESS" : "Address",
            "MAIL CITY" : "City",
            "MAIL STATE" : "State",
            "MAIL ZIP/ZIP+4" : "Zip",
            "APN - FORMATTED" : "APN",
            "SITUS STREET ADDRESS" : "Property Address",
            "SITUS CITY" : "Property City",
            "COUNTY" : "Property County",
            "SITUS STATE" : "Property State",
            "SITUS ZIP CODE": "Property Zip",
            "ZONING" : "Zoning",
            "LEGAL DESCRIPTION" : "Short Legal Description",
            "ASSESSED TOTAL VALUE" : "Assessed Value",
            "DELINQUENT TAX VALUE" : "Back Taxes",
            "LOT ACREAGE" : "Property Size"
            }
        self.must_haves = [
            "OWNER 1 FIRST NAME",
            "OWNER 1 LAST NAME",
            "MAILING STREET ADDRESS",
            "MAIL CITY",
            "MAIL STATE",
            "MAIL ZIP/ZIP+4",
            "APN - FORMATTED",
            "COUNTY",
            "SITUS STATE",
                      ]
        self.drop_log = deepcopy(self.cleaned_data)
        self.error_log = {}
        self.count = 0
        #print(self.Input)
    
    def run(self):
        #try:
        self.read()
        self.clean()
        self.render()
       # except Exception as e:
       #     self.error_log[self.Input] = e
            #Print(e)
        #self.render_log()
    
    def read(self):
        

        if str(self.Input).endswith("csv"):
            #check_encoding(self.Input)
            self.raw_data = p.read_csv(self.Input)
        else:
            raise Exception(f"Unknown data format >> {self.Input}")

    
    def clean(self):
        # self.raw_data.drop_duplicates(
        #     subset = ["OWNER 1 FULL NAME", "OWNER 2 FULL NAME", "OWNER 3 FULL NAME"],
        #     keep="first",
        #     inplace=True
        #     )
        for index, row in self.raw_data.iterrows(): 
            self._update_cleaned(row)
            
        self.cleaned_data = p.DataFrame.from_dict(self.cleaned_data)
        self.drop_log = p.DataFrame.from_dict(self.drop_log)
        self._remove_duplicates()
    
    def render(self):
        
        name = self.Input.stem + "_cleaned.csv"
        path = self.Output / name
        
        drop_path = self.Output /"Dropped"/ f"{self.Input.stem}_dropped.csv"

        
        self.cleaned_data.to_csv(str(path)) 
        self.drop_log.to_csv(str(drop_path))
    
    def _update_cleaned(self, row):
        self.count += 1
        print(self.count)
        
        if self._check_row(row):
        
            for row_name in self.map:
                
                value = str(row[row_name]).strip()
                if value.lower() == "nan":
                    value = ""
                    row[row_name] = value
                self.cleaned_data[self.map[row_name]].append(value)

            if bool(re.search("trust",row["OWNER 1 LAST NAME"], re.IGNORECASE)):
                self.cleaned_data["Type"].append("Company")
                self.cleaned_data["Company"].append(row["OWNER 1 LAST NAME"])
                self.cleaned_data["First Name"][-1] = row["OWNER 2 FIRST NAME"]
                self.cleaned_data["Last Name"][-1] = row["OWNER 2 LAST NAME"]
            
            else:
                self.cleaned_data["Type"].append("Individual")
                self.cleaned_data["Company"].append("")
            
            self.cleaned_data["__Full Name__"].append(f'{self.cleaned_data["First Name"][-1]} {self.cleaned_data["Last Name"][-1]}')
            self.cleaned_data["__Full_Situs__"].append(f'{self.cleaned_data["Property Address"][-1]}{self.cleaned_data["Property City"][-1]}{self.cleaned_data["Property County"][-1]}{self.cleaned_data["Property State"][-1]}{self.cleaned_data["Property Zip"][-1]}')
            self.cleaned_data["__Full_Mail__"].append(f'{self.cleaned_data["Address"][-1]}{self.cleaned_data["City"][-1]}{self.cleaned_data["State"][-1]}{self.cleaned_data["Zip"][-1]}')
            
        else:
            for row_name in self.map:
                value = str(row[row_name]).strip()
                if value.lower() == "nan":
                    value = ""
                    row[row_name] = value
                
                self.drop_log[self.map[row_name]].append(value)

            if bool(re.search("trust",row["OWNER 1 LAST NAME"], re.IGNORECASE)):
                self.drop_log["Type"].append("Individual")
                self.drop_log["Company"].append("")
            elif not bool(len(row["OWNER 1 LAST NAME"])):
                self.drop_log["Type"].append("UNKNOWN")
                self.drop_log["Company"].append("")
            else:
                self.drop_log["Type"].append("Company")
                self.drop_log["Company"].append(row["OWNER 1 LAST NAME"])
                self.drop_log["First Name"][-1] = row["OWNER 2 FIRST NAME"]
                self.drop_log["Last Name"][-1] = row["OWNER 2 LAST NAME"]
            
            self.drop_log["__Full Name__"].append(f'{self.drop_log["First Name"][-1]} {self.drop_log["Last Name"][-1]}')
            self.drop_log["__Full_Situs__"].append(f'{self.drop_log["Property Address"][-1]} \
                {self.drop_log["Property City"][-1]} {self.drop_log["Property County"][-1]} {self.drop_log["Property State"][-1]}\
                    {self.drop_log["Property Zip"][-1]}')
            self.drop_log["__Full_Mail__"].append(f'{self.drop_log["Address"][-1]} \
                {self.drop_log["City"][-1]} {self.drop_log["State"][-1]}\
                    {self.drop_log["Zip"][-1]}')
            
    
    def _check_row(self, row):
        found = []
        for ii in self.must_haves:
            value = str(row[ii]).strip()
            if value.lower() == "nan":
                value = ""
                row[ii] = value
            
            if not bool(len(row[ii])):
                found.append(False)
            else:
                found.append(True)
        
        if not any([found[0], found[1]]):
            return False
        elif not all(found[2:]):
            return False
        else:
            return True
    
    def _remove_duplicates(self):
        
        self.cleaned_data = self.cleaned_data.drop_duplicates(
            subset = ["__Full Name__", "__Full_Situs__", "__Full_Mail__", "APN" ],
            keep="first"
            )
        
        self.drop_log = self.drop_log.drop(columns = ["__Full Name__", "__Full_Situs__", "__Full_Mail__", "APN" ])
        
            
                
            
        
















class cleanData:
    
    def __init__(self, Input, Output = None):
        if isinstance(Input, PurePath):
            self.Input = Input
        else:
            self.Input = PurePath(Input)
        
        if isinstance(Output, PurePath):
            self.Output = Output
        else:
            self.Output = self._newOutput(Output)
            
        self.Data = None
        self.cleaned_data = None
        self.drop_log = []
        #print(self.Input)
    
    def run(self):
        self.read()
        self.clean()
        self.render()
        #self.render_log()
    
    def read(self):
        
        if str(self.Input).endswith("csv"):
            self.Data = p.read_csv(self.Input)
        
        elif str(self.Input).endswith("xlsx"):
            self.Data = p.read_excel(self.Input)
        else:
            raise Exception(f"Unknown data format >> {self.Input}")
        
        
    def clean(self):
        self.cleaned_data = self.Data.drop_duplicates(
            subset = ["OWNER 1 FULL NAME", "OWNER 2 FULL NAME", "OWNER 3 FULL NAME"],
            keep="first"
            ).copy("deep")
        drop_indexes = []
        for index, row in self.cleaned_data.iterrows():
            #print(row)
            situs = row["SITUS FULL ADDRESS"].strip()
            mail = row["MAILING FULL ADDRESS"].strip()
            name = row["OWNER MAILING NAME"].strip()
            APN = row["APN - FORMATTED"].strip()
            check_situs = re.search("^(?:\d+ )?.*, \w{2} \d+(?:-\d+)*$", situs)
            check_mail = re.search("^(\d+|PO\s+Box\s+\d+) .*, \w{2} \d+(?:-\d+)*$", mail,re.IGNORECASE)
            check_APN = len(APN)
            check_name_1 = re.search("\w+(?: \w+)+", row["OWNER 1 FULL NAME"])
            
            row["APN - FORMATTED"] = re.sub("\=", "", APN)
            
            if not all([check_situs, check_mail, check_APN, check_name_1]):
                self.drop_log.append(row)
                drop_indexes.append(index)
            else:
                row["APN - FORMATTED"] = re.sub("\=", "", APN)
                row["SITUS FULL ADDRESS"] = situs
                row["MAILING FULL ADDRESS"] = mail
                 
                
                
        self.cleaned_data.drop(index=drop_indexes, inplace = True)
    def format_name(self, inputName1, inputName2):
        inputName1List=re.split(" ", inputName1)
        
        isTrust = self.check_is_trust_name(inputName1List)
        
        if isTrust == True:
            return self._format_trust(inputName1,inputName2)
        else:
           
            numel = len(inputName1List)
            lastName = inputName1List.pop(0)
            firstName = inputName1List.pop(0)
            
            formattedName = firstName
            for i in range(len(inputName1List)):
                formattedName = formattedName + " " + inputName1List[i].strip() 
            formattedName += " " + lastName 
        
            return formattedName
    def check_is_trust_name(self, inputNameList):
        trustWords = ["trust", "tr"]
        tempNamesList = [t.lower() for t in inputNameList]
        for word in tempNamesList:
            if word == "trust" or word == "tr":
                return True
        return False
    
    # def _format_trust(self, inputName1, inputName2):
    #     inputName1List = re.split(" ", inputName1)
       
    #     if None != (inputName2) :
    #         inputName2List = re.split(" ", inputName2)
            
    #         if "tr" in [t.lower() for t in inputName1List]: #if OWNER 2 FULL NAME has the word Tr,                            
    #             return re.sub("Tr", '', inputName1, re.IGNORECASE) #if it is not, return an updated OWNER 1 FULL NAME
    #     else:
    #         indexOfFor = self._searchForTheWordFor(inputName2List)
    #         if indexOfFor == len(inputName2List) - 1:
    #             return "INVALID" #invalid
    #         else:
    #             inputName2List = inputName2List[indexOfFor+1:]
    #             formattedName=""
    #             for i in range(len(inputName2List)):
    #                 formattedName = formattedName + "" + inputName2List[i] + " "
                
    #             return f"Trustees of {formattedName}"
    #     else:
    #         return inputName1
    def _format_trust(self, inputName1, inputName2):
        inputName1List = re.split(" ", inputName1)
        if inputName2 is not None:
            inputName2List = re.split(" ", inputName2)
        
        if "tr" in [t.lower() for t in inputName1List]: #if OWNER 2 FULL NAME has the word Tr,     
            return re.sub("Tr", '', inputName1, re.IGNORECASE) #if it is not, return an updated OWNER 1 FULL NAME
        if "trust" in [t.lower() for t in inputName1List]:
            return f"Trustees of {inputName1}"
        
        
    
    def _searchForTheWordFor(self,inputNameList):
        
        
        for idx, word in enumerate(inputNameList):
            if word.lower() == "for":
                return idx
        return idx
    
    def render(self):
        
        name = self.Input.stem + "_cleaned.csv"
        path = self.Output / name
        drop_path = self.Output / f"{self.Input.stem}_dropped.csv"
        
        self.cleaned_data.to_csv("f{os.getcwd()}/Cleaned/{self.Input.stem}_cleaned.csv") 
        log = p.DataFrame(self.drop_log)
        log.to_csv(drop_path)
    
    def render_log(self):
        print("The Following rows were dropped:\n")
        for row in self.drop_log:
            Print(row)
            print("\n\n")
    
    def _newOutput(self, Output):
        if not bool(Output):
            Output = self.Input.parent
                
        else:
            Output = PurePath(Output)
        if not os.path.exists(Output):
            os.mkdir(Output)
        
        return Output
        
            


    
    





def Print(Input):
    pprint(Input)
    
def terminalRun():
    Inputs = sys.argv
    if len(Inputs) < 2:
        return False
    else:
        CD_runner = CleanData_runner(*Inputs[1:])
        CD_runner.run()
        return True
    
    
    
    
if __name__ == "__main__":
    print(__file__)
    if not terminalRun():
        cd = CleanData_runner("/Users/eitangerson/Desktop/LPG/Exported Data/Merged")
        cd.run()
        

        ### unit testing 
        # cd_ = cleanData(f"{os.getcwd()}/CO_Park_All_D.csv")
        # f = cd_.format_name("2130 ne 26th land trust", None)