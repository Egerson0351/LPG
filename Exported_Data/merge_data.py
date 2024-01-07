#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan  7 06:02:23 2024

@author: eitangerson
"""

import csv
import pandas as p

from pathlib import PurePath
import re
from pprint import pprint
import os
import sys
import chardet
from copy import deepcopy





class merge_data:
    def __init__(self, Input, Output = None, recurse = False):
        self.Input = PurePath(Input)
        self.Output = self._newOutput(Output)
        self.recurse = recurse
        self.file_dict = {}
        self.new_list = []

    def read(self):


        for root, folder, files in os.walk(self.Input):
            for file in files:
                if file.endswith("csv"):
                    try:
                        path = PurePath(root, file)
                        with open(str(path), "r") as my_file:
                            self.file_dict[path.name] = list(csv.reader(my_file))#p.read_csv(path, encoding="UTF-8").to_dict()
                    except Exception as e:
                        print(e)
            
            if not self.recurse:
                break
    
    def merge(self):
        linenum = 1
        self._check_headers()
        while self._check_exceeds(linenum):
            for ii in self.file_dict.keys():
                if linenum < len(self.file_dict[ii]):
                    item = self.file_dict[ii][linenum]
                    self.new_list.append(item)
            
            linenum += 1
    
    def write(self):
        with open(str(self.Output), "w") as f:
            writer = csv.writer(f, dialect='excel')
            writer.writerows(self.new_list)
            
            
            
    def _check_exceeds(self, linenum):
        
        return any([linenum < len(self.file_dict[ii]) for ii in self.file_dict.keys()])
    
    def _check_headers(self):
        
        lens = [len(self.file_dict[ii][0]) for ii in self.file_dict.keys()]
        if len(set(lens)) != 1:
            raise Exception(f"Inequal number of headers {lens}")
            
            
        item = 0
        while item < lens[0]:
            check_list = []
            for key in self.file_dict.keys():
                check_list.append(self.file_dict[key][0][item])
                
            if len(set(check_list)) != 1:
                raise Exception(f"Headers are not equivalent {check_list}")
            
            item += 1
            if item == lens[0]:
                break
            
        self.new_list.append(self.file_dict[key][0])
        
    
    def _newOutput(self, Output):
        if not bool(Output):
            Output = self.Input / "Merged_leads.csv"
        else:
            Output = PurePath(Output)
        
        return Output
            
            
def terminalRun():
    Inputs = sys.argv
    if len(Inputs) < 2:
        return False
    else:
        print(Inputs[1:])
        md= merge_data(*Inputs[1:])
        md.read()
        md.merge()
        md.write()
        return True
            
if __name__ == "__main__":
    
    if not terminalRun():
        md = merge_data("/Users/eitangerson/Desktop/LPG/Exported Data/trash")
        md.read()
        md.merge()
        md.write()
            