import json
import streamlit as st



class Mapping(object) :
    
    def __init__(self , mapinfo : dict) -> None:
        self.data = mapinfo

    def getkeybyValueMap(self) :

        def ModifyColumns(item : str ) -> str:
            item = item.replace(' ','')
            item = item.replace('/','')
            item = item.replace('&','')
            item = item.replace('(','')
            item = item.replace(')','')
            item = item.replace('%','')
            item = item.replace(f"'",'')
            item = item.replace('+','')
            
            return item
            

        def modifykeyvalue(x):

            return ModifyColumns(x[0]) , ModifyColumns(x[1]) 
        
        dic =  dict(map(modifykeyvalue , self.data.items() ))
        
        query = ', '.join(['"{0}"'.format(str(value).strip()) + ' AS ' + '"{0}"'.format(str(key).strip()) for key, value in dic.items()])
        
        return query

    def getListKeyValue(self) -> list :
        
        return [str(self.data[k]).strip() for k in self.data]

    def IsvalidStruct(self, source_col_list_o : list) -> bool :

        def dostrip(listelement : str) :
            return listelement.strip()
        
        s1_source_in = list(map(dostrip , source_col_list_o))
        s1_source_map = list(map(dostrip , [str(self.data[k]).strip() for k in self.data]))

        s1_source_map.sort()
        s1_source_in.sort()
        
        return s1_source_map == s1_source_in

    def getval(self, key : str) -> str :
        return self.data.get(key)