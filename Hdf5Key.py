

""" This class will take care of preparing and returing HDF dataset keys"""

import streamlit as st

class Hdf5key(object):

    def __init__(self, fund : str, yr : int) -> None:
        self.__user = st.session_state.user_id_rnd
        prevyr = int(yr) - 1
        nxtYr = int(yr) + 1

        self.fund = fund
        self.yr = str(yr)
        self.prevyr = str(prevyr)
        self.nextYr = str(nxtYr)
    
    @property
    def user(self) -> str :
        return self.__user

    def GetHoldingKey(self,fund : str , Yr : str, Ismanual:int = 1) -> str :

        if Ismanual == 0 :
            sKey = f"{self.user}" + '\\' + f"{fund}" + '\\' + f"{Yr}" + '\\' + "H"
        elif Ismanual == 1 :
            sKey = f"{fund}" + '\\' + f"{Yr}" + '\\' + "H"
        else :
            sKey = 'Invalid key'

        return sKey

    def GetTranKey(self,fund : str , Yr : str) -> str :
        
        sKey = f"{fund}" + '\\' + f"{Yr}" + '\\' + "T"
        return sKey    
        

    def GetCalcKey(self,fund : str , Yr : str) -> str :
        sKey = f"{self.user}" + '\\' + f"{fund}" + '\\' + f"{Yr}" + '\\' + "C"
        return sKey
