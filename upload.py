import streamlit as st
import pandas as pd
import duckdb as db
import numpy as np
import h5py 
import tables as pys
import HDFOperation as hd
from Hdf5Key  import Hdf5key
from mapping import Mapping
import json


class Upload(object):
     
    def __init__(self) -> None:
          HDFFile = "data\\stock_1.h5"
          self.__HDFFile = HDFFile  

    

    @property
    def getHDFFile(self) :
         return self.__HDFFile
    

    def __ModifyColumns(self,df : pd.DataFrame ) -> pd.DataFrame :
            df.columns = df.columns.str.replace(' ','')
            df.columns = df.columns.str.replace('/','')
            df.columns = df.columns.str.replace('&','')
            df.columns = df.columns.str.replace('(','')
            df.columns = df.columns.str.replace(')','')
            df.columns = df.columns.str.replace('%','')
            df.columns = df.columns.str.replace(f"'",'')
            df.columns = df.columns.str.replace('+','')
            
            return df
            
            
        
    def uploadcalcdata(self,df : pd.DataFrame, skey : str) :
            
            df.to_hdf(self.getHDFFile, key=f'{skey}',complib='zlib' ) 


    def __IfKeyexists(self,filenm : str , sKey : str) -> bool :

            with h5py.File(filenm, "r") as Fobj:
                if sKey in Fobj:
                    return True  
                else :
                    return False
    
    def __Load_Transaction(self,Tran_File, rowcount : int = 0 ) -> pd.DataFrame :
        
        if rowcount == 0 :
            df = pd.read_excel(Tran_File, index_col= False) 
        else :
            df = pd.read_excel(Tran_File, index_col= False,nrows= rowcount)

        st.session_state.tran_data = df          
        return  st.session_state.tran_data
    
    def __Load_Holding(self,holding_File, rowcount : int = 0) -> pd.DataFrame :

        if rowcount == 0 :    

            df = pd.read_excel(holding_File, index_col= False) 
        else :
            df = pd.read_excel(holding_File, index_col= False, nrows= rowcount)

        st.session_state.holding_data = df          
        return  st.session_state.holding_data

    def __DataframeRestructure(self,df : pd.DataFrame) -> pd.DataFrame :
        
        df = df.astype(str)
        df = df.replace(np.nan,'',regex=True)
        self.__ModifyColumns(df)

        return df


    def __correctfileschema(self,filetyp : str) -> list :
        
        with open('config.JSON') as config_file:
            data = json.load(config_file)
            
            if filetyp == 'H' :
                input_data = [str(data["source_holding_details"][k]).strip() for k in data["source_holding_details"]]
            else :
                input_data = [str(data["source_transaction_details"][k]).strip() for k in data["source_transaction_details"]]

        return input_data         

    def __checkOutBoundcols(self,filetyp : str , inboundfilecols : list) -> bool :

        with open('config.JSON') as config_file:
            data = json.load(config_file)

            if filetyp == 'H' :

                input_data = data["source_holding_details"]
            else :
                input_data = data["source_transaction_details"]

            helper = Mapping(mapinfo= input_data)
            check =  helper.IsvalidStruct(inboundfilecols)
            
            return check
                

    def __TransactionFiletobeUploaded(self) -> pd.DataFrame :
         
         self.datatran_file_xls = st.file_uploader("Transaction trades",type=['xlsx'] ) 
         return self.datatran_file_xls


    def __holdingFiletobeUploaded(self) -> pd.DataFrame :
         
         self.datahldg_file_xls = st.file_uploader("Holding period trades",type=['xlsx'] )
         return self.datahldg_file_xls


    def __ValidateFund(self, hld : pd.DataFrame, trn : pd.DataFrame) -> bool:

        b_check = True
        if  not hld.empty :
            if not trn.empty :
                
                Fund_h = sorted(hld['Fund'].unique())
                Fund_t = sorted(trn['Fund'].unique())
                
                b_check = (str(Fund_h) == str(Fund_t))

        return b_check    

    
    def ShowhUploader(self) :

            st.subheader("Upload trade transaction file(s)") 
            
            datahldg_file_xls = self.__holdingFiletobeUploaded()
            datatran_file_xls = self.__TransactionFiletobeUploaded()

            

            
            if  datatran_file_xls is not None :    
                    
                    with st.spinner ("Uploading File(s) ") : 

                        df_tran = self.__Load_Transaction(datatran_file_xls)

                        IsValid = self.__checkOutBoundcols('T', df_tran.columns )

                        if not IsValid :
                            st.error("In-bound Transaction file structure doesn't match with the required columns.")
                            with st.expander("The columns of the Transaction file  should be as below ", expanded= True) :
                                st.write(self.__correctfileschema('T'))
                            st.stop()  

                        df_tran = self.__DataframeRestructure(df_tran)
                        
                        Fund = sorted(df_tran['Fund'].unique())
                        df_tran['TradeDate'] = pd.to_datetime(df_tran['TradeDate'], format='mixed')
                        dates = sorted(df_tran['TradeDate'].dt.strftime('%m-%d-%Y').unique()) 
                        
                        Year = sorted(df_tran['TradeDate'].dt.strftime('%Y').unique())
                        
                        col1, col2 = st.columns(2)

                        Selected_Fund = col1.selectbox(label= "Fund" , options=Fund)
                        selected_Year = col2.selectbox("Year", options=Year)
                        
                        fnd =  f"{Selected_Fund}"
                        yr = f"{selected_Year}"
                        prevYr = int(yr) - 1
                        prevyr = str(prevYr)

                        hdKey = Hdf5key(fund= fnd, yr= int(selected_Year) )
                        
                        tran_fnd_yr = hdKey.GetTranKey(fnd,yr)
                        
                        closinghld_fnd_prevyr_s = hdKey.GetHoldingKey(fnd,prevyr,0)
                        closinghld_fnd_prevyr_m = hdKey.GetHoldingKey(fnd,prevyr)


                        if  self.__IfKeyexists(self.getHDFFile,f'{closinghld_fnd_prevyr_s}' ) :
                            if datahldg_file_xls is not None :
                                 st.error("Opening holding balanace has already been calculated.Do not upload holding file.")
                                 st.stop()
                            else :     
                                df_hlding = hd.ReadFromHDF(self.getHDFFile,f'{closinghld_fnd_prevyr_s}')
                                df_hlding = self.__DataframeRestructure(df_hlding)
                        else :
                            if datahldg_file_xls is not None :
                                df_hlding = self.__Load_Holding(datahldg_file_xls)
                                                                
                                IsValid = self.__checkOutBoundcols('H', df_hlding.columns )
                                if not IsValid :
                                    st.error("In-bound holding file structure doesn't match with the required columns.")
                                    with st.expander("The columns of the holding file  should be as below ", expanded= True) :
                                        st.write(self.__correctfileschema('H')   )
                                    st.stop()
                                      
                                if not self.__ValidateFund(hld=df_hlding, trn= df_tran):
                                    st.error("Mismatch in Fund(s) in holding and transaction files")
                                    st.stop()    
                                    
                                else :    
                                    df_hlding = self.__DataframeRestructure(df_hlding)

                            else :
                                 df_hlding = pd.DataFrame()
                                 
                        st.session_state.is_uploaded = True

                        def processdata() :
                            if Selected_Fund is not None and selected_Year is not None :
                                
                                if not df_hlding.empty : 
                                    hd.WriteToHDF(self.getHDFFile,df_hlding,f'{closinghld_fnd_prevyr_m}')

                                if not df_tran.empty:    
                                    hd.WriteToHDF(self.getHDFFile,df_tran,f'{tran_fnd_yr}')    
                                
                                st.session_state.is_processed = True

                        def processdata_parquet() :
                            if Selected_Fund is not None and selected_Year is not None :
                                
                                # if not df_hlding.empty : 
                                #     hd.WriteToHDF(self.getHDFFile,df_hlding,f'{closinghld_fnd_prevyr_m}')

                                # if not df_tran.empty:    
                                #     hd.WriteToHDF(self.getHDFFile,df_tran,f'{tran_fnd_yr}')    
                                
                                st.session_state.is_processed = True        


                        if st.session_state.is_uploaded :
                            #st.dataframe(df_tran.head())
                            #st.dataframe(df_hlding.head())
                            if st.button("Process uploaded data" , on_click= processdata ) :
                                st.success("Uploaded data processed " )         
                        
                    
                    
                        
                
        
         

            
