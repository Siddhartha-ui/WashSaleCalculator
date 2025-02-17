
import streamlit as st # type: ignore
from  Hdf5Key import Hdf5key
import h5py # type: ignore
import pandas as pd # type: ignore
from PrepareData import PrepareData
import Calculate as rl
import numpy as np # type: ignore
import duckdb as db # type: ignore
import HDFOperation as hdf
import streamlit as st # type: ignore
from mapping import Mapping
import json

import time



class CalcwashSale(object) :

    def __init__(self) -> None:
         
         con = db.connect()
         self.__con = con
         HDFFile = "data\\stock_1.h5"
         self.__HDFFile = HDFFile
         self.__stratpos =  int(len(st.session_state.user_id_rnd) + 1)
         self.__user =  st.session_state.user_id_rnd
         self.__time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
         


    def gettime(self):
        
        t = self.__time
        return str(t)


    @property
    def con(self) :
        return self.__con

    @property
    def keyStartPos(self) :
        return self.__stratpos   

    @property
    def user(self) :
         return self.__user
    
    @staticmethod
    def querybuild_Tran() -> str:
        with open('config.JSON') as config_file:
            data = json.load(config_file)
            
            helper = Mapping(mapinfo=data["source_dest_tran_mapping"])
            return helper.getkeybyValueMap()

    @staticmethod
    def querybuild_holding() -> str:
        with open('config.JSON') as config_file:
            data = json.load(config_file)
            
            helper = Mapping(mapinfo=data["source_dest_holding_mapping"])
            return helper.getkeybyValueMap()


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
    
    def __DataframeRestructure(self,df : pd.DataFrame) -> pd.DataFrame :

        df = df.astype(str)
        df = df.replace(np.nan,'',regex=True)
        self.__ModifyColumns(df)

        return df
    
    def __convertDict(self,df : pd.DataFrame) -> pd.DataFrame:
        convert_dict = {'num_shares': float, 'symbol': object , 'description' : object, 'buy_date' : object,
                 'adjusted_buy_date' : object , 'basis' : float , 'adjusted_basis' : float ,
                   'sell_date' : object ,'proceeds' : float , 'adjustment_code' : object ,
                    'adjustment' : float , 'form_position' : object, 'buy_lot' : object, 'replacement_for' : object,
                     'is_replacement' : object , 'loss_processed' : object , 'parent_lot' : object, 'adjusted_sell_date' : object, 'adjusted_proceeds' : float }

        df = df.astype(convert_dict)
        return df

    def __setaxis(self,df : pd.DataFrame) -> pd.DataFrame :

        cols =  ['num_shares', 'symbol', 'description' ,'buy_date','adjusted_buy_date','basis','adjusted_basis','sell_date','proceeds','adjustment_code','adjustment','form_position','buy_lot','replacement_for','is_replacement','loss_processed','parent_lot','adjusted_sell_date','adjusted_proceeds']
        df.columns = cols
        return df
    
    def __handleNULLvalueinDF(self,df : pd.DataFrame) -> pd.DataFrame:
        f = df.select_dtypes(include='float64').columns
        i = df.select_dtypes(include='int64').columns
        o = df.select_dtypes(include='object').columns

        df[f] = df[f.fillna(0)]
        df[i] = df[i.fillna(0)]
        df[o] = df[o.fillna('')]

        return df


    @property
    def getHDFFile(self) :
         return self.__HDFFile

    def __calculateWashSale(self,df : pd.DataFrame,isrlzd : int) -> pd.DataFrame:

        df = df.replace(np.nan,'',regex=True)

        result = df.to_dict('records')
        li = []
        for i in  range(len(result)) :

            row =  {key: str(value) for (key, value) in result[i].items()}
            li.append(row)


        lt = rl.HandleListObject(li,2)

        df = pd.DataFrame(lt)
        return df



    def __calculateWashSale_Split(self,df : pd.DataFrame,isrlzd : int) -> pd.DataFrame:

        df = df.replace(np.nan,'',regex=True)


        result = df.to_dict('records')
        li = []
        for i in  range(len(result)) :

            row =  {key: str(value) for (key, value) in result[i].items()}
            li.append(row)

        
        lt = rl.HandleListObject(li,0)
        
        df = pd.DataFrame(lt)
        return df


    def __calculateRLZDUNRLZD(self,df : pd.DataFrame, isrlzd : int) -> pd.DataFrame:

        df = df.replace(np.nan,'',regex=True)

        result = df.to_dict('records')
        li = []
        for i in  range(len(result)) :

            row =  {key: str(value) for (key, value) in result[i].items()}
            li.append(row)

        
        lt = rl.HandleListObject(li,1)
        
        df = pd.DataFrame(lt)
        return df


    def calculateWashSale(df : pd.DataFrame,isrlzd : int) -> pd.DataFrame:

        df = df.replace(np.nan,'',regex=True)

        result = df.to_dict('records')
        li = []
        for i in  range(len(result)) :

            row =  {key: str(value) for (key, value) in result[i].items()}
            li.append(row)


        lt = rl.HandleListObject(li,2)

        df = pd.DataFrame(lt)
        return df
     
    def __GetKeyParamFund(self,f, a: int ) -> list :
        
                with h5py.File(self.getHDFFile , mode= 'a') as f :
                    s ='' 
                    ParamKey = []
                    for k1 in f.keys():
                            
                            if k1.count('\\') == 2 :
                               s = str(k1)
                               
                            if k1.count('\\') == 3 :
                               if self.user == k1[:len(self.user)]:
                                  s = str(k1[self.keyStartPos:])
                                  
                               else :
                                  pass
                            else :
                               pass                 
                            if len(s) > 0 :   
                                c = '\\'
                                p = [pos for pos, char in enumerate(s) if char == c]

                                endpos = p[a]
                                if a == 0 :
                                    start_index=0
                                    endpos = p[a]
                                elif a == 1 :
                                    start_index=p[0] + 1
                                    endpos = p[a]

                                if s[start_index:endpos] not in ParamKey:

                                    ParamKey.append(s[start_index:endpos])


                    return ParamKey

    
    def GetKeyParamFund(self,f, a: int ) -> list :
        
        
        return self.__GetKeyParamFund(f , a)

    def __Fileobj(self):
         with h5py.File(self.__HDFFile, 'a') as f:
             fileObj = f
             return fileObj
    
    @property
    def getfileobj(self):
        
        return self.__Fileobj 

    def __IfKeyexists(self,filenm : str , sKey : str) -> bool :

            with h5py.File(filenm, "r") as Fobj:
                if sKey in Fobj:
                    return True  
                else :
                    return False
                
    def IfKeyexists(self,filenm : str , sKey : str) -> bool :
        return self.__IfKeyexists (filenm, sKey) 


    def __create_raw_data_in_momorydb_tran(self,df_tran : pd.DataFrame) -> pd.DataFrame :

        query_t = CalcwashSale.querybuild_Tran()
        
        self.con.execute(f"""CREATE OR REPLACE TABLE Transaction AS SELECT {query_t} FROM df_tran """)
        df = self.con.execute(f""" select * from Transaction """).df()
        return df


    def __create_raw_data_in_momorydb_holding(self,df_hlding : pd.DataFrame) -> pd.DataFrame :

        query_h = CalcwashSale.querybuild_holding()
                
        self.con.execute(f"""CREATE OR REPLACE TABLE Holding AS SELECT {query_h} FROM df_hlding """)
        df = self.con.execute(f""" select * from Holding """).df()
        return df
    

    def calcWashSale(self):
            
            pr = PrepareData()
            
            ##st.subheader("Wash sale calculation")
            
            col1 , col2,col3 =  st.columns([1,1,1])

            Fund = col1.selectbox("Select Fund" , options= self.__GetKeyParamFund(self.getfileobj,0))
            Yr = col2.selectbox("Select Year" , options= self.__GetKeyParamFund(self.getfileobj,1))

            if Fund is not None and Yr is not None :
                
                prevyr = int(Yr) - 1
                prevyr = str(prevyr)
                
                fnd =  f"{Fund}"
                yr = f"{Yr}"

                hdKey = Hdf5key(fund=Fund,yr=int(Yr))
                """  This is to check holding case"""
                hld_fnd_prevyr_m = hdKey.GetHoldingKey(fnd, prevyr)
                hld_fnd_prevyr_s = hdKey.GetHoldingKey(fnd, prevyr,0)

                if  self.__IfKeyexists(self.getHDFFile,f'{hld_fnd_prevyr_s}' ) :
                    hld_fnd_prevyr = hld_fnd_prevyr_s
                else :
                    hld_fnd_prevyr = hld_fnd_prevyr_m

                
                hld_fnd_yr_for_nxt_yr_ophold = hdKey.GetHoldingKey(fnd, yr,0)

                tran_fnd_yr = hdKey.GetTranKey(fnd,yr)

                calc_fnd_yr = hdKey.GetCalcKey(fnd,yr)
                
                s_yr = str(yr)
                if not self.__IfKeyexists(self.getHDFFile,f'{hld_fnd_prevyr}' ) :
                    st.error ("No opening holding data found ")
                    df_hlding = pd.DataFrame()
            
                else :
                    df_hld = hdf.ReadFromHDF(FileNm=self.getHDFFile, sKey=f'{hld_fnd_prevyr}')
                    
                    df_hlding = self.__create_raw_data_in_momorydb_holding(df_hlding= df_hld)
                
                if not self.__IfKeyexists(self.getHDFFile,f'{tran_fnd_yr}' ) :
                    st.error ("No transaction data found ")
                    st.stop()

                df_t = hdf.ReadFromHDF(FileNm = self.getHDFFile, sKey=f'{tran_fnd_yr}')
                df_tran = self.__create_raw_data_in_momorydb_tran(df_t)
                
                df_remaining = df_tran.copy()

                dates = sorted(df_tran['TradeDate'].dt.strftime('%m/%d/%Y').unique(),reverse= True)
                select_asof_date = col3.selectbox("Select Asof Date" , options= dates)

                all = st.sidebar.checkbox("Select all", value= True)

                df_all = pr.GetAllSymbols(df_h= df_hlding , df_t= df_tran)

                select_symbol = pr.UniqueSymbol(df_all, isfinal=0)

                symbol_selections = []

                if all:

                    symbol_selections = st.sidebar.multiselect(

                    label="Select Symbol", options=select_symbol, default=select_symbol,placeholder= "Symbols" #, on_change= isSymbolChanged
                )
                else :
                    symbol_selections = st.sidebar.multiselect(

                    label= "Select Symbol", options=select_symbol, default=None , placeholder = "None" #on_change= isSymbolChanged
                )
            
            if Fund is not None and Yr is not None and select_asof_date is not None  :

                col1, col2,col3 = st.columns([1,1,3])
                btn_calc = col1.button("Calculate Wash Sale")
                
                if btn_calc :

                    if df_hlding.empty :

                        df_tran = pr.filter_data(df_tran,symbol_selections)
                        
                        df_tran = df_tran[df_tran['TradeDate'].dt.strftime('%m/%d/%Y') <= select_asof_date]
                        df_remaining  = df_remaining[df_remaining['TradeDate'].dt.strftime('%m/%d/%Y') > select_asof_date]
                        df_tran = self.__DataframeRestructure(df_tran)
                        
                    
                    else :
                        df_tran = pr.filter_data(df_tran,symbol_selections)
                        df_hlding = pr.filter_data(df_hlding,symbol_selections)
                    
                        df_tran = df_tran[df_tran['TradeDate'].dt.strftime('%m/%d/%Y') <= select_asof_date]
                        df_remaining  = df_remaining[df_remaining['TradeDate'].dt.strftime('%m/%d/%Y') > select_asof_date]

                        df_tran = self.__DataframeRestructure(df_tran)
                        df_hlding = self.__DataframeRestructure(df_hlding) 
                    
                    

                    pr.CreateData(df_hlding, df_tran)
                    
                    df= pr.FinalDataForCalc()
                    # st.subheader("Final data before calc")
                    # st.data_editor(df["symbol"].unique(),use_container_width= True, hide_index= True)
                    # st.write(len(df["symbol"].unique()))
                    # st.stop()
                    
                    df_toeliminateWS = df.copy(deep=True)
                    holdingchkDF = df_hlding.copy(deep=True)
                    
                    with st.spinner("Calculating Wash Sale . Please wait ...") :


                        df_c = self.__setaxis(df)
                        df_c = self.__handleNULLvalueinDF(df)

                        df_c = self.__convertDict(df)   

                        df_c['buy_date'] = pd.to_datetime(df_c['buy_date'], format='mixed')
                        df_c['buy_date'] = df_c['buy_date'].dt.strftime('%m/%d/%Y')
                        df_c['sell_date'] = pd.to_datetime(df_c['sell_date'], format='mixed')
                        df_c['sell_date'] = df_c['sell_date'].dt.strftime('%m/%d/%Y')
                        
                        df_excl = df_c.groupby(by="symbol", as_index=False)['num_shares'].sum() 
                        df_excl =df_excl[df_excl['num_shares'] == 0]
                        if not holdingchkDF.empty :
                            df_excl = df_excl[~df_excl.symbol.isin(holdingchkDF.Symbol)] 
                        
                        df_c = df_c[~df_c.symbol.isin(df_excl.symbol)] 
                        
                        df_c = df_c.sort_values(by=['symbol','buy_date','sell_date'])
                        df_calc = self.__calculateRLZDUNRLZD(df_c,1)
                                                                        
                        if not df_calc.empty :
                            
                            df_calc["is_replacement"] = False
                            df_calc["replacement_for"] = ''
                            df_calc["loss_processed"] = False
                            df_calc = self.__calculateWashSale(df_calc,2)
                            
                            hdf.WriteToHDF(self.getHDFFile,df_calc,f'{calc_fnd_yr}')

                            df_r = pr.getremainingtran(df_remaining)

                            df_r['buy_date'] = pd.to_datetime(df_r['buy_date'], format='mixed')
                            df_r['buy_date'] = df_r['buy_date'].dt.strftime('%m/%d/%Y')
                            df_r['sell_date'] = pd.to_datetime(df_r['sell_date'], format='mixed')
                            df_r['sell_date'] = df_r['sell_date'].dt.strftime('%m/%d/%Y')
                            
                            df_hld = pr.GetClosingHoldingReport(df_calc,df_r)
                            df_hld = self.__DataframeRestructure(df_hld)

                            hdf.WriteToHDF(self.getHDFFile,df_hld,f'{hld_fnd_yr_for_nxt_yr_ophold}')

                            df_wash = df_calc.groupby(by="symbol", as_index=False)['adjustment'].sum()
                            df_wash = df_wash[df_wash['adjustment'] != 0]

                            if df_wash.empty :
                                st.error(f"No Wash Sale found Asof {select_asof_date}")
                            else :
                                st.success("Calculation completed!")
                                st.session_state.asof_date = str(select_asof_date)
                        else :
                            st.error(f"No Wash Sale found Asof {select_asof_date}")
            
            if Fund is not None and Yr is not None :
                

                    prev_yr = int(yr) - 1
                    prev_yr = str(prev_yr)

                    hdKey = Hdf5key(fund= fnd , yr= int(Yr))
                    
                    closinghld_fnd_prevyr_s = hdKey.GetHoldingKey(fund= Fund, Yr=prev_yr,Ismanual= 0)
                    closinghld_fnd_prevyr_m = hdKey.GetHoldingKey(fund= Fund, Yr=prev_yr)

                    if self.__IfKeyexists(self.getHDFFile,f'{closinghld_fnd_prevyr_s}' ) :
                       hld_fnd_prevyr =  closinghld_fnd_prevyr_s
                    else :
                       hld_fnd_prevyr =  closinghld_fnd_prevyr_m      
                    
                    if not self.__IfKeyexists(self.getHDFFile,f'{hld_fnd_prevyr}' ) :
                        df_hlding = pd.DataFrame()

                    else :
                        df_hlding = pd.read_hdf(self.getHDFFile, key=f'{hld_fnd_prevyr}')
                        df_hlding = pr.filter_data(df_hlding, symbol_selections)

                    

                    with st.expander("Select data", expanded= False) :
                        data_set = st.selectbox("Select data set" , options= [None ,"Holding data" , "Transaction data"] )

                    if data_set :
                            
                            if data_set == "Holding data":
                                    
                                if not df_hlding.empty :
                                    st.subheader(f"Opening holding data for {s_yr} " )
                                    st.data_editor(df_hlding, use_container_width= True , hide_index= True)
                
                                else :
                                    st.info ("No holding data found!")     

                    if data_set == "Transaction data":        

                                    st.subheader(f"Transaction data for   {s_yr} Asof {select_asof_date}")
                                    df_tran = pd.read_hdf(self.getHDFFile, key=f'{tran_fnd_yr}')
                                    df_tran = pr.filter_data(df_tran,symbol_selections)
                                    df_tran = df_tran[df_tran['TradeDate'].dt.strftime("%m/%d/%Y") <= select_asof_date]
                                    st.data_editor(df_tran,use_container_width= True,hide_index=True)
                


class WashsaleReport(CalcwashSale):
     
    def __init__(self) -> None:
          super().__init__()

     
    def __download_csv(self,df : pd.DataFrame, fileName : str) :
        csv = self.__convert_df(df)
        st.download_button(
        "Download file",
        csv,
        fileName + ".csv",
        "text/csv",
        key='download-csv'
    )

    def __convert_df(self,df):
        return df.to_csv(index=False).encode('utf-8')   

    def __getPriorYrCalc(self,fnd ,Yr) -> pd.DataFrame:

        Yr = int(Yr)-1
        yr = f"{Yr}"
        hdKey = Hdf5key(fund= fnd , yr= int(Yr))
        calc_fnd_yr_prior = hdKey.GetCalcKey(fnd,yr)

        if not super().IfKeyexists(super().getHDFFile,f'{calc_fnd_yr_prior}' ) :
            df = pd.DataFrame()        
        else :    
            df = pd.read_hdf(super().getHDFFile, key=f'{calc_fnd_yr_prior}') 
            df.rename(columns = {'symbol':'Symbol'}, inplace = True)

        return df


    def runreport(self) :
        
        pr = PrepareData()
        
        run_asof = st.session_state.asof_date
        if len(st.session_state.asof_date) > 0 :
            run_asof = " (Calculation last run Asof " + st.session_state.asof_date + ")"

        #st.subheader(f"""Reports """)
        
        with st.container() :
            col1 , col2, col3 =  st.columns([1,1,2])

            Fund = col1.selectbox("Select Fund" , options= super().GetKeyParamFund(super().getfileobj,0))
            Yr = col2.selectbox("Select Year" , options= super().GetKeyParamFund(super().getfileobj,1))

            op = ["Wash Sale Detail" , "Realized Gain/Loss" , "Unrealized transaction" , 
                "Wash Sale Summary", "Total Wash Sale" ,  "Closing holding balance" , "Adjusted Cost Detail", 
                "Adjusted Cost Summary", "Closing Holding Summary", "Adjusted Cost Total" , "Realized Gain/Loss Total", "Change In RLZD GL"]
            Reports = col3.selectbox("Select Report", options= op)

            fnd =  f"{Fund}"
            yr = f"{Yr}"


            
            if Fund is not None and Yr is not None :

                download_file_name = fnd + "_" + yr + "_" + Reports     

                hdKey = Hdf5key(fund= fnd , yr= int(Yr))
                calc_fnd_yr = hdKey.GetCalcKey(fnd,yr)
                calc_tran_yr = hdKey.GetTranKey(fnd,yr)


                if not super().IfKeyexists(super().getHDFFile,f'{calc_fnd_yr}' ) :
                        st.error ("No data found")
                        st.stop()


                if super().IfKeyexists(super().getHDFFile,f'{calc_tran_yr}' ) :
                        calc_df_o = pd.read_hdf(super().getHDFFile, key=f'{calc_tran_yr}')
                        calc_df_o_unique = calc_df_o[['Symbol', 'ProceedsCurrency']].drop_duplicates().reset_index(drop=True)
                
                calc_df = pd.read_hdf(super().getHDFFile, key=f'{calc_fnd_yr}')

                            
                select_symbol = pr.UniqueSymbol(calc_df, isfinal= 1)


                symbol_selections = []
                all = st.sidebar.checkbox("Select all", value= True)
                if all:
                    #isSymbolChanged()
                    symbol_selections = st.sidebar.multiselect(

                    "Select Symbol", options=select_symbol, default=select_symbol #, on_change= isSymbolChanged
                )
                else :
                    symbol_selections = st.sidebar.multiselect(

                    "Select Symbol", options=select_symbol, default=None #, on_change= isSymbolChanged
                )

                calc_df.rename(columns = {'symbol':'Symbol'}, inplace = True)
                calc_df = pr.filter_data(calc_df, symbol_selections)

                calc_df_o_unique = pr.filter_data(calc_df_o_unique, symbol_selections)
                
                if Reports == "Wash Sale Detail" :
                    calc_df = pr.GetWashSaleReport(calc_df, calc_df_o_unique, 1)
                if Reports == "Total Wash Sale" :
                    calc_df = pr.GetWashSaleReport(calc_df, calc_df_o_unique, 2)
                elif Reports == "Wash Sale Summary" :
                    calc_df = pr.GetWashSaleReport(calc_df, calc_df_o_unique, 0)
                elif Reports == "Closing holding balance" :
                    calc_df =  pr.GetClosingHoldingWithAdjReport(calc_df)
                elif Reports == "Adjusted Cost Detail" :
                    calc_df =  pr.GetoffsetHoldingReport(calc_df)
                elif Reports == "Adjusted Cost Total" :
                    calc_df =  pr.GetoffsetHoldingReportdetail(calc_df)     
                elif Reports == "Adjusted Cost Summary" :
                    calc_df =  pr.GetoffsetHoldingReportTotal(calc_df)
                elif Reports == "Closing Holding Summary" :
                    calc_df =  pr.GetClosingHoldingWithAdjSummaryReport(calc_df)
                elif Reports == "Realized Gain/Loss" :
                    calc_df =  pr.RealizedGL(calc_df)
                elif Reports == "Realized Gain/Loss Total" :
                    calc_df =  pr.RealizedGLTotal(calc_df)

                elif Reports == "Unrealized transaction" :
                    calc_df =  pr.UNRealizedGL(calc_df)
                
                elif Reports == "Change In RLZD GL" :
                    
                    calc_df_prioryr = self.__getPriorYrCalc(fnd,yr)
                    if not calc_df_prioryr.empty :
                        df_calc_c =  pr.RealizedGLTotal(calc_df)
                        df_calc_p = pr.GetoffsetHoldingReportdetail(calc_df_prioryr)   
                        df = pr.RealizedGLChange(df_calc_c,df_calc_p) 
                        calc_df = df
                    else:

                        st.error("Required data not found")
                        st.stop()


                st.data_editor(calc_df,use_container_width= True, hide_index= True)

                self.__download_csv(calc_df, download_file_name)    
