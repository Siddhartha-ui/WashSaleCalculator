
import streamlit as st
import pandas as pd
import duckdb as db
import numpy as np


class PrepareData(object):
     
    def __init__(self) -> None:
          
          if not "Symbol_Changed"  in st.session_state :
            st.session_state.Symbol_Changed = False
            
          __con = db.connect()
          self.con = __con.cursor()

    
    def filter_data(self, df: pd.DataFrame, selections: list[str] ) -> pd.DataFrame:
            
        df = df.copy()
        df = df[df['Symbol'].isin(selections)]

        return df        

    
    def isSymbolChanged(self) :
            
            st.session_state.Symbol_Changed = True
            
    def UniqueSymbol(self,df: pd.DataFrame, isfinal : int) -> list :

            if isfinal == 1 :
                df.rename(columns = {'Symbol':'symbol'}, inplace = True)
                select_symbol = sorted(df['symbol'].unique())
            else :
                select_symbol = sorted(df['Symbol'].unique())  

            return select_symbol
    
    def CreateOrReplaceHoldingTbl(self) :
        self.con.execute(f"""CREATE OR REPLACE TABLE Holding AS SELECT * FROM df_hlding""") 
    
    def InsertHoldingTbl(self) :
        self.con.execute(f"""delete from Holding""")
        self.con.execute(f"""INSERT INTO Holding SELECT * FROM df_hlding""")
    
    def CreateOrReplaceTransactionTbl(self) :
        self.con.execute(f"""CREATE OR REPLACE TABLE Transaction AS SELECT * FROM df_tran""")


    def InsertTransactionTbl(self) :
        self.con.execute(f"""delete from  Transaction""")
        self.con.execute(f"""INSERT INTO Transaction SELECT * FROM df_tran""")

    def ModifyColumns(self, df : pd.DataFrame ) -> pd.DataFrame :
            
            df.columns = df.columns.str.replace(' ','')
            df.columns = df.columns.str.replace('/','')
            df.columns = df.columns.str.replace('&','')
            df.columns = df.columns.str.replace('(','')
            df.columns = df.columns.str.replace(')','')
            df.columns = df.columns.str.replace('%','')
            df.columns = df.columns.str.replace(f"'",'')
            df.columns = df.columns.str.replace('+','')
            
            return df   

    
    def __TransactionDataSQLstring(self) -> str :
            ssql = f"""
                            
                            select
                            
                            case when 
                                 TransactionType = 'Sell' then  abs(cast(Quantity as double)) * (-1)
                            else     
                                abs(cast(Quantity as double)) 
                            end AS num_shares,

                            
                            Symbol AS symbol,
                            '' AS description,

                            case when 
                                TransactionType = 'Buy' then TradeDate else '' 
                            end AS buy_date,

                            '' AS adjusted_buy_date,
                            case when 
                                TransactionType = 'Buy'  then cast(NetProceeds AS  double) 
                                else  
                                0.00 
                            end AS basis,

                            0.00 AS adjusted_basis,
                            case when 
                                TransactionType = 'Sell'  then TradeDate 
                            else '' 
                            end AS sell_date,

                            case when 
                                TransactionType = 'Sell' then cast(NetProceeds AS  double) 
                            else 0.00 
                            end AS proceeds,

                            '' AS adjustment_code,
                            0.00 AS adjustment,
                        '' AS form_position,
                        Id AS buy_lot,
                        '' AS replacement_for,
                        '' AS is_replacement,
                        '' AS loss_processed,
                        '' AS parent_lot,
                        '' AS adjusted_sell_date,
                        0.00 AS  adjusted_proceeds
                            from Transaction
                            where Quantity <> 0
                    """
            return ssql 

    def __HoldingDataSQLstring(self) -> str :
            ssql = f"""
                            select  Quantity AS num_shares ,
                            Symbol AS symbol,
                            SymbolDescription AS description,
                            (case when Quantity > 0 then TradeDate else '' end) AS buy_date,
                            '' AS adjusted_buy_date,
                            (case when Quantity > 0 then abs(cast(TotalCost as double)) * (-1) else 0.00 end) AS basis,
                            0.00 AS adjusted_basis,
                            (case when Quantity < 0 then TradeDate else '' end) AS sell_date,
                            (case when Quantity < 0 then abs(cast(TotalCost as double))  else 0.00 end) AS proceeds,
                            '' AS adjustment_code,
                            0.00 AS adjustment,
                            '' AS form_position,
                            TrxNum AS buy_lot,
                            '' AS replacement_for,
                            '' AS is_replacement,
                            '' AS loss_processed,
                            '' AS parent_lot,
                            '' AS adjusted_sell_date,
                            0.00 AS  adjusted_proceeds

                            from Holding
                            where Quantity <> 0
                    """
            return ssql 

    def __PrepareHoldingData(self,ssql : str) -> pd.DataFrame:
            df =  self.con.execute(ssql).df()
            return df

    def __PrepareTransactionData(self,ssql : str) -> pd.DataFrame:
            df =  self.con.execute(ssql).df()

            return df

    def combineholdingTran( self, df_t : pd.DataFrame, df_h : pd.DataFrame) -> pd.DataFrame :
            frames = [df_t, df_h]
            
            result = pd.concat(frames)
            return result

    def __createTransactiononly(self, df_tran : pd.DataFrame) -> None :
            
            self.con.register('Transaction', df_tran)
            
            ssql_tran = self.__TransactionDataSQLstring()
            df_tran = self.__PrepareTransactionData(ssql_tran)
            
            self.con.execute(f"""

                        CREATE OR REPLACE TABLE combined AS
                        
                          SELECT cast(num_shares as double) AS num_shares ,
                                Symbol,
                                description,
                                buy_date,
                                adjusted_buy_date,
                                cast(basis as double) AS basis,
                                cast(adjusted_basis as double) as adjusted_basis,
                                sell_date,
                                cast(proceeds as double)  as proceeds,
                                adjustment_code ,
                                cast(adjustment as double)  as adjustment,
                                form_position,
                                buy_lot as buy_lot,
                                replacement_for,
                                is_replacement,
                                loss_processed,
                                parent_lot,
                                adjusted_sell_date,
                                cast(adjusted_proceeds as double) as adjusted_proceeds 
                                    
                            FROM df_tran
                            """
                    )


    def CreateData(self, df : pd.DataFrame , df_tran : pd.DataFrame ) -> None :
            
            

            if df.empty :
               self.__createTransactiononly(df_tran= df_tran)

            else :       

                self.con.register('Holding', df)
                self.con.register('Transaction', df_tran)
                
                ssql = self.__HoldingDataSQLstring()
                df = self.__PrepareHoldingData(ssql)

                

                ssql_tran = self.__TransactionDataSQLstring()
                df_tran = self.__PrepareTransactionData(ssql_tran)
                    
                    
                self.con.execute("CREATE OR REPLACE TABLE combined AS SELECT * FROM df")
                self.con.execute("delete from  combined")

                self.con.execute(f"""

                            CREATE OR REPLACE TABLE combined AS
                            select * from     
                            (    
                            SELECT cast(num_shares as double) AS num_shares ,
                            Symbol,
                            description,
                            buy_date,
                            adjusted_buy_date,
                            cast(basis as double) AS basis,
                            cast(adjusted_basis as double) as adjusted_basis,
                            sell_date,
                            cast(proceeds as double)  as proceeds,
                            adjustment_code ,
                            cast(adjustment as double)  as adjustment,
                            form_position,
                            buy_lot as buy_lot,
                            replacement_for,
                            is_replacement,
                            loss_processed,
                            parent_lot,
                            adjusted_sell_date,
                            cast(adjusted_proceeds as double) as adjusted_proceeds 
                                
                        FROM df
                        
                        
                        UNION ALL

                        SELECT cast(num_shares as double) AS num_shares ,
                            Symbol,
                            description,
                            buy_date,
                            adjusted_buy_date,
                            cast(basis as double) AS basis,
                            cast(adjusted_basis as double) as adjusted_basis,
                            sell_date,
                            cast(proceeds as double)  as proceeds,
                            adjustment_code ,
                            cast(adjustment as double)  as adjustment,
                            form_position,
                            buy_lot as buy_lot,
                            replacement_for,
                            is_replacement,
                            loss_processed,
                            parent_lot,
                            adjusted_sell_date,
                            cast(adjusted_proceeds as double) as adjusted_proceeds 
                                
                        FROM df_tran
                         ) combined

                        """
                        )

                


    def GetClosingHoldingReport(self,df : pd.DataFrame,df_r : pd.DataFrame ) -> pd.DataFrame :

            
            
        df = self.con.execute(f"""
                                        
                        select 
                        buy_lot AS "Trx Num" ,
                        Symbol AS Symbol ,   
                        
                        '' AS "Symbol Description",
                        '' AS "Deal Type",
                        '' AS "Curr" ,   
                        case when len(adjusted_buy_date) = 0 then buy_date else adjusted_buy_date end AS "Trade Date",
                        '' AS "Settle Date",
                        '' AS "Term",
                        '' AS "Exec. Broker", 
                        '' AS "Pct Assets",
                        num_shares AS Quantity,
                        (case when adjusted_basis = 0 then  abs(cast(basis as double)) else abs(cast(adjusted_basis as double)) end ) / num_shares AS "Unit Cost",             
                        Case when adjusted_basis = 0 then  abs(cast(basis as double)) else abs(cast(adjusted_basis as double)) end AS "Total Cost",
                        '' AS "Market Price",
                        '' AS "Market Value" ,
                        '' AS "Unrealized Gain/Loss",
                        '' AS "Pct G/L",
                        '' AS "Div/Int/Deal Accrual"       
                             
                        from df
                        where len(sell_date) = 0 and len(buy_date) > 0 and num_shares > 0

                        UNION ALL
                        
                        select 
                              
                        buy_lot AS "Trx Num" ,
                        Symbol AS Symbol , 
                        
                        '' AS "Symbol Description",
                        '' AS "Deal Type",
                        '' AS "Curr" , 
                        case when len(adjusted_sell_date) = 0 then sell_date else adjusted_sell_date end AS "Trade Date", 
                        '' AS "Settle Date",
                        '' AS "Term",
                        '' AS "Exec. Broker", 
                        '' AS "Pct Assets",       
                        num_shares AS Quantity,
                        
                        (Case when cast(adjusted_basis as double) = 0 then  abs(cast(proceeds as double)) * (-1) else abs(cast(adjusted_basis as double)) *(-1) end ) / num_shares AS "Unit Cost",
                        Case when cast(adjusted_basis as double) = 0 then  abs(cast(proceeds as double)) * (-1) else abs(cast(adjusted_basis as double)) *(-1) end AS "Total Cost",
                        '' AS "Market Price",
                        '' AS "Market Value" ,
                        '' AS "Unrealized Gain/Loss",
                        '' AS "Pct G/L",
                        '' AS "Div/Int/Deal Accrual"         
                        from df
                        where len(sell_date) > 0 and len(buy_date) = 0  and num_shares < 0 

                        UNION ALL

                        select 
                        buy_lot AS "Trx Num" ,
                        Symbol AS Symbol ,   
                        
                        '' AS "Symbol Description",
                        '' AS "Deal Type",
                        '' AS "Curr" ,   
                        case when len(adjusted_buy_date) = 0 then buy_date else adjusted_buy_date end AS "Trade Date",
                        '' AS "Settle Date",
                        '' AS "Term",
                        '' AS "Exec. Broker", 
                        '' AS "Pct Assets",
                        num_shares AS Quantity,
                        (case when adjusted_basis = 0 then  abs(cast(basis as double)) else abs(cast(adjusted_basis as double)) end ) / num_shares AS "Unit Cost",             
                        Case when adjusted_basis = 0 then  abs(cast(basis as double)) else abs(cast(adjusted_basis as double)) end AS "Total Cost",
                        '' AS "Market Price",
                        '' AS "Market Value" ,
                        '' AS "Unrealized Gain/Loss",
                        '' AS "Pct G/L",
                        '' AS "Div/Int/Deal Accrual"       
                            
                        from df_r
                        where len(sell_date) = 0 and len(buy_date) > 0 and num_shares > 0

                        UNION ALL
                        
                        select 
                        buy_lot AS "Trx Num" ,
                        Symbol AS Symbol , 
                        
                        '' AS "Symbol Description",
                        '' AS "Deal Type",
                        '' AS "Curr" , 
                        case when len(adjusted_sell_date) = 0 then sell_date else adjusted_sell_date end AS "Trade Date", 
                        '' AS "Settle Date",
                        '' AS "Term",
                        '' AS "Exec. Broker", 
                        '' AS "Pct Assets",       
                        num_shares AS Quantity,
                        
                        (Case when cast(adjusted_basis as double) = 0 then  abs(cast(proceeds as double)) * (-1) else abs(cast(adjusted_basis as double)) *(-1) end ) / num_shares AS "Unit Cost",
                        Case when cast(adjusted_basis as double) = 0 then  abs(cast(proceeds as double)) * (-1) else abs(cast(adjusted_basis as double)) *(-1) end AS "Total Cost",
                        '' AS "Market Price",
                        '' AS "Market Value" ,
                        '' AS "Unrealized Gain/Loss",
                        '' AS "Pct G/L",
                        '' AS "Div/Int/Deal Accrual"         
                        from df_r
                        where len(sell_date) > 0 and len(buy_date) = 0  and num_shares < 0        

                """
                            
            ).df()

            
        return df

    def GetoffsetHoldingReportTotal(self,df : pd.DataFrame ) -> pd.DataFrame :

            
            df = self.con.execute(f"""


                        select Symbol , 
                        sum(Quantity) AS Quantity , 
                        sum("Total Cost") AS "Total Cost",
                        sum("Adjusted Total Cost") AS "Adjusted Total Cost",
                        sum("Deffered Loss") AS "Deffered Loss"


                        from

                        (                
                        select 
                        
                        Symbol AS Symbol ,   
                        
                        num_shares AS Quantity,
                        cast(basis as double) AS "Total Cost",          
                        Case when adjusted_basis = 0 then  cast(basis as double) else cast(adjusted_basis as double) end AS "Adjusted Total Cost",
                        (Case when adjusted_basis = 0 then  cast(basis as double) else cast(adjusted_basis as double) end) - cast(basis as double) AS "Deffered Loss"
                            
                        from df
                        where len(sell_date) = 0 and len(buy_date) > 0 and num_shares > 0
                        and (Case when adjusted_basis = 0 then  abs(cast(basis as double)) else abs(cast(adjusted_basis as double)) end) - abs(cast(basis as double))  <> 0  

                        UNION ALL
                        
                        select 
                        
                        Symbol AS Symbol , 
                        
                        num_shares AS Quantity,
                        
                        cast(proceeds as double)  AS "Total Cost",  
                        Case when cast(adjusted_basis as double) = 0 then  cast(proceeds as double) else cast(adjusted_basis as double)  end AS "Adjusted Total Cost" ,
                        (Case when cast(adjusted_basis as double) = 0 then  cast(proceeds as double) else cast(adjusted_basis as double) end) - cast(proceeds as double) AS "Deffered Loss"  
                        
                        from df
                        where len(sell_date) > 0 and len(buy_date) = 0  and num_shares < 0
                        and (Case when cast(adjusted_basis as double) = 0 then  abs(cast(proceeds as double)) * (-1) else abs(cast(adjusted_basis as double)) *(-1) end) - abs(cast(proceeds as double)) * (-1) <> 0          
                        ) t

                        group by Symbol

                """
                            
            ).df()

            
            return df




    def GetoffsetHoldingReport(self,df : pd.DataFrame ) -> pd.DataFrame :

            
            df = self.con.execute(f"""
                                        
                        select 
                        buy_lot as ID,
                        Symbol AS Symbol ,   
                        
                        case when len(adjusted_buy_date) = 0 then buy_date else adjusted_buy_date end AS "Adjusted Trade Date",
                        buy_date AS "Trade Date",
                        num_shares AS Quantity,
                        (case when adjusted_basis = 0 then  abs(cast(basis as double)) else abs(cast(adjusted_basis as double)) end ) / num_shares AS "Unit Cost",
                        abs(cast(basis as double)) AS "Total Cost",          
                        Case when adjusted_basis = 0 then  abs(cast(basis as double)) else abs(cast(adjusted_basis as double)) end AS "Adjusted Total Cost",
                        ((Case when adjusted_basis = 0 then  abs(cast(basis as double)) else abs(cast(adjusted_basis as double)) end) - abs(cast(basis as double))) * (-1)  AS "Deffered Loss"
                            
                        from df
                        where len(sell_date) = 0 and len(buy_date) > 0 and num_shares > 0
                        and (Case when adjusted_basis = 0 then  abs(cast(basis as double)) else abs(cast(adjusted_basis as double)) end) - abs(cast(basis as double))  <> 0  

                        UNION ALL
                        
                        select 
                        
                        buy_lot as ID,          
                        Symbol AS Symbol , 
                        
                        case when len(adjusted_sell_date) = 0 then sell_date else adjusted_sell_date end AS "Adjusted Trade Date", 
                        sell_date AS "Trade Date",      
                        num_shares AS Quantity,
                        
                        (Case when cast(adjusted_basis as double) = 0 then  abs(cast(proceeds as double)) * (-1) else abs(cast(adjusted_basis as double)) *(-1) end ) / num_shares AS "Unit Cost",
                        abs(cast(proceeds as double)) * (-1) AS "Total Cost",  
                        Case when cast(adjusted_basis as double) = 0 then  abs(cast(proceeds as double)) * (-1) else abs(cast(adjusted_basis as double)) *(-1) end AS "Adjusted Total Cost" ,
                        (Case when cast(adjusted_basis as double) = 0 then  abs(cast(proceeds as double)) * (-1) else abs(cast(adjusted_basis as double)) *(-1) end) - abs(cast(proceeds as double)) * (-1) AS "Deffered Loss"  
                        
                        from df
                        where len(sell_date) > 0 and len(buy_date) = 0  and num_shares < 0
                        and (Case when cast(adjusted_basis as double) = 0 then  abs(cast(proceeds as double)) * (-1) else abs(cast(adjusted_basis as double)) *(-1) end) - abs(cast(proceeds as double)) * (-1) <> 0          

                """
                            
            ).df()

            
            return df


    def GetWashSaleReport(self,df : pd.DataFrame, df_Curr : pd.DataFrame, Isdetail : int) -> pd.DataFrame :

            
            if Isdetail == 1 :
                df = self.con.execute(f"""

                        select num_shares AS Quantity,
                        Symbol ,
                        buy_date AS "Buy Date",
                        sell_date AS "Sell Date",
                        buy_lot AS "Sell_Id",
                        parent_lot AS "Buy_Id",                        
                        basis AS "Cost Basis",
                        proceeds AS Proceeds,
                        (basis + proceeds) AS "Gain Loss",
                        adjustment * (-1) AS "Wash Sale"             
                        from df
                        where adjustment_code = 'W'      
                """
                            
                ).df()

            if Isdetail == 0 :
                df = self.con.execute(f"""

                        select sum(df.num_shares) AS Quantity,
                        df.Symbol ,
                        df_Curr.ProceedsCurrency AS Currency ,
                                      
                        Sum(adjustment) * (-1) AS "Wash Sale"             
                        from df df inner join df_Curr df_Curr
                        on df.Symbol =  df_Curr.Symbol             
                        where adjustment_code = 'W'
                        group by df.Symbol  , df_Curr.ProceedsCurrency            
                """
                            
                ).df()

            if Isdetail == 2 :
                df = self.con.execute(f"""

                        select sum(num_shares) AS Quantity,
                        
                        Sum(adjustment) * (-1) AS "Wash Sale"             
                        from df
                        where adjustment_code = 'W'
                        
                """
                            
                ).df()
            
            return df
    

    def GetClosingHoldingWithAdjReport(self,df : pd.DataFrame ) -> pd.DataFrame :

            
            df = self.con.execute(f"""

                                                 
                        select 
                        
                        Symbol AS Symbol ,   
                        buy_lot AS "Id",
                        case when len(adjusted_buy_date) = 0 then buy_date else adjusted_buy_date end AS "Adjusted Trade Date",
                        buy_date AS "Trade Date",
                        num_shares AS Quantity,
                        (case when adjusted_basis = 0 then  abs(cast(basis as double)) else abs(cast(adjusted_basis as double)) end ) / num_shares AS "Unit Cost",
                        abs(cast(basis as double)) AS "Total Cost",          
                        Case when adjusted_basis = 0 then  abs(cast(basis as double)) else abs(cast(adjusted_basis as double)) end AS "Adjusted Total Cost"
                        
                            
                        from df
                        where len(sell_date) = 0 and len(buy_date) > 0 and num_shares > 0
                        

                        UNION ALL


                        select 
                        
                        Symbol AS Symbol , 
                        buy_lot AS "Id",
                        case when len(adjusted_sell_date) = 0 then sell_date else adjusted_sell_date end AS "Adjusted Trade Date", 
                        sell_date AS "Trade Date",      
                        num_shares AS Quantity,
                        
                        (Case when cast(adjusted_basis as double) = 0 then  abs(cast(proceeds as double)) * (-1) else abs(cast(adjusted_basis as double)) *(-1) end ) / num_shares AS "Unit Cost",
                        abs(cast(proceeds as double)) * (-1) AS "Total Cost",  
                        Case when cast(adjusted_basis as double) = 0 then  abs(cast(proceeds as double)) * (-1) else abs(cast(adjusted_basis as double)) *(-1) end AS "Adjusted Total Cost" 
                        
                        
                        from df
                        where len(sell_date) > 0 and len(buy_date) = 0  and num_shares < 0
                        

                """
            
                            
            ).df()
            
            
            return df

    def GetClosingHoldingWithAdjReportSUM(self,df : pd.DataFrame ) -> pd.DataFrame :

            
            df = self.con.execute(f"""

                        select Symbol , 
                        Id , 
                        "Adjusted Trade Date",
                        "Trade Date" ,
                        sum(Quantity) AS Quantity ,
                        sum("Total Cost") AS "Total Cost",
                        sum("Adjusted Total Cost") AS "Adjusted Total Cost"                    
                                                    
                        from 
                                            
                        (                                  
                        select 
                        
                        Symbol AS Symbol ,   
                        buy_lot AS "Id",
                        case when len(adjusted_buy_date) = 0 then buy_date else adjusted_buy_date end AS "Adjusted Trade Date",
                        buy_date AS "Trade Date",
                        num_shares AS Quantity,
                        (case when adjusted_basis = 0 then  abs(cast(basis as double)) else abs(cast(adjusted_basis as double)) end ) / num_shares AS "Unit Cost",
                        abs(cast(basis as double)) AS "Total Cost",          
                        Case when adjusted_basis = 0 then  abs(cast(basis as double)) else abs(cast(adjusted_basis as double)) end AS "Adjusted Total Cost"
                        
                            
                        from df
                        where len(sell_date) = 0 and len(buy_date) > 0 and num_shares > 0
                        

                        UNION ALL


                        select 
                        
                        Symbol AS Symbol , 
                        buy_lot AS "Id",
                        case when len(adjusted_sell_date) = 0 then sell_date else adjusted_sell_date end AS "Adjusted Trade Date", 
                        sell_date AS "Trade Date",      
                        num_shares AS Quantity,
                        
                        (Case when cast(adjusted_basis as double) = 0 then  abs(cast(proceeds as double)) * (-1) else abs(cast(adjusted_basis as double)) *(-1) end ) / num_shares AS "Unit Cost",
                        abs(cast(proceeds as double)) * (-1) AS "Total Cost",  
                        Case when cast(adjusted_basis as double) = 0 then  abs(cast(proceeds as double)) * (-1) else abs(cast(adjusted_basis as double)) *(-1) end AS "Adjusted Total Cost" 
                        
                        
                        from df
                        where len(sell_date) > 0 and len(buy_date) = 0  and num_shares < 0
                        ) t

                       group by Symbol , Id , "Adjusted Trade Date", "Trade Date" 

                """
            
                            
            ).df()
            
            
            return df
    
    def GetClosingHoldingWithAdjSummaryReport(self, df : pd.DataFrame ) -> pd.DataFrame :

            
            df = self.con.execute(f"""

                        select 
                        
                        Symbol AS Symbol ,   
                        
                        sum(num_shares) AS Quantity,
                        
                        sum(abs(cast(basis as double))) AS "Total Cost",          
                        sum(Case when adjusted_basis = 0 then  abs(cast(basis as double)) else abs(cast(adjusted_basis as double)) end ) AS "Adjusted Total Cost"
                        
                            
                        from df
                        where len(sell_date) = 0 and len(buy_date) > 0 and num_shares > 0
                        group by Symbol      
                        

                        UNION ALL


                        select 
                        
                        Symbol AS Symbol , 
                        
                        
                        sum(num_shares) AS Quantity,
                        
                        
                        sum(abs(cast(proceeds as double)) * (-1) ) AS "Total Cost",  
                        sum(Case when cast(adjusted_basis as double) = 0 then  abs(cast(proceeds as double)) * (-1) else abs(cast(adjusted_basis as double)) *(-1) end ) AS "Adjusted Total Cost" 
                        
                        
                        from df
                        where len(sell_date) > 0 and len(buy_date) = 0  and num_shares < 0
                        group by Symbol                       

                """
                            
            ).df()

            
            return df


    def FinalDataForCalc(self) -> pd.DataFrame :

            df_c  = self.con.execute(
                
                """
                select 
                num_shares, 
                Symbol,
                description ,
                (case when num_shares > 0 then buy_date else '' end) AS buy_date,
                adjusted_buy_date,
                basis,
                adjusted_basis,
                (case when num_shares < 0 then sell_date else '' end) AS sell_date,
                proceeds,
                adjustment_code,
                adjustment,
                form_position,
                buy_lot,
                replacement_for,
                is_replacement,
                loss_processed,
                parent_lot,
                adjusted_sell_date,
                adjusted_proceeds
                from combined   

                """
            ).df()

            return df_c


    def FinalDataForChart(self,df_t : pd.DataFrame) -> pd.DataFrame :
                
                df_t  = self.con.execute(
                
                """
                select Symbol , 
                TradeDate , 
                TransactionType AS Type , 
                Sum(case when TransactionType = 'Buy' then cast(Quantity as double) else cast(Quantity as double) * (-1) end ) AS Quantity,
                Sum(cast(NetProceeds as double))  AS "Net Proceeds" ,
                (abs(Sum(cast(NetProceeds as double))) / Sum(cast(Quantity as double))) AS "Per Unit Cost"
                
                from df_t where Quantity > 0
                group by Symbol , TradeDate , TransactionType
                order by Symbol , TradeDate
                
                """
                ).df()

                return df_t


    def getremainingtran(self,df_r : pd.DataFrame) -> pd.DataFrame :
            df_r  = self.con.execute( 

                            """
                            
                            select

                            case when 
                                TransactionType = 'Sell' then  abs(cast(Quantity as double)) * (-1) 
                            else 
                                abs(cast(Quantity as double))  
                            end AS num_shares,

                            Symbol AS symbol,
                            '' AS description,

                            case when 
                                TransactionType = 'Buy' then TradeDate else '' 
                            end AS buy_date,

                            '' AS adjusted_buy_date,
                            case when 
                                TransactionType = 'Buy'  then cast(NetProceeds AS  double) 
                                else  
                                0.00 
                            end AS basis,

                            0.00 AS adjusted_basis,
                            case when 
                                TransactionType = 'Sell' then TradeDate 
                            else '' 
                            end AS sell_date,

                            case when 
                                TransactionType = 'Sell' then cast(NetProceeds AS  double) 
                            else 0.00 
                            end AS proceeds,

                            '' AS adjustment_code,
                            0.00 AS adjustment,
                        '' AS form_position,
                        '' AS buy_lot,
                        '' AS replacement_for,
                        '' AS is_replacement,
                        '' AS loss_processed,
                        '' AS parent_lot,
                        '' AS adjusted_sell_date,
                        0.00 AS  adjusted_proceeds
                            from df_r
                            where Quantity <> 0
                    """
            ).df()

            return df_r

    def GetoffsetHoldingReportdetail(self,df : pd.DataFrame ) -> pd.DataFrame :
            # abs(sum("Deffered Loss") * (-1)) AS "Deffered Loss",
            #abs(sum(Quantity)) * "Per unit cost Difference" * (-1)     AS "Carry Forward Loss"
            df = self.con.execute(f"""

                                 
                        select Symbol , 
                        sum(Quantity) AS "Available Holding Quantity" , 
                        abs(sum("Total Cost")) AS "Total Cost",
                        abs(sum("Adjusted Total Cost")) AS "Adjusted Total Cost",
                        abs(sum("Total Cost")) / abs(sum(Quantity))  AS "Per Unit Total Cost",
                        abs(sum("Adjusted Total Cost")) / abs(sum(Quantity))  AS "Per Unit adjusted Cost",
                        abs("Per Unit adjusted Cost" - "Per Unit Total Cost") AS "Per unit cost Difference",
                        abs(sum("Deffered Loss") * (-1)) AS "Carry Forward Loss"                                                                   


                        from        

                        (                
                        select 
                        
                        Symbol AS Symbol ,   
                        
                        num_shares AS Quantity,
                        abs(cast(basis as double)) AS "Total Cost",          
                        Case when adjusted_basis = 0 then  abs(cast(basis as double)) else abs(cast(adjusted_basis as double)) end AS "Adjusted Total Cost",
                        (Case when adjusted_basis = 0 then  abs(cast(basis as double)) else abs(cast(adjusted_basis as double)) end) - abs(cast(basis as double)) AS "Deffered Loss"
                            
                        from df
                        where len(sell_date) = 0 and len(buy_date) > 0 and num_shares > 0
                        and (Case when adjusted_basis = 0 then  abs(cast(basis as double)) else abs(cast(adjusted_basis as double)) end) - abs(cast(basis as double))  <> 0  

                        UNION ALL
                        
                        select 
                        
                        Symbol AS Symbol , 
                        
                        num_shares AS Quantity,
                        
                        abs(cast(proceeds as double)) * (-1) AS "Total Cost",  
                        Case when cast(adjusted_basis as double) = 0 then  abs(cast(proceeds as double)) * (-1) else abs(cast(adjusted_basis as double)) *(-1) end AS "Adjusted Total Cost" ,
                        (Case when cast(adjusted_basis as double) = 0 then  abs(cast(proceeds as double)) * (-1) else abs(cast(adjusted_basis as double)) *(-1) end) - abs(cast(proceeds as double)) * (-1) AS "Deffered Loss"  
                        
                        from df
                        where len(sell_date) > 0 and len(buy_date) = 0  and num_shares < 0
                        and (Case when cast(adjusted_basis as double) = 0 then  abs(cast(proceeds as double)) * (-1) else abs(cast(adjusted_basis as double)) *(-1) end) - abs(cast(proceeds as double)) * (-1) <> 0          
                        ) t

                        group by Symbol
                           

                """
                            
            ).df()

            
            return df


    def GetoffsetHoldingReporttotal(self,df : pd.DataFrame ) -> pd.DataFrame :

            
            df = self.con.execute(f"""

                                 
                        select Symbol , 
                        sum(Quantity) AS Quantity , 
                        sum("Total Cost") AS "Total Cost",
                        sum("Adjusted Total Cost") AS "Adjusted Total Cost",
                        sum("Deffered Loss") * (-1) AS "Deffered Loss"


                        from

                        (                
                        select 
                        
                        Symbol AS Symbol ,   
                        
                        num_shares AS Quantity,
                        abs(cast(basis as double)) AS "Total Cost",          
                        Case when adjusted_basis = 0 then  abs(cast(basis as double)) else abs(cast(adjusted_basis as double)) end AS "Adjusted Total Cost",
                        (Case when adjusted_basis = 0 then  abs(cast(basis as double)) else abs(cast(adjusted_basis as double)) end) - abs(cast(basis as double)) AS "Deffered Loss"
                            
                        from df
                        where len(sell_date) = 0 and len(buy_date) > 0 and num_shares > 0
                        and (Case when adjusted_basis = 0 then  abs(cast(basis as double)) else abs(cast(adjusted_basis as double)) end) - abs(cast(basis as double))  <> 0  

                        UNION ALL
                        
                        select 
                        
                        Symbol AS Symbol , 
                        
                        num_shares AS Quantity,
                        
                        abs(cast(proceeds as double)) * (-1) AS "Total Cost",  
                        Case when cast(adjusted_basis as double) = 0 then  abs(cast(proceeds as double)) * (-1) else abs(cast(adjusted_basis as double)) *(-1) end AS "Adjusted Total Cost" ,
                        (Case when cast(adjusted_basis as double) = 0 then  abs(cast(proceeds as double)) * (-1) else abs(cast(adjusted_basis as double)) *(-1) end) - abs(cast(proceeds as double)) * (-1) AS "Deffered Loss"  
                        
                        from df
                        where len(sell_date) > 0 and len(buy_date) = 0  and num_shares < 0
                        and (Case when cast(adjusted_basis as double) = 0 then  abs(cast(proceeds as double)) * (-1) else abs(cast(adjusted_basis as double)) *(-1) end) - abs(cast(proceeds as double)) * (-1) <> 0          
                        ) t

                        group by Symbol
                           

                """
                            
            ).df()

            
            return df


    def RealizedGLTotal(self,df : pd.DataFrame) -> pd.DataFrame :

                # case when "Buy Date" > "Sell Date" then "Sell_Id" else "Buy_Id" end  AS Id,
                #         case when "Buy Date" > "Sell Date" then ""Sell Date" else "Buy Date" end AS TradeDate,       
                ##case when strptime("Buy Date" , '%m/%d/%Y') > strptime("Sell Date" ,'%m/%d/%Y') then "Sell_Id" else "Buy_Id" end  AS Id,
                        ##case when strptime("Buy Date" , '%m/%d/%Y') > strptime("Sell Date" ,'%m/%d/%Y') then "Sell Date" else "Buy Date" end AS TradeDate,                            
                #SELECT strftime(DATE your_string, '%Y-%m-%d');

                df = self.con.execute(f"""

                        select 
                        Symbol,
                        
                        sum(Quantity) AS Quantity,
                        sum("Cost Basis") AS "Cost Basis",
                        sum(Proceeds) AS Proceeds ,
                        sum("Gain Loss") AS "Gain Loss"  

                        from                                                                                                 

                        (
                        select num_shares AS Quantity,
                        Symbol ,
                        buy_date AS "Buy Date",
                        sell_date AS "Sell Date",
                        buy_lot AS "Sell_Id",
                        parent_lot AS "Buy_Id",                            
                        basis AS "Cost Basis",
                        proceeds AS Proceeds,
                        (basis + proceeds) AS "Gain Loss"
                        
                        from df
                        where len(buy_date) <> 0 and  len(sell_date) <> 0
                        ) t
                       group by ALL                             
                                           
                """
                            
                ).df()

                return df

    def RealizedGL(self,df : pd.DataFrame) -> pd.DataFrame :

            
            
                df = self.con.execute(f"""

                        
                        select num_shares AS Quantity,
                        Symbol ,
                        buy_date AS "Buy Date",
                        sell_date AS "Sell Date",
                        buy_lot AS "Sell_Id",
                        parent_lot AS "Buy_Id",                            
                        basis AS "Cost Basis",
                        proceeds AS Proceeds,
                        (basis + proceeds) AS "Gain Loss"
                        
                        from df
                        where len(buy_date) <> 0 and  len(sell_date) <> 0
                                           
                """
                            
                ).df()

                return df

    def RealizedGLChange(self,df_c : pd.DataFrame, df_p : pd.DataFrame) -> pd.DataFrame :

                
                df = self.con.execute(f"""

                        select 
                        c."Symbol" AS Symbol, 
                        "Quantity" AS "Quantity sold", 
                        "Cost Basis" ,
                        "Proceeds", 
                        "Gain Loss", 

                        coalesce("Available Holding Quantity", 0) AS  "Prior year AvailableHoldingQuantity",

                        (case when coalesce("Available Holding Quantity", 0) <  0 then abs(coalesce("Total Cost", 0)) * (-1) 
                        else abs(coalesce("Total Cost", 0)) end)   AS "Prior Year Total Cost",

                        ( case when coalesce("Available Holding Quantity", 0) < 0 then abs(coalesce("Adjusted Total Cost",0)) * (-1) 
                        else abs(coalesce("Adjusted Total Cost",0)) end) AS "Prior Year  Adjusted total Cost",

                        coalesce("Carry Forward Loss", 0)  AS "Prior year Deferred Loss",
                        coalesce("Per Unit Total Cost", 0) AS   "Prior year  PerUnitTotalCost",
                        coalesce("Per Unit adjusted Cost", 0) AS "Prior year  PerUnitAdjustedTotalCost",
                        coalesce("Per unit cost Difference" ,0) AS "Prior year  PerUnitCostDifference",
                        (case when abs(coalesce("Available Holding Quantity", 0)) <= abs(coalesce(Quantity ,0))
                        then coalesce("Per unit cost Difference" ,0) * abs(coalesce("Available Holding Quantity", 0)) *( -1)
                        else 
                        coalesce("Per unit cost Difference" ,0) *  abs(coalesce(Quantity ,0)) * (- 1) end ) AS "Change in RLZD Gain Loss"
                        
                        from   df_c c
                        left join df_p p on c.Symbol = p.Symbol                      
                                           
                """
                            
                ).df()

                return df

    def UNRealizedGL(self,df : pd.DataFrame) -> pd.DataFrame :

            
            
                df = self.con.execute(f"""

                        select * 
                        from         
                            
                        (   
                        select num_shares AS Quantity,
                        Symbol ,
                        'Buy' AS "Transaction Type",     
                        buy_date AS "Trade Date",
                        basis AS "Cost"
                        
                        from df
                        where len(buy_date) <> 0 and  len(sell_date) = 0

                        UNION  ALL   

                        select num_shares AS Quantity,
                        Symbol ,
                        'Sell' AS "Transaction Type",     
                        sell_date AS "Trade Date",
                        proceeds AS "Cost"
                        
                        from df
                        where len(buy_date) = 0 and  len(sell_date) <> 0
                        ) T    
                                
                """
                            
                ).df()

                return df
    
    def GetAllSymbols(self, df_h : pd.DataFrame, df_t : pd.DataFrame) -> pd.DataFrame :

        if not df_h.empty :
        
            df = self.con.execute(f"""

                                
                            select Symbol 
                            
                            from df_h
                            
                            UNION  ALL   
                            
                            select Symbol 
                            
                            from df_t
                            
                                    
                    """
                                
                    ).df()

        else :
                df = self.con.execute(f"""

                            
                            select Symbol 
                            
                            from df_t
                            
                                    
                    """
                                
                    ).df()
                

        return df 