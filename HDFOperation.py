import pandas as pd

def WriteToHDF(FileNm : str ,df : pd.DataFrame, sKey : str) -> None :
    
    df.to_hdf(FileNm,key = sKey)

def ReadFromHDF(FileNm : str , sKey : str) -> pd.DataFrame :
    
    df = pd.read_hdf(FileNm, key = sKey)
    return df


