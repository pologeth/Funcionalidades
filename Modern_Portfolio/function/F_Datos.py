import pandas as pd
import numpy  as np
import pickle
import yfinance as yf

from time import time
import os

# Get the directory where this file is located
current_dir = os.path.dirname(os.path.abspath(__file__))
# Get the parent directory (Modern_Portfolio)
parent_dir = os.path.dirname(current_dir)
# Define the data directory path
data_dir = os.path.join(parent_dir, 'data')

def cargarDatosSiNo(ind: bool):
    
    if ind:
        tiempo_inicial = time()
        maestro_path = os.path.join(data_dir, "IronIA", "maestro.csv")
        navs_path = os.path.join(data_dir, "IronIA", "navs.pickle")
        
        maestro = pd.read_csv(maestro_path)
        navs = dict(pickle.load(open(navs_path,"rb")))
        datosFondosDepurados = depuracionFondos(navs = navs,percentNA = 10,percentNARow= 70,limFfill = 0)
        tiempo_final = time() 
        tiempo_ejecucion = tiempo_final - tiempo_inicial
        print ('El tiempo de depuracion de la muestra fue:', tiempo_ejecucion )
    else:
        maestro_path = os.path.join(data_dir, "IronIA", "maestro.csv")
        datos_path = os.path.join(data_dir, "IronIA", "DS_datosFondosDepurados.csv")
        
        maestro = pd.read_csv(maestro_path)
        datosFondosDepurados= pd.read_csv(datos_path, index_col=0, parse_dates=True)        
    return(datosFondosDepurados)

def depuracionFondos(navs: dict, percentNA: int, percentNARow: int, limFfill: int) -> pd.DataFrame:
  

    datosFondos = pd.concat(navs.values(), ignore_index=False)
    
    datosFondos = datosFondos.pivot_table(index = "date",
                                          columns="isin",
                                          values="nav")
    
    datosFondos["fechas"] =pd.to_datetime(datosFondos.index)
    datosFondos = datosFondos[datosFondos.fechas.dt.weekday < 5]   
    datosFondos.drop(["fechas"],axis=1, inplace = True)
    datosFondos.sort_index(inplace = True)
    
    analExpl = missing_zero_values_table(datosFondos)


    analExplExAnte = analExpl

    datosFiltrados = analExpl[analExpl.iloc[:,4]<percentNA]
    
    dias_eliminar = ~(datosFiltrados.isna().sum(axis=1) > datosFiltrados.shape[1]*(percentNARow/100))
    datosFiltrados = datosFiltrados.loc[dias_eliminar,:]
    
    datosFondos_filt = datosFondos.T
    datosFondos_filt =datosFondos_filt[datosFondos_filt.index.isin(datosFiltrados.index)].T

    if limFfill == 0:
        datosFondos_filt.ffill(inplace = True)

        datosFondos_filt.bfill(inplace = True)
    else:      
        datosFondos_filt.ffill(limit = limFfill,inplace = True)

        datosFondos_filt.bfill(limit = limFfill,inplace = True)
        
    analExpl = missing_zero_values_table(datosFondos_filt)

    analExplExPost = analExpl

    # Use absolute path for saving
    output_path = os.path.join(data_dir, "IronIA", "DS_datosFondosDepurados.csv")
    datosFondos_filt.to_csv(output_path)
    
    return(datosFondos_filt, analExplExAnte, analExplExPost)

def missing_zero_values_table(df: pd.DataFrame):  
    
    zero_val = (df == 0.00).astype(int).sum(axis=0)
    mis_val = df.isna().sum()
    mis_val_percent = 100 * df.isnull().sum() / len(df)
    
    mz_table = pd.concat([zero_val, mis_val, mis_val_percent], axis=1)
    mz_table = mz_table.rename(
    columns = {0 : 'Zero Values', 1 : 'NA Values', 2 : '% of Total Values'})
    mz_table['Total Zero NA Values'] = mz_table['Zero Values'] + mz_table['NA Values']
    mz_table['% Total Zero NA Values'] = 100 * mz_table['Total Zero NA Values'] / len(df)
    mz_table['Data Type'] = df.dtypes
    mz_table = mz_table[mz_table.iloc[:,1] != 0].sort_values('% of Total Values', ascending=False).round(1)
        
    print ("El DataFrame seleccionado " + str(df.shape[1]) + " columnas y " + str(df.shape[0]) + " filas.\n"      
        "Hay " + str(mz_table.shape[0]) +
            " columnas que tienen valores NA.")

    return mz_table

def calcRetornos(activos:pd.DataFrame):

    
    activos.sort_index(inplace = True)

    retornos = np.log(activos).diff()
    retornos = retornos.iloc[1:(retornos.shape[0]),:]

    
    return(retornos)

def load_ticker_ts_df(ticker, start_date, end_date):
   
    cached_file_path = os.path.join(data_dir, f'{ticker}_{start_date}_{end_date}.pkl')
    try:
        if os.path.exists(cached_file_path):
            df = pd.read_pickle(cached_file_path)
        else:
            df = yf.download(ticker, start=start_date, end=end_date)
            if not os.path.exists(data_dir):
                os.makedirs(data_dir)
            df.to_pickle(cached_file_path)
    except FileNotFoundError:
        print(
            f'Error en el proceso de descarga de datos: {ticker}')

    return df

def load_ticker_prices_ts_df(tickers, start_date, end_date):
    
    df = pd.DataFrame()
    for ticker in tickers:
        cached_file_path = os.path.join(data_dir, f'{ticker}_{start_date}_{end_date}.pkl')

        try:
            if os.path.exists(cached_file_path):
                temp_df = pd.read_pickle(cached_file_path)
            else:
                temp_df = yf.download(ticker, start=start_date, end=end_date)
                if not os.path.exists(data_dir):
                    os.makedirs(data_dir)
                temp_df.to_pickle(cached_file_path)
            temp_df = temp_df.rename(columns={'Adj Close': ticker})[ticker]
            df = pd.concat([df, temp_df], axis=1)
        except Exception as e:
            print(f'Error descargando {ticker}: {e}')

    return df

