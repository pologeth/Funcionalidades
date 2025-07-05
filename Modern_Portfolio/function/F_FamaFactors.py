"""
Funciones de French and Fama
Miguel Garcia Cordo
Raquel Hernadez

Master MiAX
"""

"---------------------------------"
"-----Importacion de librerias----"
"---------------------------------"

# Datos
#-------------------------------------------------------------------------------------------
import pandas as pd
from dateutil.relativedelta import relativedelta

# Webscraping
#-------------------------------------------------------------------------------------------
import requests
from bs4 import BeautifulSoup

"---------------------------------"
"-----------WebScraping-----------"
"---------------------------------"

url = "http://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html"
response = requests.get(url)
soup = BeautifulSoup(response.text, 'lxml')

text_to_search = ['Fama/French 3 Factors', 'Momentum Factor (Mom)']
all_factors_text = soup.findAll('b', text=text_to_search)

home_url = "http://mba.tuck.dartmouth.edu/pages/faculty/ken.french/"
all_factor_links = []
for text in all_factors_text:
    links_for_factor = []  # Guarda todos los link para los factores
    for sib in text.next_siblings:  # Busca el proximo elemento 
        # Guardamos las URL
        if sib.name == 'b':
            bold_tags = sib
            try:
                link = bold_tags.find('a')['href']
                links_for_factor.append(link)
            except TypeError:
                pass
    csv_links   = [home_url + link for link in links_for_factor if 'csv' in link.lower()]
    txt_links   = [home_url + link for link in links_for_factor if 'txt' in link.lower()]
    factor_dict = {'factor' : text, 'csv_links' : csv_links, 'txt_links' : txt_links}
    all_factor_links.append(factor_dict)

ff3factor_dict    = dict(all_factor_links[0])
momAndOthers_dict = dict(all_factor_links[1])

"---------------------------------"
"-------------Funciones-----------"
"---------------------------------"

def famaFrench3Factor(frequency='m'):
    '''
    Devuelve los 3 Factores de Fama And French (Market Risk Premium, SMB, HML)
    La frecuencia se fija en 'm' para los meses, y 'a' para los años
    '''
    
    try:
        rows_to_skip = 3
        ff3_raw_data = ff3factor_dict['csv_links'][0]

        ff3_factors = pd.read_csv(ff3_raw_data, skiprows=rows_to_skip)
        ff3_factors.rename(columns = {ff3_factors.columns[0] : 'date_ff_factors'},inplace=True)

        # Tomamos los datos anuales
        annual_factor_index_loc = ff3_factors[ff3_factors.values == ' Annual Factors: January-December '].index

        
        if frequency == 'm':
            ff3_factors.drop(ff3_factors.index[annual_factor_index_loc[0]:], inplace=True)

            # Convertivos a datetime
            ff3_factors['date_ff_factors'] = pd.to_datetime(ff3_factors['date_ff_factors'],format='%Y%m')
            
            # Desplazamos para fin de mes
            ff3_factors['date_ff_factors'] = ff3_factors['date_ff_factors'].apply(lambda date : date + relativedelta(day = 1, months = +1, days = -1))

        elif frequency == 'a':
            # Extraemos datos anuales
            ff3_factors.drop(ff3_factors.index[:annual_factor_index_loc[0]],inplace=True)

            # Ignoramos el copyright
            ff3_factors = ff3_factors.iloc[2:-1]
            ff3_factors.reset_index(inplace=True)
            ff3_factors.drop(columns=ff3_factors.columns[0], inplace=True)

            # Espacios
            ff3_factors['date_ff_factors'] = ff3_factors['date_ff_factors'].apply(lambda x : x.strip())

            # Fechas date a datiem (valores deben de estar en int64)
            ff3_factors['date_ff_factors'] = pd.to_datetime(ff3_factors['date_ff_factors'],format='%Y').dt.year.values


        # Conversión de los factores de numérico a decimal (%)
        for col in ff3_factors.columns[1:]:ff3_factors[col] = pd.to_numeric(ff3_factors[col]) / 100

        return ff3_factors
    except Exception as error:
      print(f"Ha ocurrido un error en los 3 factores de French And Fama: {error}")



def momentumFactor(frequency='m'):
    '''
    Devuleve el factor de Momentum
    La frecuencia se fija en 'm' para los meses, y 'a' para los años
    '''
    
    try:
        rows_to_skip = 13
        mom_raw_data = momAndOthers_dict['csv_links'][0]

        mom_factor = pd.read_csv(mom_raw_data, skiprows=rows_to_skip)
        mom_factor.rename(columns = {mom_factor.columns[0] : 'date_ff_factors'},
                        inplace=True)

        # Tomamos los datos anuales
        annual_factor_index_loc = mom_factor[mom_factor.values == 'Annual Factors:'].index
        
       
        if frequency == 'm':
            # Exclude annual factor returns
            mom_factor.drop(mom_factor.index[annual_factor_index_loc[0]:], inplace=True)

            # Convertivos a datetime
            mom_factor['date_ff_factors'] = pd.to_datetime(mom_factor['date_ff_factors'], format='%Y%m')

            # Desplazamos para fin de mes
            mom_factor['date_ff_factors'] = mom_factor['date_ff_factors'].apply(lambda date : date + relativedelta(day = 1, months = +1, days = -1))

        elif frequency == 'a':
            # Extraemos datos anuales
            mom_factor.drop(mom_factor.index[:annual_factor_index_loc[0]],inplace=True)

            # Ignoramos el copyright
            mom_factor = mom_factor.iloc[3:-1]
            mom_factor.reset_index(inplace=True)
            mom_factor.drop(columns=mom_factor.columns[0], inplace=True)

            # Espacios
            mom_factor['date_ff_factors'] = mom_factor['date_ff_factors'].apply(lambda x : x.strip())

            # Convertir a datetime
            mom_factor['date_ff_factors'] = pd.to_datetime(mom_factor['date_ff_factors'], format='%Y').dt.year.values

        # Conversión de los factores de numérico a decimal (%)
        for col in mom_factor.columns[1:]:
            mom_factor[col] = pd.to_numeric(mom_factor[col]) / 100

        # Renombramos las columnas
        mom_factor.rename(columns={mom_factor.columns[1] : 'MOM'}, inplace=True)

        return mom_factor
    except Exception as error:
      print(f"Ha ocurrido un error en el factor de momentum: {error}")



def carhart4Factor(frequency='m'):
    '''
    Combina las 3 factores de Fama And French (Market Risk Premium, SMB, HML) y el factor de Momentum y los devuelve
    '''

    try:
        if frequency == 'm':
            ff3_factors = famaFrench3Factor(frequency='m')
            mom_factor = momentumFactor(frequency='m')

            carhart_4_factor = pd.merge(ff3_factors, mom_factor,
                                        on='date_ff_factors', how='left')
        elif frequency == 'a':
            ff3_factors = famaFrench3Factor(frequency='a')
            mom_factor = momentumFactor(frequency='a')

            carhart_4_factor = pd.merge(ff3_factors, mom_factor,
                                        on='date_ff_factors', how='left')

        return carhart_4_factor
    except Exception as error:
      print(f"Ha ocurrido un error en el factor de Carhart 4: {error}")



def famaFrench5Factor(frequency='m'):
    '''
    Combina los 5 factores de Fama And French (Market Risk Premium, SMB, HML, RMW, CMA) y los devuelve
    '''
    
    try:
        rows_to_skip = 3
        ff5_raw_data = ff3factor_dict['csv_links'][3]

        ff5_factors = pd.read_csv(ff5_raw_data, skiprows=rows_to_skip)
        ff5_factors.rename(columns = {ff5_factors.columns[0] : 'date_ff_factors'},inplace=True)

        # Tomamos los datos anuales
        annual_factor_index_loc = ff5_factors[ff5_factors.values == ' Annual Factors: January-December '].index

        
        if frequency == 'm':
            ff5_factors.drop(ff5_factors.index[annual_factor_index_loc[0]:], inplace=True)

            # Convertivos a datetime
            ff5_factors['date_ff_factors'] = pd.to_datetime(ff5_factors['date_ff_factors'],
                                                            format='%Y%m')
            # Desplazamos para fin de mes
            ff5_factors['date_ff_factors'] = ff5_factors['date_ff_factors'].apply(lambda date : date + relativedelta(day = 1, months = +1, days = -1))

        elif frequency == 'a':
            # Extraemos datos anuales
            ff5_factors.drop(ff5_factors.index[:annual_factor_index_loc[0]],
                            inplace=True)

            # Ignoramos el copyright
            ff5_factors = ff5_factors.iloc[2:]
            ff5_factors.reset_index(inplace=True)
            ff5_factors.drop(columns=ff5_factors.columns[0], inplace=True)

            # Espacios
            ff5_factors['date_ff_factors'] = ff5_factors['date_ff_factors'].apply(lambda x : x.strip())

            # Convertir a datetime
            ff5_factors['date_ff_factors'] = pd.to_datetime(ff5_factors['date_ff_factors'],format='%Y').dt.year.values

        # Conversión de los factores de numérico a decimal (%)
        for col in ff5_factors.columns[1:]:
            ff5_factors[col] = pd.to_numeric(ff5_factors[col]) / 100

        return ff5_factors
    except Exception as error:
      print(f"Ha ocurrido un error en el factor de Fama-French 5: {error}")
