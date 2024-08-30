# %%
import pandas as pd
import requests

class GoogleSheetProcessor:
    def __init__(self, sheet_url:str):
        self.sheet_url = sheet_url
        self.spreadsheet_id = self.extract_spreadsheet_id(sheet_url)
        self.sheet_id = self.extract_sheet_id(sheet_url)
        self.csv_export_url = self.construct_csv_export_url()


    def extract_spreadsheet_id(self, url):
        return url.split('/d/')[1].split('/')[0]

    def extract_sheet_id(self, url):
        return url.split('gid=')[1]

    def construct_csv_export_url(self):
        return f"https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}/export?format=csv&gid={self.sheet_id}"

    def download_csv(self, output_filename='temp_sheet.csv'):
        # Descarga el archivo CSV y lo guarda temporalmente
        response = requests.get(self.csv_export_url)
        response.raise_for_status()  # Asegurarse de que la solicitud fue exitosa
        with open(output_filename, 'wb') as f:
            f.write(response.content)
        return output_filename
    def get_unique(self, df: pd.DataFrame, column: str):
        """
        Obtiene un DataFrame con valores únicos de la columna 'Column', con índices ajustados.

        Returns:
        pd.DataFrame: Un DataFrame con valores únicos de la columna 'Column' y un índice ajustado.
        """
        df[column] = df[column].str.strip()
        df = df[df[column].notnull()]
        df_unique = pd.DataFrame(df[column].unique(), columns=['value'])
        df_unique.index = df_unique.index + 1
        return df_unique
    def buscarIndice(self, df:pd.DataFrame, valor,columna='value'):
        return int (df[df[columna]==valor].index[0])


    def read_csv(self, filename="temp_sheet.csv"):
        # Lee el archivo CSV usando pandas
        self.df = pd.read_csv(filename)
        self.df.columns = self.df.loc[2, :].to_list()  # la fila 2 como fila
        self.df = self.df.loc[4:, :]   # Obtener desde la fila 4 en adelante
        return self.df

    def process_data(self,filename = "temp_sheet.csv",valores="", regimen=""):        
        pass
        #return df_plan, df_action, df_speciality, filtered_data
            


# %%
def read_csv( filename="temp_sheet.csv"):
    # Lee el archivo CSV usando pandas
    df = pd.read_csv(filename)
    df.columns = df.loc[2, :].to_list()  # la fila 2 como fila
    df = df.loc[4:, :]   # Obtener desde la fila 4 en adelante
    return df

# Combinacion de dataframes
gs1 = GoogleSheetProcessor("https://docs.google.com/spreadsheets/d/1oUHkuKpHtuhMirNW6SvAQ4A0ns5PZs71iZ_WFXZHNn8/edit?gid=1115106678#gid=1115106678")
archivo = "input/pad.csv"
#gs1.download_csv(archivo)
df1 = gs1.read_csv(archivo)
df1.to_csv("input/process_pad.csv")

# Si existen columnas con nombres duplicados, arrojar un error de columnas duplicadas
if df1.columns.duplicated().any():
    assert False, "Hay columnas duplicadas en el DataFrame df1"

# Realizar el procesamiento, elimina columnas, con los nombres de lineas
columnas_eliminar = ['RO', 'AM', 'AZ', 'MO']
df = df1.copy(deep=True)
columns_to_drop = columnas_eliminar  # Eliminar columnas de lineas innecesarias
df = df.drop(columns=columns_to_drop, errors='ignore')
# Eliminar las filas donde la columna 'TIPO' tenga los valores 'Sistema' o 'Subsistema'
df = df[~df['Tipo'].isin(['Sistema', 'Subsistema'])]
df['Tipo'].value_counts()

# %%
# Filtrar el dataframe para que solo contenga filas donde la columna 'Plan' no sea nula
df = df[df['Plan'].notna()]
df['Tipo'].value_counts()

# %%
def convertir_booleano(df:pd.DataFrame, ini , end ):
    # Convertir a tipo booleano las columnas a partir de la columna 'AMEF'
    try:
        # Aplicar la función lambda a cada columna a partir de 'AMEF'    
        for col in df.columns[ini + 1:end]:            
            df[col] = df[col].map(lambda x: True if x == 'TRUE' else False)
    except KeyError:
        print("Columna 'AMEF' no encontrada")
    return df

amef_index = df.columns.get_loc('AMEF')
end = len(df.columns)

df_boolean = convertir_booleano(df,amef_index,end)
df_boolean.head()

# %%
import pandas as pd

# Crear un dataframe vacío para almacenar los resultados
df_salida = pd.DataFrame(columns=['Tag', 'Location','Plan'])

# Iterar sobre las filas del dataframe
for index, row in df_boolean.iterrows():
    # Buscar el índice de la columna 'AMEF'
    try:
        amef_index = df_boolean.columns.get_loc('AMEF')
    except KeyError:
        continue
    
    # Iterar a partir de la columna siguiente a 'AMEF'
    for col in df_boolean.columns[amef_index + 1:]:
        #print(col)
        if row[col] == True:  # Si el valor es True
            # Adicionar una nueva fila al dataframe de salida
            new_row = pd.DataFrame({
                'Tag': [row['Tag']],
                'Location': [col],
                'Plan': [row['Plan']]                
            })
            df_salida = pd.concat([df_salida, new_row], ignore_index=True)

# Mostrar las primeras filas del dataframe de salida
df_salida.head()


# %%
# convertir en booleano
#df.iloc[:, amef_index + 1:] = df.iloc[:, amef_index + 1:].astype(bool)
# Sumar la cantidad de valores verdaderos que existen
cantidad_activos = df.iloc[:, amef_index + 1:].sum().sum()
print(f"Existen {cantidad_activos} activos en la hoja de datos")

if len(df_salida) != cantidad_activos:
    assert False, "La cantidad de activos no corresponde con la salida"


# %%
import pandas as pd
import sqlalchemy

# Crear un engine de SQLAlchemy para conectarse a la base de datos
engine = sqlalchemy.create_engine('postgresql://postgres:postgres@localhost/simyo')

# Cargar la tabla completa en un DataFrame
query = """
SELECT
	base."id", 
	"structure".tag, 
	locations.location_code, 
	"plans"."name", 
	base.fk_plan
FROM
	base
	INNER JOIN
	locations
	ON 
		base.fk_location = locations."id"
	INNER JOIN
	"structure"
	ON 
		base.fk_structure = "structure"."id"
	LEFT JOIN
	"plans"
	ON 
		base.fk_plan = "plans"."id"
"""

df_base = pd.read_sql_query(query, engine)
#df_base = pd.read_sql_table('base', engine)
df_base.index = df_base['id'].values
# Mostrar las primeras filas del DataFrame para verificar
df_base.head()

# %%
# Renombrar los nombres de las columnas requeridas
df_salida.rename(columns={'Tag':'tag','Location':'location_code'},inplace=True)
df_salida.head()

# %%
# Realizar el join
df_result = pd.merge(df_salida, df_base, on=['tag', 'location_code'], how='left',suffixes=('_left','_right'))
# Convertir el tipo de dato de la columna id en entero
df_result['id'] = df_result['id'].convert_dtypes(int)
# Seleccionar solamnte las columnas aqe se requieren ser mostradas
df_result = df_result[[ 'id','tag', 'location_code', 'Plan','name','fk_plan']]
#df_result = df_result[[ 'id','tag', 'location_code', 'Plan']]

# %%
try:
    # Supongamos que df_result es tu DataFrame
    df_result.to_excel("output/SalidaEquipos.xlsx")
    print("Archivo guardado exitosamente.")
except PermissionError:
    print("Error: No se pudo guardar el archivo. Asegúrate de que el archivo no esté abierto en otra aplicación y que tengas permisos de escritura en la carpeta de destino.")
except Exception as e:
    print(f"Ocurrió un error inesperado: {e}")

# %%


# %%


# %%
# Realizar el merge de ambos planes en un solo dataframe
df_merged = pd.concat([df1,df2],ignore_index=True).reset_index(drop=True)
df_merged.to_csv("mix_plan.csv")


# %%
#gs1.process_data(filename="mix_plan.csv",valores=valores,regimen=regimen)
# Diccionarios originales
valores = {
    "D": 1,
    "S": 1,
    "2S": 2,
    "M": 5,
    "MC": 1,
    "2M": 2,
    "T": 3,
    "SE": 6,
    "8M": 8,
    "A": 1,
    "1.5A": 18,
    "2A": 2,
    "3A": 3,
    "4A": 4,
    "5A": 5,
    "6A": 6,
    "8A": 8,
    "10A": 10,
    "1000": 1000,
    "1300": 1300,
    "1800": 1800,
    "6000": 6000,
    "22500": 6000,
    "40000": 40000,
    "55000": 55000,
    "55000C": 55000
}

regimen = {
    "D": 'dia',
    "S": 'semana',
    "2S": 'semana',
    "M": 'semana',
    "MC": 'mes',
    "2M": 'mes',
    "T": 'mes',
    "SE": 'mes',
    "8M": 'mes',
    "A": 'Año',
    "1.5A": 'mes',
    "2A": 'Año',
    "3A": 'Año',
    "4A": 'Año',
    "5A": 'Año',
    "6A": 'Año',
    "8A": 'Año',
    "10A": 'Año',
    "1000": 'horas',
    "1300": 'horas',
    "1800": 'horas',
    "6000": 'horas',
    "22500": 'horas',
    "40000": 'horas',
    "55000": 'horas',
    "55000C": 'ciclos'
}
#df_merged.tail(5)

# %%
# Combinacion de dataframes
gs1 = GoogleSheetProcessor ("https://docs.google.com/spreadsheets/d/1oUHkuKpHtuhMirNW6SvAQ4A0ns5PZs71iZ_WFXZHNn8/edit?gid=1199302294")
archivo = "input/pad.csv"
#gs.download_csv(archivo)
df1 = gs1.read_csv(archivo)

gs2 = GoogleSheetProcessor("https://docs.google.com/spreadsheets/d/1yOaSeqRBr1FW6tvFMi_Y-s4011cKBoyiWU5dTMlujrU/edit?gid=1199302294")
archivo = "input/pbd.csv"
#gs.download_csv(archivo)
df2 = gs2.read_csv(archivo)
# Realizar el merge de ambos planes en un solo dataframe
df_merged = pd.concat([df1,df2],ignore_index=True).reset_index(drop=True)

filename = "input/mix_plan_2.csv"
df_merged.to_csv(filename)






# %%
## Script 2
# https://ipywidgets.readthedocs.io/en/latest/examples/Widget%20List.html
import warnings
import pandas as pd
import ipywidgets as widgets
from ipywidgets import Button, Layout
from IPython.display import display

#warnings.simplefilter(action='ignore', category=FutureWarning)

lad = widgets.Textarea(value='https://docs.google.com/spreadsheets/d/1oUHkuKpHtuhMirNW6SvAQ4A0ns5PZs71iZ_WFXZHNn8/edit?gid=1199302294',placeholder='Plan Maestro LAD',description='Lineas Alta Demanda:',disabled=False,layout=Layout(width='70%',height="200px"))
lbd = widgets.Textarea(value='https://docs.google.com/spreadsheets/d/1yOaSeqRBr1FW6tvFMi_Y-s4011cKBoyiWU5dTMlujrU/edit?gid=1199302294',placeholder='Plan Maestro LBD',description='Lineas Baja Demanda:',disabled=False,layout=Layout(width='70%',height="200px"))
host = widgets.Text(value='192.168.100.50',placeholder='Host',description='Host:',disabled=False)
basedatos = widgets.Text(value='simyo2',placeholder='BaseDatos',description='BaseDatos',disabled=False)
usuario = widgets.Text(value='mantto',description='Usuario')
password = widgets.Password(value='Sistemas0',description='Password')
button1 = widgets.Button(description="Generar Archivo Excel",button_style='success',layout=Layout(width='20%'))
button2 = widgets.Button(description="Cargar en Base de datos",button_style='danger',layout=Layout(width='20%'))
output = widgets.Output()
salida = widgets.Text(value="output/SalidaEquipos.xlsx",description="Nombre:",disabled=False)
accordion = widgets.Accordion(children=[ salida], titles=(['Archivo Salida']))
accordion1 = widgets.Accordion(children=[ usuario,password,host,basedatos], titles=('Usuario','Password','Host','Base de Datos'))

display(lad,lbd,host,usuario,password,accordion,button1, accordion1,button2,output)

def on_button_clicked(b):    
    # Combinacion de dataframes
    gs1 = GoogleSheetProcessor(lad.value) #("https://docs.google.com/spreadsheets/d/1oUHkuKpHtuhMirNW6SvAQ4A0ns5PZs71iZ_WFXZHNn8/edit?gid=1199302294")
    archivo = "input/pad.csv"
    #gs.download_csv(archivo)
    df1 = gs1.read_csv(archivo)

    gs2 = GoogleSheetProcessor (lbd.value) #("https://docs.google.com/spreadsheets/d/1yOaSeqRBr1FW6tvFMi_Y-s4011cKBoyiWU5dTMlujrU/edit?gid=1199302294")
    archivo = "input/pbd.csv"
    #gs.download_csv(archivo)
    df2 = gs2.read_csv(archivo)
    # Realizar el merge de ambos planes en un solo dataframe
    df_merged = pd.concat([df1,df2],ignore_index=True).reset_index(drop=True)
    
    filename = "input/mix_plan.csv"
    df_merged.to_csv(filename)
    
    #df_plan, df_action, df_speciality, filtered_data = gs1.process_data(filename=filename,valores=valores,regimen=regimen)      
######################    
    
#####################

    df_plan, df_action, df_speciality, filtered_data = gs1.process_data(filename=filename,valores=valores,regimen=regimen)  
    with pd.ExcelWriter("output/"+salida.value) as writer:
        df_action.to_excel(writer, sheet_name='actions')
        df_plan.to_excel(writer, sheet_name='plans')
        df_speciality.to_excel(writer, sheet_name='specialties')
        filtered_data.to_excel(writer, sheet_name='activities')
    
    # gs1.save_to_excel(output_path=salida.value,valores=valores,regimen=regimen,filename="mix_plan.csv")        
    with output:
        print("Se Genera archivo excel Salida.xlsx")

button1.on_click(on_button_clicked)
button2.on_click(lambda _: print("Boton 2 accionado"))
#https://ipywidgets.readthedocs.io/en/7.6.3/examples/Widget%20Styling.html
## Script 2


