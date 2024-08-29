import pandas as pd
import requests

class GoogleSheetProcessor:
    def __init__(self, sheet_url:str):
        self.sheet_url = sheet_url
        self.spreadsheet_id = self.extract_spreadsheet_id(sheet_url)
        self.sheet_id = self.extract_sheet_id(sheet_url)
        self.csv_export_url = self.construct_csv_export_url()

        # Diccionarios originales
        self.valores = {
            "D": 1, "S": 1, "M": 5, "MC": 1, "2M": 2, "T": 3, "4M": 4, "SE": 6,
            "8M": 8, "A": 1, "1.5A": 18, "2A": 2, "3A": 3, "4A": 4, "5A": 5,
            "6A": 6, "8A": 8, "10A": 10, "1000": 1000, "6000": 6000, "22500": 22500,
            "40000": 40000, "55000": 55000
        }

        self.regimen = {
            "D": 'dia', "S": 'semana', "M": 'semana', "MC": 'mes', "2M": 'mes', "T": 'mes',
            "4M": 'mes', "SE": 'mes', "8M": 'mes', "A": 'Año', "1.5A": 'mes', "2A": 'Año',
            "3A": 'Año', "4A": 'Año', "5A": 'Año', "6A": 'Año', "8A": 'Año', "10A": 'Año',
            "1000": 'horas', "6000": 'horas', "22500": 'horas', "40000": 'horas', "55000": 'horas'
        }

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
    
    def process_data_with_validation(self, df: pd.DataFrame, valores: dict):
        # Iterar sobre las claves del diccionario valores
        for col in valores.keys():
            # Verificar si la columna existe en el DataFrame
            if col in df.columns:
                # Comprobar si la columna no es booleana
                if not pd.api.types.is_bool_dtype(df[col]):
                    # Si no es booleana, intentamos convertirla
                    df[col] = df[col].apply(lambda x: True if str(x).upper() == 'TRUE' else False)
                    # df[list(valores.keys())] = df[valores.keys()].applymap(lambda x: True if x == 'TRUE' else False)
            else:
                print(f"La columna '{col}' no se encuentra en el DataFrame.")
        return df




    def process_data(self,filename = "temp_sheet.csv",valores="", regimen=""):        
        df = pd.DataFrame()
        df = pd.read_csv(filename)

        #if valores == "":valores = self.valores    
        #if regimen == "":regimen = self.regimen
        
        if valores.keys() != regimen.keys():
            raise AssertionError(f"Las claves no coinciden: {valores.keys()} != {regimen.keys()}")

        #self.df = df.copy(deep=True)

        # Realiza el procesamiento necesario
        # Este es un lugar para incluir toda la lógica de procesamiento
        
        # Suponiendo que el procesamiento produce 'filtered_data' y otros DataFrames
        df_plan = pd.DataFrame()  # Placeholder
        df_action = pd.DataFrame()  # Placeholder
        df_speciality = pd.DataFrame()  # Placeholder
        filtered_data = pd.DataFrame()  # Placeholder        
        #print(valores)
        ## convertir a booleano
        # df[list(valores.keys())] = df[valores.keys()].applymap(lambda x: True if x == 'TRUE' else False)
        ## convertir a booleano
        df = self.process_data_with_validation(df,valores)
        print(df.dtypes)

        # Quitar planes
        df = df[df['Tipo']!= 'Plan']

        # Obtener la unidades
        parametros = regimen
        df['unidad'] = df.apply(lambda row: next((parametros[key] for key in parametros.keys() if key in row and row[key] == True), None), axis=1)
        # Obtener los valores
        parametros = valores
        df['valor'] = df.apply(lambda row: next((parametros[key] for key in parametros.keys() if key in row and row[key] == True), None), axis=1)
        # Filtrar las columnas necesarias solamente
        print("Valores unicos en unidades: ")
        print(df['unidad'].unique())        
        # Mantener solo las columnas necesarias
        columns = ['Plan','Accion','Trabajo','Actividad','Tipo','Parada','Relevancia','Especialidad','valor','unidad']
        df = df[columns]
        # Crear la nueva columna fk_activity que tendra relaciones con las actividades padre
        df['fk_activity']= None
        df['fkc_regime']= None

        # renombrar los nombres de las columnas
        nuevos_nombres = {
            'Plan': 'fk_plan',
            'Accion': 'fk_action',
            'Actividad': 'name',
            'Tipo': 'fkc_activity_type',
            'Relevancia': 'fkc_priority',
            'Especialidad': 'fk_specialty',
            'valor': 'time_interval_value',
            'unidad': 'fk_periodicity_unit',
            'Parada': 'stoppage',
        }
        df.rename(columns=nuevos_nombres, inplace=True)
                # Mantener las columnas del excel en el orden indicado
        columnas_excel = ['fk_activity','fk_plan','fk_action','name','fkc_activity_type','fkc_priority','fk_specialty','fkc_regime','stoppage','time_interval_value','fk_periodicity_unit'] 

        df = df[columnas_excel]
        df_plan = self.get_unique(df,"fk_plan")
        df_action = self.get_unique(df,"fk_action")
        df_speciality = self.get_unique(df,"fk_specialty")
        df_activity_type = self.get_unique(df,"fkc_activity_type")
        df_regime = self.get_unique(df,"fkc_regime")

        # Filter the data
        #df = df_raw.copy(deep=True)
        filtered_data = df[(df['fkc_activity_type'] == 'Actividad') | (df['fkc_activity_type'] == 'Tarea')]

        # Add fk_activity column
        filtered_data['fk_activity'] = None

        # Buscar fk_activity para las Tareas que provienen de una Actividad
        parent_index = None
        for i, row in filtered_data.iterrows():
            if row['fkc_activity_type'] == 'Actividad':
                parent_index = i
            elif row['fkc_activity_type'] == 'Tarea':
                filtered_data.at[i, 'fk_activity'] = parent_index

        filtered_data['fk_plan']= filtered_data['fk_plan'].apply(lambda x: self.buscarIndice(df_plan,x)) 
        return df_plan, df_action, df_speciality, filtered_data   


    def save_to_excel(self, output_path="Salida.xlsx",filename="mix_plan.csv",valores="",regimen=""):
        df_plan, df_action, df_speciality, filtered_data = self.process_data(filename=filename,valores=valores,regimen=regimen)  
        with pd.ExcelWriter(output_path) as writer:
            df_action.to_excel(writer, sheet_name='actions')
            df_plan.to_excel(writer, sheet_name='plans')
            df_speciality.to_excel(writer, sheet_name='specialties')
            filtered_data.to_excel(writer, sheet_name='activities')