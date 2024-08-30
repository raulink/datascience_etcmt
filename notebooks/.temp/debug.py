import os
import pandas as pd

from notebooks.include._googlesheetprocessor import GoogleSheetProcessor


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


def main():
    df = pd.DataFrame()    
    current_directory = os.getcwd()
    filename =  os.path.join (current_directory,"notebooks","input","mix_plan.csv")
    print(f"La ruta completa del archivo es: {filename}") 
    df = pd.read_csv(filename)
    print(df)

#def process():    

    #if valores == "":valores = self.valores    
    #if regimen == "":regimen = self.regimen    
    if valores.keys() != regimen.keys():
        raise AssertionError(f"Las claves no coinciden: {valores.keys()} != {regimen.keys()}")

    #self.df = df.copy(deep=True)

    # Realiza el procesamiento necesario
    # Este es un lugar para incluir toda la lógica de procesamiento
    df
    # Suponiendo que el procesamiento produce 'filtered_data' y otros DataFrames
    df_plan = pd.DataFrame()  # Placeholder
    df_action = pd.DataFrame()  # Placeholder
    df_speciality = pd.DataFrame()  # Placeholder
    filtered_data = pd.DataFrame()  # Placeholder        
    
    print(valores)
    print(valores)
    ## convertir a booleano
    df[list(valores.keys())] = df[valores.keys()].applymap(lambda x: True if x == 'TRUE' else False)
    # Obtener la unidades
    parametros = regimen
    df['unidad'] = df.apply(lambda row: next((parametros[key] for key in parametros.keys() if key in row and row[key] == True), None), axis=1)
    # Obtener los valores
    parametros = valores
    df['valor'] = df.apply(lambda row: next((parametros[key] for key in parametros.keys() if key in row and row[key] == True), None), axis=1)
    # Filtrar las columnas necesarias solamente

    # Quitar planes
    df = df[df['Tipo']!= 'Plan']

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
    #df_activity_type = self.get_unique(df,"fkc_activity_type")
    #df_regime = self.get_unique(df,"fkc_regime")

    # Filter the data
    #df = df_raw.copy(deep=True)
    filtered_data = df[(df['fkc_activity_type'] == 'Actividad') | (df['fkc_activity_type'] == 'Tarea')]

    # Add fk_activity column
    filtered_data['fk_activity'] = None

    # Set fk_activity for Tareas based on their parent Actividad
    parent_index = None
    for i, row in filtered_data.iterrows():
        if row['fkc_activity_type'] == 'Actividad':
            parent_index = i
        elif row['fkc_activity_type'] == 'Tarea':
            filtered_data.at[i, 'fk_activity'] = parent_index
    #filtered_data

    filtered_data['fk_plan']= filtered_data['fk_plan'].apply(lambda x: self.buscarIndice(df_plan,x)) 

    print (filtered_data)
    #return df_plan, df_action, df_speciality, filtered_data
    #return filtered_data

if __name__ == "__main__":
    main()