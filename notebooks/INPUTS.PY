import pandas as pd
import mysql.connector

# Cargar los datos del archivo Excel ya limpio
df = pd.read_excel('INPUT/INGRESOS AGOSTO.xlsx', skiprows=6)
df.columns = ['codigo_ingreso', 'fecha_ingreso', 'motivo_ingreso', 'gerencia_solicitante', 
              'departamento_solicitante', 'ubicacion', 'tipo_gasto', 'partida_presupuestaria', 
              'proveedor', 'codigo_item', 'descripcion', 'unidad_medida', 'grupo', 'sub_grupo', 
              'cantidad', 'precio_unitario']

# Configurar la conexión a la base de datos MySQL
connection = mysql.connector.connect(
    host='tu_host',       # Cambia por tu host
    user='tu_usuario',    # Cambia por tu usuario
    password='tu_contraseña',  # Cambia por tu contraseña
    database='tu_base_de_datos'  # Cambia por tu base de datos
)

cursor = connection.cursor()

# Insertar los datos en la tabla `ingreso`
insert_query = """
INSERT INTO ingreso (codigo_ingreso, fecha_ingreso, motivo_ingreso, gerencia_solicitante, 
                     departamento_solicitante, ubicacion, tipo_gasto, partida_presupuestaria, 
                     proveedor, codigo_item, descripcion, unidad_medida, grupo, sub_grupo, 
                     cantidad, precio_unitario) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Iterar sobre las filas del dataframe e insertar cada una en la base de datos
for index, row in df.iterrows():
    cursor.execute(insert_query, (
        row['codigo_ingreso'], row['fecha_ingreso'], row['motivo_ingreso'], row['gerencia_solicitante'],
        row['departamento_solicitante'], row['ubicacion'], row['tipo_gasto'], row['partida_presupuestaria'],
        row['proveedor'], row['codigo_item'], row['descripcion'], row['unidad_medida'], row['grupo'], 
        row['sub_grupo'], row['cantidad'], row['precio_unitario']
    ))

# Confirmar la transacción
connection.commit()

# Cerrar la conexión
cursor.close()
connection.close()

print("Datos insertados exitosamente.")