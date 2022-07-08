from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import functions as f, types as t
from pyspark.sql.functions import  when
from datetime import datetime

# -------- Constantes de configuración

path_jar_driver = r'mysql-connector-java-8.0.29.jar'
db_user = 'Estudiante_15'
db_psswd = '4UBA5050J2'
source_db_connection_string  = 'jdbc:mysql://157.253.236.116:8080/WWImportersTransactional'
dest_db_connection_string = 'jdbc:mysql://157.253.236.116:8080/Estudiante_15'

# --------Configuración de la sesión

conf=SparkConf() \
    .set('spark.driver.extraClassPath', path_jar_driver)

spark_context = SparkContext(conf=conf)
sql_context = SQLContext(spark_context)
spark = sql_context.sparkSession


## ------- Funciones Generales

def obtener_dataframe_de_bd(db_connection_string, sql, db_user, db_psswd):
    df_bd = spark.read.format('jdbc')\
        .option('url', db_connection_string) \
        .option('dbtable', sql) \
        .option('user', db_user) \
        .option('password', db_psswd) \
        .option('driver', 'com.mysql.cj.jdbc.Driver') \
        .load()
    return df_bd

def guardar_db(db_connection_string, df, tabla, db_user, db_psswd):
    df.select('*').write.format('jdbc') \
      .mode('append') \
      .option('url', db_connection_string) \
      .option('dbtable', tabla) \
      .option('user', db_user) \
      .option('password', db_psswd) \
      .option('driver', 'com.mysql.cj.jdbc.Driver') \
      .save()

print("------ INICIA ETL DE PROVEEDORES -----")
# -------- Extracción Data de Proveedores
sql_proveedores = '''(
SELECT 
	P.ProveedorID AS ID_Proveedor, 
	P.NombreProveedor AS Nombre,  
	P.DiasPago AS Dias_pago , 
	P.CodigoPostal AS Codigo_postal, 
	P.CategoriaProveedorID AS ID_Categoria,
	P.PersonaContactoPrincipalID AS ID_Persona
FROM 
	WWImportersTransactional.Proveedores as P
	) AS TMP_PROVEEDOR'''
proveedores = obtener_dataframe_de_bd(source_db_connection_string, sql_proveedores, db_user, db_psswd)
proveedores.show(5)

# -------- Extracción Data de Categoria Proveedores
sql_categorias_proveedores = '''
(
 SELECT
	C.CategoriaProveedor AS Categoria, 
	C.CategoriaProveedorID AS ID_Categoria 
FROM 
	WWImportersTransactional.CategoriasProveedores AS C
) AS TMP_CATEGORIA_PROVEEDORES'''
categorias_proveedores = obtener_dataframe_de_bd(source_db_connection_string, sql_categorias_proveedores, db_user, db_psswd)
categorias_proveedores.show(5)

# -------- Extracción Data de Personas Contacto
sql_personas_contacto = '''
(
 SELECT
	P.ID_persona as ID_Persona,
    P.NombreCompleto AS Contacto_principal
FROM 
	WWImportersTransactional.Personas AS P
) AS TMP_PERSONAS_CONTACTO'''
personas_contacto = obtener_dataframe_de_bd(source_db_connection_string, sql_personas_contacto, db_user, db_psswd)
personas_contacto.show(5)

# -------- Transformación para unir registros y eliminar duplicados
proveedores = proveedores.join(categorias_proveedores, how='inner', on = 'ID_Categoria')
proveedores.show(5)

proveedores = proveedores.join(personas_contacto, how='inner', on = 'ID_Persona')
proveedores.show(5)

proveedores = proveedores.drop('ID_Categoria')
proveedores = proveedores.drop('ID_Persona')

proveedores.withColumn("Dias_pago", when(proveedores.Dias_pago < 0,proveedores.Dias_pago * -1).otherwise(proveedores.Dias_pago))

proveedores = proveedores.drop_duplicates()

proveedores.show(5)


# --------  Load - Carga de los datos de Proveedor
guardar_db(dest_db_connection_string, proveedores,'Estudiante_15.Proveedor', db_user, db_psswd)
print("------ FINALIZA ETL DE PROVEEDORES -----")


print("------ INICIA ETL DE TIPOS DE TRANSACCION -----")
# -------- Extracción Data de Tipo Transacciones
sql_tipos_transacciones = '''(
SELECT 
	TT.TipoTransaccionID AS ID_Tipo_transaccion,
    TT.TipoTransaccionNombre AS Tipo
FROM 
	WWImportersTransactional.TiposTransaccion as TT
	) AS TMP_TIPOS_TRANSACCION'''
tipos_transacciones = obtener_dataframe_de_bd(source_db_connection_string, sql_tipos_transacciones, db_user, db_psswd)
tipos_transacciones.show(5)

# --------  Load - Carga de los datos de Tipos de Transacciones
guardar_db(dest_db_connection_string, tipos_transacciones,'Estudiante_15.TipoTransaccion', db_user, db_psswd)

print("------ FINALIZA ETL DE TIPOS DE TRANSACCION -----")


print("------ INICIA ETL DE MOVIMIENTOS -----")
# -------- Extracción Data de Movimientos
sql_movimientos = '''(
SELECT 
	M.StockItemTransactionID AS ID_proveedor,
    M.StockItemID AS ID_Producto,
    M.TransactionTypeID AS ID_Tipo_transaccion,
    M.CustomerID AS ID_Cliente,
    M.Quantity as Cantidad,
    M.TransactionOccurredWhen as Fecha_movimiento
FROM 
	WWImportersTransactional.movimientos as M
	) AS TMP_MOVIMIENTOS'''
movimientos = obtener_dataframe_de_bd(source_db_connection_string, sql_movimientos, db_user, db_psswd)
movimientos.show(5)

# -------- Transformación para datos de movimientos
# TRANSFORMACION

# Ajuste de fecha
regex = "([0-2]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1]))"
cumplenFormato = movimientos.filter(movimientos["Fecha_movimiento"].rlike(regex))
noCumplenFormato = movimientos.filter(~movimientos["Fecha_movimiento"].rlike(regex))
print(noCumplenFormato.count(), cumplenFormato.count())
print(noCumplenFormato.show(5))
noCumplenFormato = noCumplenFormato.withColumn('Fecha_movimiento', f.udf(lambda d: datetime.strptime(d, '%b %d,%Y').strftime('%Y-%m-%d'), t.StringType())(f.col('Fecha_movimiento')))
movimientos = noCumplenFormato.union(cumplenFormato)
noCumplenFormato.count(), movimientos.count()
## Eliminar duplicados

movimientos = movimientos.drop_duplicates()
## Filtrar por cantidad de movimientos viaje
movimientos = movimientos.filter(movimientos["Cantidad"] < 5000000)
movimientos.show(5)

# --------  Load - Carga de los datos de Movimientos
guardar_db(dest_db_connection_string, movimientos,'Estudiante_15.Movimiento', db_user, db_psswd)

print("------ FINALIZA ETL DE MOVIMIENTOS -----")
