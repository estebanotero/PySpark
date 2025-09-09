#!/usr/bin/env python
# coding: utf-8

# ## PySpark y Spark SQL
# 
# New notebook

# # Uso PySpark con Tablas Delta y Archivos Parquet

# In[1]:


# Mapeo el path abfss de la tabla que voy a consultar (puede de ser de este u otro workspace)

workspace= '7155916d-4bfe-4f62-a98d-6d96db5ff211'
dfs= '@onelake.dfs.fabric.microsoft.com'
lakehouse= '4051f8c5-adbc-47a6-9592-24a1151389f0'
schema= 'masters'
table_name= 'drivers'

path= 'abfss://' + workspace + dfs + '/' + lakehouse + '/Tables/' + schema + '/' + table_name

print(path)


# In[2]:


# Armo dataframe leyendo tabla delta
df_drivers= spark.read.load(path)

display(df_drivers)


# In[6]:


# Guardo el dataframe como una tabla delta 
relative_path= 'Tables/f1/drivers'
df_drivers.write.mode("overwrite").format("delta").save(relative_path)


# In[3]:


# Guardo el dataframe en Onelake 
relative_path= 'Files/F1/drivers'

df_drivers.write.mode("overwrite").format("parquet").save(relative_path)


# Puedo armar dataframes haciendo queries a las tablas delta

# In[4]:


# Armo dataframe leyendo tabla delta
df_calendario= spark.sql('SELECT * FROM LH_Capacitaciones.masters.calendario order by Fecha')

# Muestro el dataframe
display(df_calendario)


# In[5]:


# filtro los años 2020 y 2022
df_2020_2022= df_calendario.filter(df_calendario["Year"].isin(2020, 2022))

display(df_2020_2022)


# In[7]:


# Mostrar los tipos de datos de cada columna

print("Tipos de datos con dtypes:")
df_drivers.printSchema()


# Armo dataframe leyendo un csv

# In[ ]:


df_csv= spark.read


# In[18]:


path_csv= 'Files/Csv/btc.csv'

df_btc = spark.read.format("csv").option("header","true").option("inferSchema", True).load(path_csv)

display(df_btc)


# In[12]:


df_btc.printSchema()


# In[15]:


from pyspark.sql import functions as F

df_btc = (df_btc
    .withColumn("Fecha", F.to_date(F.col("snapped_at")))
    .withColumn("price", F.col("price").cast("double"))
    .withColumn("market_cap", F.col("market_cap").cast("double"))
    .withColumn("total_volume", F.col("total_volume").cast("double"))
        )


# In[16]:


df_btc.printSchema()


# Guardo un dataframe como un CSV

# In[25]:


# Convierto en dataframe de Spark a Pandas
path= 'abfss://116036f4-f54e-4111-b8da-ab15d521c465@onelake.dfs.fabric.microsoft.com/2438b443-392a-4ff1-a77e-3c28c2480e2a/Files/Csv/drivers.csv'

df_drivers.toPandas().to_csv(path, index=False)


# In[32]:


# Convierto en dataframe de Spark a Pandas
path= 'abfss://116036f4-f54e-4111-b8da-ab15d521c465@onelake.dfs.fabric.microsoft.com/2438b443-392a-4ff1-a77e-3c28c2480e2a/Files/Csv/calendario.csv'

df_calendario.toPandas().to_csv(path, index=False)


# Armo dataframe leyendo un json

# In[9]:


path_json = 'Files/Json/tc-2024.json'

df_tc_2024 = spark.read.option("multiline", "true").json(path_json)

display(df_tc_2024)


# Parseo el json

# In[17]:


from pyspark.sql.functions import col, lit, struct
from pyspark.sql import functions as F

# Lista de monedas que quieres extraer
monedas = ['ARS', 'BRL', 'EUR', 'CLP', 'PYG', 'UYU', 'COP', 'MXN', 'PEN']

# Obtener las claves del struct (fechas) desde el esquema
fechas = df_tc_2024.select("rates").schema[0].dataType.fieldNames()

# Inicializamos una lista para almacenar los DataFrames parciales
dataframes = []

# Iteramos por cada fecha en las claves del struct
for fecha in fechas:
    # Seleccionamos la base y las tasas de la fecha actual
    df_tmp = df_tc_2024.select(
        col("base"),
        lit(fecha).alias("Fecha"),  # Usamos la fecha como una nueva columna
        *[col(f"rates.`{fecha}`.{moneda}").alias(moneda) for moneda in monedas]
    )
    
    # Añadimos el DataFrame temporal a la lista
    dataframes.append(df_tmp)

# Unir los DataFrames parciales uno por uno
df_unido = dataframes[0]
for df in dataframes[1:]:
    df_unido = df_unido.unionByName(df)

# Agregar la columna del año
df_final = df_unido.withColumn("year", F.year("Fecha"))

# Mostrar el resultado
display(df_final)


# In[27]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# -- Leo un Json apuntando al abfss 
# select *
# from json.`abfss://DP-600@onelake.dfs.fabric.microsoft.com/LH_Capacitaciones.Lakehouse/Files/Json/tc-2024.json`


# Guardo dataframes en Onelake como json

# In[35]:


# Convierto a Pandas y uso abfss
onelake_path= 'abfss://116036f4-f54e-4111-b8da-ab15d521c465@onelake.dfs.fabric.microsoft.com/2438b443-392a-4ff1-a77e-3c28c2480e2a/Files/Json/drivers.json'

df_drivers.toPandas().to_json(onelake_path, orient="records")


# In[36]:


# Convierto a Pandas y uso abfss
onelake_path= 'abfss://116036f4-f54e-4111-b8da-ab15d521c465@onelake.dfs.fabric.microsoft.com/2438b443-392a-4ff1-a77e-3c28c2480e2a/Files/Json/calendario.json'

df_calendario.toPandas().to_json(onelake_path, orient="records")


# Puedo armar tablas temporales con los dataframes

# In[38]:


# Armo tabla temporal con dataframe y usarlos con Spark SQL

df_drivers.createOrReplaceTempView('temp_drivers')


# # Uso Spark SQL con tablas Delta y tablas temporales

# In[39]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT *
# from temp_drivers
# Limit 5


# Puedo llamar a tablas delta especificando el Lakehouse y el esquema

# In[41]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT *
# FROM LH_Capacitaciones.tc.TC
# where Fecha >= date_add(current_date,-30)
# order by Fecha DESC


# In[42]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# -- Creo tabla delta con una query sobre la tabla delta

# CREATE TABLE IF NOT EXISTS LH_Capacitaciones.dbo.tc_2020_2022
# As
# SELECT *
# FROM LH_Capacitaciones.tc.tc
# where Fecha between '2020-01-01' and '2022-12-31'


# In[43]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# -- Creo tabla delta con una query sobre la tabla delta

# CREATE TABLE IF NOT EXISTS LH_Capacitaciones.dbo.calendario_2020_2022
# As
# SELECT *
# FROM LH_Capacitaciones.masters.calendario
# where Fecha between '2020-01-01' and '2022-12-31'


# In[44]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT Year,
#        count(*)
# FROM LH_Capacitaciones.dbo.calendario_2020_2022
# GROUP BY Year


# In[45]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# -- Borro datos de una tabla Delta
# delete from LH_Capacitaciones.dbo.calendario_2020_2022
# where Fecha <= '2020-12-31'


# In[46]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# Update LH_Capacitaciones.dbo.calendario_2020_2022
# set 
#     Year= 9999
# where Fecha = '2022-07-08'


# In[47]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# -- Puedo ver las diferentes versiones de la tabla Delta

# DESCRIBE history LH_Capacitaciones.dbo.calendario_2020_2022


# In[48]:


# Si especifico la versión puedo usar su Delta_log para leer datos de versiones anteriores
df__old= spark.read.format("delta").option('VERSION AS OF', 3).load("Tables/dbo/calendario_2020_2022")
df__old= df__old.filter(df__old.Fecha== '2022-07-08' )
display(df__old)


# In[49]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# -- Si especifico la versión puedo usar su Delta_log para leer datos de versiones anteriores

# select *
# from LH_Capacitaciones.dbo.calendario_2020_2022 VERSION AS OF 1
# where Fecha = '2022-07-08'


# # Uso MERGE para hacer UPDATE, INSERT y DELETE en tablas DELTA a partir de una condicion

# In[53]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# -- Actualizo la tabla target usando otra tabla delta usando la Fecha como join

# MERGE INTO LH_Capacitaciones.dbo.TC    					         AS target  
#     USING 
# 		(SELECT *
#         FROM LH_Capacitaciones.tc.TC
#         WHERE Fecha BETWEEN '2020-01-01' and '2022-01-03')    AS source -- puedo usar una query o una tabla delta

#         ON target.Fecha = source.Fecha 	--condicion que se evalua entre las dos tablas delta

#     -- Actualiza cuando se cumple la condición
#     WHEN MATCHED THEN
#         UPDATE SET 
#              target.base = 'modificado'
#             ,target.EUR = source.EUR
  
#     -- Inserto registros cuando no se cumple la condición
#     WHEN NOT MATCHED THEN
#         INSERT 
#             (Fecha,
#             base,
# 			EUR,
# 			year)

#         VALUES
#             (source.Fecha,
#             source.base,
# 			source.EUR,
# 			source.year)


# In[55]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# select *
# from LH_Capacitaciones.dbo.tc
# limit 5


# In[56]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# -- hago un UPSERT sobre los tablas de la tabla target

# MERGE INTO LH_Capacitaciones.dbo.calendario_2020_2022                AS target
#   USING (SELECT *
#         FROM LH_Capacitaciones.masters.calendario
#         WHERE Fecha BETWEEN '2021-01-01' and '2021-01-03')    AS source --Puedo utilizar una query como tabla origen
#   ON target.Fecha = source.Fecha            --condicion que se evalua entre las dos tablas delta

# -- Actualizao los registros cuando se cumple la condición
#   WHEN MATCHED THEN
#     UPDATE SET
#       target.Periodo = 'Modificado'

# -- Elimino registros cuando no se cumple la condición
#   WHEN NOT MATCHED BY SOURCE THEN
#     DELETE


# In[57]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# Select *
# from LH_Capacitaciones.dbo.calendario_2020_2022

