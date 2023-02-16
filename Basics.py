# Databricks notebook source
# MAGIC %md
# MAGIC #Collect()/count()/map

# COMMAND ----------

numeros = sc.parallelize ([1,2,3,4,5,6,7,8,9,10], 2)

# COMMAND ----------

print(numeros.count())

# COMMAND ----------

numeros.collect()

# COMMAND ----------

type(numeros)

# COMMAND ----------

numeros.getNumPartitions()

# COMMAND ----------

# MAGIC %md 
# MAGIC #Transformation map()

# COMMAND ----------

numeros = sc.parallelize([1,2,3,4,5])
num3 = numeros.map(lambda x: x * 3)
num3.collect()

# COMMAND ----------

palabras = sc.parallelize(['Hola', 'Que', 'Tal'])
pal_minus = palabras.map(lambda elemento: elemento.lower())
pal_minus.collect()

# COMMAND ----------

palabras = sc.parallelize(['Hola', 'Que', 'Tal'])
pal_long = palabras.map(lambda p: len(p))
pal_long.collect()
