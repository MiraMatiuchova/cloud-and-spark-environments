# Databricks notebook source
# MAGIC %md
# MAGIC ### Upload a file "Quijote"

# COMMAND ----------

#define file path
file = 'dbfs:/FileStore/quijote.txt/quijote.txt'
#create a new RDD called lines
lineas = sc.textFile(file)
#create a new RDD where we count the number of features in each element
long_lineas = lineas.map(lambda elemento: len(elemento))
#we print and apply the reduce action to obtain a count of all the characters in the file
print(long_lineas.reduce(lambda elem1, elem2: elem1 + elem2))


# COMMAND ----------

type(lineas)

# COMMAND ----------

lineas.collect()
