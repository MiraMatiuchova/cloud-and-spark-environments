# Databricks notebook source
# MAGIC %md 
# MAGIC ## Working with files 
# MAGIC ####Exercise  2

# COMMAND ----------

file = 'dbfs:/FileStore/shared_uploads/miruncik04@gmail.com/quijote.txt'

# COMMAND ----------

#create RDD from 'quijote' file
lineas = sc.textFile(file)

#create RDD separate lines in don quijote file
separar_palabras = lineas.map(lambda x: x.split (' '))

#create an RDD count the words
num_palabras = separar_palabras.map(lambda x: lem (x))

#sum the elements
total_palabras = numero_palabras.reduce(lambda el1, el2: el1 + el2 )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Exercise 3
# MAGIC #### Calculate the average of the measurements of humidity sensors of a land (plantation)

# COMMAND ----------

#store the path of the file
file = 'dbfs:/FileStore/shared_uploads/miruncik04@gmail.com/sensores.txt'
#assign the file path to the variable
medidas = sc.textFile(file)

#separate the measures in the same way that we separate the words of Don Quixote
separar_medidas = medidas.map(lambda x: x.split(' '))

#we create a function to loop through each element and convert it into a numeric value
def obtener_medidas_elemento(elemento):
    lista_salida  = []
    
    for medida in elemento:
        numero = float(medida)
        lista_salida.append(numero)
        
    return (lista_salida)
        

# COMMAND ----------

#we use the flatMap transformation and call the function obtain_measurements_element
final_medidas = separar_medidas.flatMap(obtener_medidas_elemento)

#Print 10 elements of the final rdd line
final_medidas.take(10)
                                     

# COMMAND ----------

separar_medidas.take(2)

# COMMAND ----------

medidas.collect()

# COMMAND ----------

#we count how many values there are in the final_measures rdd
numeros_medidas = final_medidas.count()

#we add the values of the final_measurements rdd
suma_total_medidas = final_medidas.reduce(lambda e1, e2: e1 + e2)

#we print the average by applying the formula (divide the sum of a cluster of numbers by the number of them)
print('la medida es: ',suma_total_medidas/numeros_medidas, 'cm3 de agua')

# COMMAND ----------

suma_total_medidas
