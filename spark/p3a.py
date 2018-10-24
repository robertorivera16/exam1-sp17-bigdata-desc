spark = SparkSession.builder.getOrCreate()
schema_escuela = StructType([
	StructField('region', StringType()), 
	StructField('distrito', StringType()), 
	StructField('ciudad', StringType()), 
	StructField('id_escuela', IntegerType()), 
	StructField('nombre_escuela', StringType()), 
	StructField('nivel', StringType()), 
	StructField('serie', IntegerType())])
schema_estudiantes = StructType([
	StructField('region', StringType()), 
	StructField('distrito', StringType()), 
	StructField('id_escuela', IntegerType()), 
	StructField('nombre_escuela', StringType()),
	StructField('nivel', StringType()), 
	StructField('sexo', StringType()), 
	StructField('id_estudiante', IntegerType())])

escuelas_DF = spark.read.format('csv').option('header', 'false').schema(schema_escuela).load("/home/robbs/escuelasPR.csv")
estudiantes_DF= spark.read.format('csv').option('header', 'false').schema(schema_estudiantes).load("/home/robbs/studentsPR.csv")

estudiantes_DF.join(escuelas_DF, 'id_estudiante').filter(estudiantes_DF.sexo == 'M').filter((escuelas_DF.ciudad == 'Ponce')| (escuelas_DF.ciudad == 'San Juan')).filter(escuelas_DF.nivel == 'Superior').select(estudiantes_DF.id_estudiante).show()