spark = SparkSession.builder.getOrCreate()

schema_escuela = StructType([
	StructField('region', StringType()), 
	StructField('distrito', StringType()), 
	StructField('ciudad', StringType()), 
	StructField('id_escuela', IntegerType()), 
	StructField('nombre_escuela', StringType()), 
	StructField('nivel', StringType()), 
	StructField('serie', IntegerType())])

escuelas_DF = spark.read.format('csv').option('header', 'false').schema(schema_escuela).load("/home/robbs/escuelasPR.csv")
escuelas_DF.filter(escuelas_DF.region == 'Arecibo').groupBy('distrito', 'ciudad').count().show()