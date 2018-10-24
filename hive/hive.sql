
-- Creation of tables:
create table escuelas (region STRING, distrito STRING, ciudad STRING, id_escuela INT, nombre STRING, nivel STRING, serie INT);
create table estudiantes (region STRING, distrito STRING, id_escuela INT, nombre_escuela STRING, nivel STRING, sexo STRING, id_estudiante INT);


-- Task #1:
select estudiantes.region, escuelas.ciudad, count(*) from estudiantes, escuelas where escuelas.region = estudiantes.region group by estudiantes.region, escuelas.ciudad; 

-- Task #2:
		select escuelas.ciudad, escuelas.nivel, count(*) from escuelas 
		group by escuelas.ciudad, escuelas.nivel;

-- Task #3:
 		select count(*) from escuelas, estudiantes  
 		where escuelas2.id_escuela=estudiantes.id_escuela 
 		and estudiantes.sexo='F' 
 		and escuelas2.ciudad='Ponce';

-- Task #4:
		select escuelas.region, escuelas.distrito, escuelas.ciudad, count(*)  
		from escuelas, estudiantes 
		where escuelas.id_escuela = estudiantes.id_escuela 
		and estudiantes.sexo = 'M' 
		group by escuelas.region, escuelas.distrito, escuelas.ciudad;

