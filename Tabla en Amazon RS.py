import psycopg2
from psycopg2.extras import execute_values

#Conexión a Base de datos  
def redshift():
    url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws"
    data_base="data-engineer-database"
    user="mendietadaniel1994_coderhouse"
    pwd= "xxxxx"
    try:
        conn = psycopg2.connect(
        host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
        dbname=data_base,
        user=user,
        password=pwd,
        port='5439'
    )
        print("Conexión exitosa!")
    
    except:
        print("Fallo la conexión.")
     
#CREACION DE LA TABLA EN RED SHIFT

    try:
        cur = conn.cursor()
        sql = """CREATE TABLE Productos (id INT PRIMARY KEY, 
        color VARCHAR(50), 
        area VARCHAR(50), 
        material VARCHAR(50), 
        producto VARCHAR(50),	
        precio FLOAT)"""    
        cur.execute(sql)
        conn.commit()
        print ("Tabla Creada")
    except:
        print("Algo salio mal!")
#DATOS A CARGAR

    id = 1
    color ='Blanco'
    area = 'Electrodomestico'
    material = 'Metal'
    producto = 'Heladera'
    precio = 80000

#CARGAR DATOS A LA TABLA CREADA
    try:
        cur = conn.cursor()
        sql = """INSERT INTO Productos (id, color, area, material, producto, precio ) 
    values ({0}, '{1}', '{2}', '{3}', '{4}',{5})""".format (id, color, area, material, producto, precio)
        cur.execute(sql)
        conn.commit()
        print ("Carga exitosa")
    except:
        print("Error al Cargar")   
    finally:
        cur.close()
        conn.close()    
        print("Conexión Terminada con exito")   
redshift()        
