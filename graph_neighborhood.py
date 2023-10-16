import pymysql
import pymysql.cursors
import plotly.express as px


def connect_to_db():
    """
    Initie la connexion à la base de données

    :return: Array : Tableau contenant l'objet de connexion ainsi que le curseur.
    """
     
    connection = pymysql.connect(host="localhost", port=3306, user="root", passwd="", database="base_bien_visualizer")
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    return [connection, cursor]


def close_db_connection(connection):
    """
    Ferme la connexion à la base de données

    :param p1: pymysql.Connection connection : Objet de connexion à la base de données.
    """
    connection.close()
    
# fig = px.bar(x=["a", "b", "c"], y=[1, 3, 2])
# fig.write_html('first_figure.html', auto_open=True)

# Récupération en BDD d'un an puis décomposition mois par mois du prix moyen d'achat au m²
def fetch_data(cursor):
    """
    Récupère les données en base en fonction d'un mois.

    :param p1: pymysql.Cursor connection : Objet de requêtage de la base de données.
    
    :return: Null
    """
    
    
    
    # periods = [['']]
    
    rq = '''
    SELECT *
    FROM dvf
    WHERE date_mutation >= '2022-01-01' 
    LIMIT 10
    '''
    
    rq.replace("\n", "")
    
    cursor.execute(rq)
    
    for row in cursor:
        print(row)
    
    return


def determine_periods(cursor):
    
    # Détermination de la date minimale et de la maximale dans le but de définir les périodes dans une liste exploitable.
    
    # (datetime.date(2018, 1, 2),)
    # (datetime.date(2022, 12, 31)
    
    rq = '''
    SELECT MAX(date_mutation) 
    FROM dvf
    '''
    rq.replace("\n", "")
    
    cursor.execute(rq)
    resMax = cursor.fetchall()
    print(resMax)
    
    
    # for row in cursor:
        # print(row)
    
    # minDate = 
    
    return 0

db_objects = connect_to_db()
connection = db_objects[0]
cursor = db_objects[1]

print("Début de la récupération des données.")
determine_periods(cursor)

# fetch_data(cursor)
close_db_connection(connection)
print("Fin de la récupération des données.")

