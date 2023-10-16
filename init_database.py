import pymysql


def connect_to_db():
    """
    Initie la connexion à la base de données

    :return: Array : Tableau contenant l'objet de connexion ainsi que le curseur.
    """
     
    connection = pymysql.connect(host="localhost", port=3306, user="root", passwd="", database="base_bien_visualizer")
    cursor = connection.cursor()
    return [connection, cursor]
    
    
def close_db_connection(connection):
    """
    Ferme la connexion à la base de données

    :param p1: pymysql.Connection connection : Objet de connexion à la base de données.
    """
    connection.close()


def create_db(cursor):
    """
    Crée la base de données avec toutes ses tables.

    :param p1: pymysql.Cursor connection : Objet de requêtage de la base de données.
    
    :return: Null
    """
    
    rq = get_file_content('sql/DROP_DB.sql')
    rq.replace("\n", "")
    cursor.execute(rq)
    rq = get_file_content('sql/CREATE_DB.sql')
    rq.replace("\n", "")
    cursor.execute(rq)
    return


def get_file_content(filename):
    """
    Récupère le contenu du fichier en paramètre (requête SQL à effectuer).

    :param p1: String filename : Nom du fichier sur le répertoire courant à lire.
    
    :return: String content : Contenu du fichier extrait.
    """
    
    with open(filename) as f:
        content = f.read()
        return content
    

print("Début de la création de la base de données.")
print("Connexion à la base de données.")
db_objects = connect_to_db()
connection = db_objects[0]
cursor = db_objects[1]
print("Création.")
create_db(cursor)
close_db_connection(connection)
print("La base de données a correctement été créée.")
print("Déconnexion de la base de données.")