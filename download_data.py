import requests
import json
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


def get_file_content(filename):
    """
    Récupère le contenu du fichier en paramètre (requête SQL à effectuer).

    :param p1: String filename : Nom du fichier sur le répertoire courant à lire.
    
    :return: String content : Contenu du fichier extrait.
    """
    
    with open(filename) as f:
        content = f.read()
        return content


def get_file_content_as_list(filename):
    """
    Récupère le contenu du fichier en paramètre et le stocke dans une liste, une ligne par élément.

    :param p1: String filename : Nom du fichier sur le répertoire courant à lire.

    :return: List : Contenu du fichier extrait dans une liste.
    """
    data_into_list = []
    with open(filename, "r") as my_file:
        for line in my_file:
            data_into_list.append(line.strip())  # Utilisez strip() pour supprimer les sauts de ligne

    return data_into_list


def request_to_dvf(city_id, neighborhood_id):
    """
    Effectue une requête sur l'API des mutations de DVF.

    :param p1: Int city_id : Identifiant de la ville ciblée.
    :param p2: String neighborhood_id : Section cadastrale pour laquelle récupérer les données.

    :return: List : Contenu du fichier extrait dans une liste.
    """
    r = requests.get('https://app.dvf.etalab.gouv.fr/api/mutations3/' + str(city_id) + '/' + neighborhood_id)
    return r.text


def treat_mutations_before_insert(mutations, section_cadastrale_clean):
    """
    Traite les données récupérées et le ajoute dans une Liste pour la future insertion.

    :param p1: List mutations : Liste de mutations à ajouter en base.
    :param p2: String section_cadastrale_clean : Nom de la section cadastrale.
    """
    print("-----------------------------------------------------")
    print("Section cadastrale en cours de traitement : " + section_cadastrale_clean)
    print("Nombre de lignes à insérer en base de données : " + str(len(mutations)))
    
    for mutation in mutations:
        values_to_insert.append((
            None if mutation['id_mutation'] == 'None' else mutation['id_mutation'],
            mutation['date_mutation'],
            mutation['numero_disposition'],
            mutation['nature_mutation'],
            None if mutation['valeur_fonciere'] == 'None' or mutation['valeur_fonciere'] == 'nan' else float(mutation['valeur_fonciere']),
            None if mutation['adresse_numero'] == 'None' or mutation['adresse_numero'] == 'nan' else float(mutation['adresse_numero']),
            mutation['adresse_suffixe'],
            mutation['adresse_nom_voie'],
            mutation['adresse_code_voie'],
            mutation['code_postal'],
            mutation['code_commune'],
            mutation['nom_commune'],
            mutation['code_departement'],
            mutation['ancien_code_commune'],
            mutation['ancien_nom_commune'],
            section_cadastrale_clean,
            mutation['id_parcelle'],
            mutation['ancien_id_parcelle'],
            mutation['numero_volume'],
            mutation['lot1_numero'],
            None if mutation['lot1_surface_carrez'] == 'None' or mutation['lot1_surface_carrez'] == 'nan' else float(mutation['lot1_surface_carrez']),
            mutation['lot2_numero'],
            None if mutation['lot2_surface_carrez'] == 'None' or mutation['lot2_surface_carrez'] == 'nan' else float(mutation['lot2_surface_carrez']),
            mutation['lot3_numero'],
            None if mutation['lot3_surface_carrez'] == 'None' or mutation['lot3_surface_carrez'] == 'nan' else float(mutation['lot3_surface_carrez']),
            mutation['lot4_numero'],
            None if mutation['lot4_surface_carrez'] == 'None' or mutation['lot4_surface_carrez'] == 'nan' else float(mutation['lot4_surface_carrez']),
            mutation['lot5_numero'],
            None if mutation['lot5_surface_carrez'] == 'None' or mutation['lot5_surface_carrez'] == 'nan' else float(mutation['lot5_surface_carrez']),
            mutation['nombre_lots'],
            mutation['code_type_local'],
            mutation['type_local'],
            None if mutation['surface_reelle_bati'] == 'None' or mutation['surface_reelle_bati'] == 'nan' else float(mutation['surface_reelle_bati']),
            None if mutation['nombre_pieces_principales'] == 'None' or mutation['nombre_pieces_principales'] == 'nan' else float(mutation['nombre_pieces_principales']),
            mutation['code_nature_culture'],
            mutation['nature_culture'],
            mutation['code_nature_culture_speciale'],
            mutation['nature_culture_speciale'],
            None if mutation['surface_terrain'] == 'None' or mutation['surface_terrain'] == 'nan' else float(mutation['surface_terrain']),
            None if mutation['longitude'] == 'None' or mutation['longitude'] == 'nan' else float(mutation['longitude']),
            None if mutation['latitude'] == 'None' or mutation['latitude'] == 'nan' else float(mutation['latitude'])
        ))

    
def insert_to_db(values_to_insert):
    """
    Insère les données mise en forme en base de données.

    :param p1: List values_to_insert : Liste de mutations à ajouter en base.
    """
    db_objects = connect_to_db()
    connection = db_objects[0]
    cursor = db_objects[1]
    
    sql = '''INSERT INTO dvf (
        id_mutation,
        date_mutation,
        numero_disposition,
        nature_mutation,
        valeur_fonciere,
        adresse_numero,
        adresse_suffixe,
        adresse_nom_voie,
        adresse_code_voie,
        code_postal,
        code_commune,
        nom_commune,
        code_departement,
        ancien_code_commune,
        ancien_nom_commune,
        section_cadastrale,
        id_parcelle,
        ancien_id_parcelle,
        numero_volume,
        lot1_numero,
        lot1_surface_carrez,
        lot2_numero,
        lot2_surface_carrez,
        lot3_numero,
        lot3_surface_carrez,
        lot4_numero,
        lot4_surface_carrez,
        lot5_numero,
        lot5_surface_carrez,
        nombre_lots,
        code_type_local,
        type_local,
        surface_reelle_bati,
        nombre_pieces_principales,
        code_nature_culture,
        nature_culture,
        code_nature_culture_speciale,
        nature_culture_speciale,
        surface_terrain,
        longitude,
        latitude
    ) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
    
    cursor.executemany(sql, values_to_insert)
    connection.commit()
    close_db_connection(connection)


print("-----------------------------------------------------")
print("-----------------------------------------------------")
print("Début du script de récupération des données et de leur insertion en base.")

# Récupération des codes quartiers de Rennes
neighborhoodList = get_file_content_as_list("code_quartiers.txt")

# Définition de la ville ciblée. Ici c'est Rennes.
city_id = 35238

values_to_insert = []

print("-----------------------------------------------------")
print("Récupération des données.")

# Pour chaque section cadastrale, récupération des données et insertion en base :
for neighborhood in neighborhoodList:
    res = request_to_dvf(city_id, neighborhood)
    parsed_response = json.loads(res)
    mutations = parsed_response['mutations']
    treat_mutations_before_insert(mutations, neighborhood.replace('0', ''))

print("-----------------------------------------------------")
print("Insertion en base de données.")

insert_to_db(values_to_insert)

print("-----------------------------------------------------")
print("Succès. Fin du script.")
