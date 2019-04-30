# Benodigde packages

import os
import json
import getpass         # Package om een paswoordveldje te genereren.
import mysql.connector # MySQL package
import pandas as pd    # Populaire package voor data-verwerking

filename = os.path.join(os.path.dirname(os.getcwd()), 'solution', 'all_q_colnam.json')
col_names = json.load(open(filename, 'r'))


def verbind_met_GB(username, hostname, gegevensbanknaam):
    """
    Maak verbinding met een externe gegevensbank

    :param  username:          username van de gebruiker, string
    :param  hostname:          naam van de host, string.
                               Dit is in het geval van een lokale server gewoon 'localhost'
    :param  gegevensbanknaam:  naam van de gegevensbank, string.
    :return connection:        connection object, dit is wat teruggeven wordt
                               door connect() methods van packages die voldoen aan de DB-API
    """

    password = getpass.getpass()  # Genereer vakje voor wachtwoord in te geven

    connection = mysql.connector.connect(host=hostname,
                                         user=username,
                                         passwd=password,
                                         db=gegevensbanknaam)
    return connection


username = 'adam'  # Vervang dit als je via een andere user queries stuurt
hostname = 'localhost'  # Als je een databank lokaal draait, is dit localhost.
db = 'lahman2016'  # Naam van de gegevensbank op je XAMPP Mysql server

# We verbinden met de gegevensbank
c = verbind_met_GB(username, hostname, db)


def run_query(connection, query):
    """
    Voer een query uit op een reeds gemaakte connectie, geeft het resultaat van de query terug
    """

    # Making a cursor and executing the query
    cursor = connection.cursor(buffered=True)
    cursor.execute(query)

    # Collecting the result and casting it in a pd.DataFrame
    res = cursor.fetchall()

    return res


def res_to_df(query_result, column_names):
    """
    Giet het resultaat van een uitgevoerde query in een 'pandas dataframe'
    met vooraf gespecifieerde kolomnamen.

    Let op: Het resultaat van de query moet dus exact evenveel kolommen bevatten
    als kolomnamen die je meegeeft. Als dit niet het geval is, is dit een indicatie
    dat je oplossing fout is. (Gezien wij de kolomnamen van de oplossing al cadeau doen)

    """
    df = pd.DataFrame(query_result, columns=column_names)
    return df
