#import bibliothèques
import mysql.connector as mysql
import pandas as pd
from Connexion_1 import Connexion


#import cvs avec data
data = pd.read_csv("Eminem_Lyrics.csv", sep='\t', comment='#', encoding = "ISO-8859-1")
print(data.dtypes)
del data["Unnamed: 6"]
data = data.dropna(axis = 'rows')


Connexion.ouvrir_connexion()
Connexion.remplir_table_album(data)
Connexion.remplir_table_song(data)
Connexion.creer_vue()
Connexion.creer_procedure()
        