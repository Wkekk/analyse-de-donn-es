import mysql.connector as mysql

class Connexion:
    __host = 'localhost'
    __port = '8081'
    __database = 'eminem'
    __user = 'root'
    __password = 'root'

    __curseur = None

#methode de connexion a la bdd
    @classmethod
    def ouvrir_connexion(cls):
        if cls.__curseur == None :
            cls.__bdd = mysql.connect(user = cls.__user,
                                    password = cls.__password,
                                    host = cls.__host,
                                    port = cls.__port,
                                    database = cls.__database)
            cls.__curseur = cls.__bdd.cursor(buffered=True)

#methode pour fermer la connexion a la bdd
    @classmethod
    def fermer_connexion(cls):
        cls.__curseur.close()
        cls.__bdd.close()
        cls.__curseur = None

#methode pour alimenter la base SQL à partir du fichier CSV importe
    @classmethod
    def remplir_table_song(cls, df):
        c = 0
        for i in df.index :
            cls.ouvrir_connexion()
            song_name = df['Song_Name'][i]
            lyrics = df['Lyrics'][i]
            views = df['Views'][i]
            release_date = df['Release_date'][i]
            album_id = cls.__curseur.execute(f"SELECT Album_id FROM album where Album_name = \"{df['Album_Name'][i]}\" ;")
            album_id =[x for x in cls.__curseur][0][0]
            remplir_table_song = "INSERT INTO songs (songs_name, lyrics,views, release_date, Album_id) VALUES (%s, %s, %s, %s, %s)" 
            val = (song_name, lyrics, views, release_date, album_id)                               
            cls.__curseur.execute(remplir_table_song, val)
            cls.__bdd.commit()
        
            cls.fermer_connexion()       

    @classmethod
    def remplir_table_album(cls, df):
        for i in df.index :
            album_name = df['Album_Name'][i]
            album_url = df['Album_URL'][i]    
            cls.ouvrir_connexion()
            remplir_table_album = "INSERT INTO album (album_name, album_url) VALUES (%s, %s)"
            val = (album_name, album_url) 
            cls.__curseur.execute(remplir_table_album, val)
            cls.__bdd.commit()
            cls.fermer_connexion()    

    @classmethod
    def creer_vue(cls):
            cls.ouvrir_connexion()
            drop = "DROP VIEW IF EXISTS Chronologie;"
            query = f"CREATE VIEW Chronologie AS SELECT Songs_Name, views FROM songs ORDER BY lyrics DESC;"
            cls.__curseur.execute(drop)
            cls.__curseur.execute(query)
            cls.__bdd.commit()
            cls.fermer_connexion()

   
    @classmethod
    def creer_procedure(cls):
            cls.ouvrir_connexion()
            drop = "DROP PROCEDURE IF EXISTS localisation_song;"
            query = "CREATE PROCEDURE localisation_song (IN titre CHAR(100)) BEGIN SELECT Album_Name FROM album natural join songs WHERE Songs_name = titre; END  "  
            cls.__curseur.execute(drop)
            cls.__curseur.execute(query)
            cls.__bdd.commit()
            cls.fermer_connexion()

    