from db_manager.db_manager import DatabaseManager

# main.py


# Initialiser avec une base SQLite
db = DatabaseManager("test_sqlite.db")

# Créer une table valide
db.create_table("scores", "id ipk, name str, score real, data blob")

# Vérifier si la table est vide
print("La table scores est vide ?", db.is_empty("scores"))  # Résultat : True

# Insérer des données
data = [
    (None, "Alice", 85.5, b"Alice data"),
    (None, "Bob", 92.3, b"Bob data"),
    (None, "Charlie", 77.8, b"Charlie data"),
]
db.insert_many("scores", data)

# Vérifier à nouveau si la table est vide
print("La table scores est vide après insertion ?", db.is_empty("scores"))  # Résultat : False

# Utiliser read
print("Toutes les données (read) :", db.read("scores"))

# Utiliser select
print("Noms et scores uniquement :", db.select("scores", ["name", "score"]))
print("Scores > 80 (noms et scores) :", db.select("scores", ["name", "score"], "WHERE score > 80"))

# Compter les lignes
print("Nombre total d'entrées :", db.count("scores"))

# Vérifier si la table existe
print("Table scores existe ?", db.table_exists("scores"))

# Obtenir les colonnes
print("Colonnes de la table scores :", db.get_columns("scores"))

# Supprimer la table
db.drop_table("scores")
print("Table scores existe après suppression ?", db.table_exists("scores"))

# Tester une tentative d'injection (échouera)
try:
    db.read("scores; DROP TABLE users")  # Tentative d’injection
except ValueError as e:
    print(f"Erreur de sécurité détectée : {e}")

# Fermer
db.close()