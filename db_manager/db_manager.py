# db_manager/db_manager.py
import sqlite3
import logging
import re
from typing import List, Tuple, Any, Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="db_manager.log"
)
logger = logging.getLogger(__name__)


class DatabaseManager:
    """Un module pour gérer une base de données SQLite avec sécurité renforcée."""

    def __init__(self, db_name: str):
        """Initialise la connexion à la base de données SQLite."""
        self.db_name = db_name
        self.connection = None
        self.cursor = None
        self.connect()

    def _validate_identifier(self, identifier: str) -> None:
        """Valide qu'un identifiant (table ou colonne) est sûr."""
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", identifier):
            raise ValueError(
                f"Identifiant invalide : {identifier}. Seuls lettres, chiffres et underscores sont autorisés.")

    def connect(self) -> None:
        """Établit la connexion à la base de données SQLite."""
        try:
            self.connection = sqlite3.connect(self.db_name)
            self.cursor = self.connection.cursor()
            logger.info(f"Connecté à la base de données : {self.db_name}")
        except sqlite3.Error as e:
            logger.error("Erreur de connexion à la base de données")
            raise

    def create_table(self, table_name: str, columns_def: str) -> None:
        """Crée une table avec une définition simplifiée des colonnes."""
        self._validate_identifier(table_name)
        try:
            type_mapping = {
                "ipk": "INTEGER PRIMARY KEY AUTOINCREMENT",
                "str": "TEXT",
                "int": "INTEGER",
                "blob": "BLOB",
                "real": "REAL"
            }
            columns = [col.strip() for col in columns_def.split(",")]
            columns_sql = []
            for col in columns:
                name, type_short = col.strip().split()
                self._validate_identifier(name)
                sql_type = type_mapping.get(type_short, "TEXT")
                columns_sql.append(f"{name} {sql_type}")
            columns_str = ", ".join(columns_sql)
            query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_str})"
            self.cursor.execute(query)
            self.connection.commit()
            logger.info(f"Table {table_name} créée avec succès.")
        except sqlite3.Error as e:
            logger.error("Erreur lors de la création de la table")
            raise
        except ValueError as e:
            logger.error(f"Erreur dans la définition des colonnes : {str(e)}")
            raise

    def insert(self, table_name: str, data: Tuple[Any, ...]) -> None:
        """Insère une ligne dans la table."""
        self._validate_identifier(table_name)
        placeholders = ",".join("?" * len(data))
        try:
            query = f"INSERT INTO {table_name} VALUES ({placeholders})"
            self.cursor.execute(query, data)
            self.connection.commit()
            logger.info("Données insérées avec succès.")
        except sqlite3.Error as e:
            logger.error("Erreur lors de l'insertion")
            raise

    def insert_many(self, table_name: str, data_list: List[Tuple[Any, ...]]) -> None:
        """Insère plusieurs lignes dans une table en une seule opération."""
        self._validate_identifier(table_name)
        if not data_list:
            logger.info("Aucune donnée à insérer.")
            return
        placeholders = ",".join("?" * len(data_list[0]))
        try:
            query = f"INSERT INTO {table_name} VALUES ({placeholders})"
            self.cursor.executemany(query, data_list)
            self.connection.commit()
            logger.info(f"{len(data_list)} lignes insérées dans {table_name}.")
        except sqlite3.Error as e:
            logger.error("Erreur lors de l'insertion multiple")
            raise

    def read(self, table_name: str, condition: str = None) -> List[Tuple[Any, ...]]:
        """Lit toutes les colonnes d'une table."""
        self._validate_identifier(table_name)
        try:
            query = f"SELECT * FROM {table_name}" + (f" {condition}" if condition else "")
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            logger.info(f"Lecture effectuée sur {table_name}.")
            return result
        except sqlite3.Error as e:
            logger.error("Erreur lors de la lecture")
            return []

    def select(self, table_name: str, columns: List[str], condition: str = None) -> List[Tuple[Any, ...]]:
        """Lit des colonnes spécifiques d'une table avec une condition optionnelle."""
        self._validate_identifier(table_name)
        for col in columns:
            self._validate_identifier(col)
        try:
            columns_str = ", ".join(columns)
            query = f"SELECT {columns_str} FROM {table_name}" + (f" {condition}" if condition else "")
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            logger.info(f"Sélection personnalisée effectuée sur {table_name} pour {columns_str}.")
            return result
        except sqlite3.Error as e:
            logger.error("Erreur lors de la sélection personnalisée")
            return []

    def update(self, table_name: str, set_clause: str, condition: str) -> None:
        """Met à jour des données."""
        self._validate_identifier(table_name)
        try:
            query = f"UPDATE {table_name} SET {set_clause} {condition}"
            self.cursor.execute(query)
            self.connection.commit()
            logger.info("Données mises à jour avec succès.")
        except sqlite3.Error as e:
            logger.error("Erreur lors de la mise à jour")
            raise

    def delete(self, table_name: str, condition: str) -> None:
        """Supprime des données."""
        self._validate_identifier(table_name)
        try:
            query = f"DELETE FROM {table_name} {condition}"
            self.cursor.execute(query)
            self.connection.commit()
            logger.info("Données supprimées avec succès.")
        except sqlite3.Error as e:
            logger.error("Erreur lors de la suppression")
            raise

    def execute_custom_query(self, query: str, params: Tuple[Any, ...] = None) -> Optional[List[Tuple[Any, ...]]]:
        """Exécute une requête SQL personnalisée avec paramètres sécurisés."""
        try:
            self.cursor.execute(query, params or ())
            if query.strip().upper().startswith("SELECT"):
                result = self.cursor.fetchall()
                logger.info(f"Requête personnalisée exécutée : {query}")
                return result
            else:
                self.connection.commit()
                logger.info(f"Requête personnalisée exécutée : {query}")
                return None
        except sqlite3.Error as e:
            logger.error("Erreur lors de l'exécution de la requête personnalisée")
            raise

    def count(self, table_name: str, condition: str = None) -> int:
        """Compte le nombre de lignes dans une table avec une condition optionnelle."""
        self._validate_identifier(table_name)
        try:
            where_clause = f" WHERE {condition}" if condition else ""
            query = f"SELECT COUNT(*) FROM {table_name}{where_clause}"
            self.cursor.execute(query)
            result = self.cursor.fetchone()[0]
            logger.info(f"Compte effectué sur {table_name}: {result} lignes.")
            return result
        except sqlite3.Error as e:
            logger.error("Erreur lors du comptage")
            return 0

    def is_empty(self, table_name: str) -> bool:
        """Vérifie si une table est vide."""
        self._validate_identifier(table_name)
        try:
            query = f"SELECT COUNT(*) FROM {table_name}"
            self.cursor.execute(query)
            count = self.cursor.fetchone()[0]
            is_empty = count == 0
            logger.info(f"Vérification si {table_name} est vide : {is_empty}")
            return is_empty
        except sqlite3.Error as e:
            logger.error("Erreur lors de la vérification si la table est vide")
            return True

    def table_exists(self, table_name: str) -> bool:
        """Vérifie si une table existe dans la base de données."""
        self._validate_identifier(table_name)
        try:
            query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
            self.cursor.execute(query, (table_name,))
            result = self.cursor.fetchone() is not None
            logger.info(f"Vérification : la table {table_name} existe = {result}")
            return result
        except sqlite3.Error as e:
            logger.error("Erreur lors de la vérification de la table")
            return False

    def drop_table(self, table_name: str) -> None:
        """Supprime une table de la base de données."""
        self._validate_identifier(table_name)
        try:
            query = f"DROP TABLE IF EXISTS {table_name}"
            self.cursor.execute(query)
            self.connection.commit()
            logger.info(f"Table {table_name} supprimée avec succès.")
        except sqlite3.Error as e:
            logger.error("Erreur lors de la suppression de la table")
            raise

    def get_columns(self, table_name: str) -> List[str]:
        """Retourne la liste des noms de colonnes d'une table."""
        self._validate_identifier(table_name)
        try:
            self.cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [row[1] for row in self.cursor.fetchall()]
            logger.info(f"Colonnes de {table_name} : {columns}")
            return columns
        except sqlite3.Error as e:
            logger.error("Erreur lors de la récupération des colonnes")
            return []

    def find_by_column(self, table_name: str, column: str, condition: str, value: Any) -> List[Tuple[Any, ...]]:
        """Recherche des lignes avec une condition flexible sur une colonne."""
        self._validate_identifier(table_name)
        self._validate_identifier(column)
        try:
            query = f"SELECT * FROM {table_name} WHERE {column} {condition} ?"
            self.cursor.execute(query, (value,))
            result = self.cursor.fetchall()
            logger.info(f"Recherche dans {table_name} sur {column} {condition} : {len(result)} résultats.")
            return result
        except sqlite3.Error as e:
            logger.error("Erreur lors de la recherche")
            return []

    def close(self) -> None:
        """Ferme la connexion à la base de données."""
        if self.connection:
            self.connection.close()
            logger.info("Connexion fermée.")