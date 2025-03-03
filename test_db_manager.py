import pytest
# test_db_manager.py
from db_manager.db_manager import DatabaseManager


@pytest.fixture
def db():
    db = DatabaseManager(":memory:")
    db.create_table("users", "id ipk, name str, age int")
    return db

@pytest.fixture
def db_blob():
    db = DatabaseManager(":memory:")
    db.create_table("files", "id ipk, name str, content blob")
    return db

@pytest.fixture
def db_real():
    db = DatabaseManager(":memory:")
    db.create_table("scores", "id ipk, name str, score real")
    return db

def test_create_table(db):
    result = db.read("users")
    assert result == [], "La table devrait être vide après création."
    assert db.get_columns("users") == ["id", "name", "age"], "Les colonnes ne correspondent pas."

def test_insert(db):
    db.insert("users", (None, "Alice", 25))
    result = db.read("users")
    assert result == [(1, "Alice", 25)], "L'insertion avec auto-incrémentation a échoué."

def test_insert_many(db):
    data = [(None, "Alice", 25), (None, "Bob", 30), (None, "Charlie", 22)]
    db.insert_many("users", data)
    result = db.read("users")
    assert result == [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 22)], "L'insertion multiple a échoué."

def test_read_with_condition(db):
    db.insert("users", (None, "Alice", 25))
    db.insert("users", (None, "Bob", 30))
    result = db.read("users", "WHERE age > 25")
    assert result == [(2, "Bob", 30)], "La lecture avec condition a échoué."

def test_select(db):
    db.insert("users", (None, "Alice", 25))
    db.insert("users", (None, "Bob", 30))
    result = db.select("users", ["name", "age"])
    assert result == [("Alice", 25), ("Bob", 30)], "La sélection personnalisée sans condition a échoué."
    result_cond = db.select("users", ["name"], "WHERE age > 25")
    assert result_cond == [("Bob",)], "La sélection personnalisée avec condition a échoué."

def test_update(db):
    db.insert("users", (None, "Alice", 25))
    db.update("users", "age=26", "WHERE id=1")
    result = db.read("users")
    assert result == [(1, "Alice", 26)], "La mise à jour a échoué."

def test_delete(db):
    db.insert("users", (None, "Alice", 25))
    db.delete("users", "WHERE id=1")
    result = db.read("users")
    assert result == [], "La suppression a échoué."

def test_custom_query(db):
    db.execute_custom_query("INSERT INTO users VALUES (?, ?, ?)", (None, "Alice", 25))
    result = db.execute_custom_query("SELECT * FROM users WHERE age = ?", (25,))
    assert result == [(1, "Alice", 25)], "La requête personnalisée a échoué."

def test_count(db):
    db.insert("users", (None, "Alice", 25))
    db.insert("users", (None, "Bob", 30))
    db.insert("users", (None, "Charlie", 22))
    assert db.count("users") == 3, "Le comptage total a échoué."
    assert db.count("users", "age > 25") == 1, "Le comptage avec condition a échoué."

def test_is_empty(db):
    assert db.is_empty("users") == True, "La table devrait être vide après création."
    db.insert("users", (None, "Alice", 25))
    assert db.is_empty("users") == False, "La table ne devrait pas être vide après insertion."

def test_table_exists(db):
    assert db.table_exists("users") == True, "La table devrait exister après création."
    db.drop_table("users")
    assert db.table_exists("users") == False, "La table ne devrait pas exister après suppression."

def test_drop_table(db):
    db.insert("users", (None, "Alice", 25))
    db.drop_table("users")
    assert db.table_exists("users") == False, "La table n’a pas été supprimée correctement."

def test_get_columns(db):
    columns = db.get_columns("users")
    assert columns == ["id", "name", "age"], "Les colonnes retournées ne correspondent pas."

def test_find_by_column(db):
    db.insert("users", (None, "Alice", 25))
    db.insert("users", (None, "Bob", 30))
    db.insert("users", (None, "Charlie", 22))
    result_eq = db.find_by_column("users", "age", "=", 25)
    result_gt = db.find_by_column("users", "age", ">", 25)
    assert result_eq == [(1, "Alice", 25)], "La recherche par égalité a échoué."
    assert result_gt == [(2, "Bob", 30)], "La recherche par supérieur a échoué."

def test_blob_column(db_blob):
    db_blob.insert("files", (None, "file1.txt", b"Contenu binaire"))
    result = db_blob.read("files")
    assert result == [(1, "file1.txt", b"Contenu binaire")], "L'insertion avec BLOB a échoué."
    assert db_blob.get_columns("files") == ["id", "name", "content"], "Les colonnes avec BLOB ne correspondent pas."

def test_real_column(db_real):
    db_real.insert("scores", (None, "Alice", 85.5))
    db_real.insert("scores", (None, "Bob", 92.3))
    result = db_real.read("scores")
    assert result == [(1, "Alice", 85.5), (2, "Bob", 92.3)], "L'insertion avec REAL a échoué."
    assert db_real.get_columns("scores") == ["id", "name", "score"], "Les colonnes avec REAL ne correspondent pas."
    result_gt = db_real.find_by_column("scores", "score", ">", 90)
    assert result_gt == [(2, "Bob", 92.3)], "La recherche avec REAL a échoué."

def test_security(db):
    with pytest.raises(ValueError):
        db.read("users; DROP TABLE scores")  # Tentative d’injection dans table_name
    with pytest.raises(ValueError):
        db.select("users", ["name; DROP TABLE scores"])  # Tentative d’injection dans columns