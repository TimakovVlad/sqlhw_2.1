import psycopg2

def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(50) NOT NULL,
                last_name VARCHAR(50) NOT NULL,
                email VARCHAR(100) UNIQUE NOT NULL
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS phones (
                id SERIAL PRIMARY KEY,
                client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
                phone_number VARCHAR(20) NOT NULL
            );
        """)

def drop_tables(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS phones;")
        cur.execute("DROP TABLE IF EXISTS clients;")

def add_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO clients (first_name, last_name, email) 
            VALUES (%s, %s, %s) RETURNING id;
        """, (first_name, last_name, email))
        client_id = cur.fetchone()[0]

    if phones:
        for phone in phones:
            add_phone(conn, client_id, phone)

def add_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO phones (client_id, phone_number) 
            VALUES (%s, %s);
        """, (client_id, phone))

def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    with conn.cursor() as cursor:
        set_values = []
        update_params = []

        if first_name is not None:
            set_values.append("first_name = %s")
            update_params.append(first_name.encode('utf-8').decode('latin-1'))

        if last_name is not None:
            set_values.append("last_name = %s")
            update_params.append(last_name.encode('utf-8').decode('latin-1'))

        if email is not None:
            set_values.append("email = %s")
            update_params.append(email.encode('utf-8').decode('latin-1'))

        if set_values:
            set_clause = ", ".join(set_values)
            update_query = f"UPDATE clients SET {set_clause} WHERE id = %s;"
            update_params.append(client_id)
            cursor.execute(update_query, update_params)

        if phones is not None:
            cursor.execute("DELETE FROM phones WHERE client_id = %s;", (client_id,))
            for phone in phones:
                add_phone(conn, client_id, phone)


def delete_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM phones WHERE client_id = %s AND phone_number = %s;", (client_id, phone))

def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM clients WHERE id = %s;", (client_id,))

def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    with conn.cursor() as cursor:
        if phone is not None:
            cursor.execute("""
                SELECT c.id, c.first_name, c.last_name, c.email, ARRAY_AGG(p.phone_number) as phones
                FROM clients c
                LEFT JOIN phones p ON c.id = p.client_id
                WHERE p.phone_number = %s
                GROUP BY c.id;
            """, (phone,))
        else:
            cursor.execute("""
                SELECT c.id, c.first_name, c.last_name, c.email, ARRAY_AGG(p.phone_number) as phones
                FROM clients c
                LEFT JOIN phones p ON c.id = p.client_id
                WHERE c.first_name = %s OR c.last_name = %s OR c.email = %s
                GROUP BY c.id;
            """, (first_name, last_name, email))

        return cursor.fetchall()


# Пример использования:
with psycopg2.connect(database="netology_bd", user="postgres", password="root") as conn:
    drop_tables(conn)
    create_db(conn)
    add_client(conn, "John", "Jostar", "john.doe@example.com", phones=["123456789", "987654321"])
    add_client(conn, "Alice", "Smith", "alice.smith@example.com")

    print("Before update:")
    print(find_client(conn, first_name="John"))

    change_client(conn, client_id=1, first_name="Jonathan", phones=["111111111"])

    print("After update:")
    print(find_client(conn, first_name="Jonathan"))

    delete_phone(conn, client_id=1, phone="111111111")

    print("After deleting phone:")
    print(find_client(conn, first_name="Jonathan"))

    delete_client(conn, client_id=1)

    print("After deleting client:")
    print(find_client(conn, last_name="Doe"))

    print(find_client(conn, first_name="Alice"))