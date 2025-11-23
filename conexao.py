import mysql.connector
from mysql.connector import Error

HOST = "193.203.175.97"
USER = "u207281299_robo_exportado"
PASSWORD = "842413Ka@@"
DATABASE = "u207281299_robo_exportado"

def conectar_banco():
    return mysql.connector.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DATABASE
    )

def testar_conexao():
    try:
        conn = conectar_banco()
        if conn.is_connected():
            conn.close()
            return "✅ Conectado ao banco de dados com sucesso!"
        else:
            return "⚠️ Falha na conexão com o banco de dados."
    except Error as e:
        return f"❌ Erro ao conectar: {e}"

if __name__ == "__main__":
    print(testar_conexao())
