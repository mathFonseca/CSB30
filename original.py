import psycopg2
import numpy as np

# Parte 1 - Conexão com o banco de dados.
try:
    conn = psycopg2.connect("dbname='1802BandoDeDados' user='1802BandoDeDados' host='200.134.10.32' password='803322'")
except Exception as error:
    print("Falha na conexão com o banco de dados.")
    print(error)

#-----
# Funções


def distanciaJaccard(conjuntoA, conjuntoB):
    # Calcula a distância de Jaccard entre os conjuntos A e B.
    conjuntoA = set(conjuntoA.split())
    conjuntoB = set(conjuntoB.split())
    # Separa os elementos do conjunto em uma lista.
    coeficiente = len(conjuntoA & conjuntoB) / len(conjuntoB | conjuntoA)
    # Divide a Intersecção de A e B pela União de A e B.
    return (1 - coeficiente)


def obtemNumeroDeArtistas(conn):
    # Obtem o número total de artistas cadastrados no banco de dados.
    cur = conn.cursor()
    consulta = "SELECT COUNT(*) FROM Artista_Musical;"
    cur.execute(consulta)
    numero_artista = cur.fetchone()
    cur.close()
    return numero_artista[0]


def obtemNumeroDeFilmes(conn):
    # Obtem o número total de filmes cadastrados no banco de dados.
    cur = conn.cursor()
    consulta = "SELECT COUNT(*) FROM Filmes;"
    cur.execute(consulta)
    numero_filmes = cur.fetchone()
    cur.close()
    return numero_filmes[0]


def obtemNumeroDeUsuarios(conn):
    # Obtem o número total de usuários cadastrados no banco de dados.
    cur = conn.cursor()
    consulta = "SELECT COUNT(*) FROM Pessoa;"
    cur.execute(consulta)
    numero_usuarios = cur.fetchone()
    cur.close()
    return numero_usuarios[0]


def criaViewNumeroArtista(conn):
    # Cria a VIEW que associa um número único para cada artista cadastrado.
    cur = conn.cursor()
    consulta = "CREATE OR REPLACE VIEW Numero_Artista AS SELECT -1+ROW_NUMBER() OVER (ORDER BY Artista_Musical) AS Numero, ID FROM Artista_Musical;"
    cur.execute(consulta)
    cur.close()
    # A VIEW criada começa com os números de [1...] e não em [0...]


def criaViewNumeroFilmes(conn):
    # Cria a VIEW que associa um número único para cada filme cadastrado.
    cur = conn.cursor()
    consulta = "CREATE or REPLACE VIEW Numero_filmes AS SELECT -1+ROW_NUMBER() OVER(ORDER BY Filmes) AS Numero, ID FROM Filmes;"
    cur.execute(consulta)
    cur.close()
    # A VIEW criada começa com os números de [1...] e não em [0...]


def criaViewNumeroUsuario(conn):
    # Cria a VIER que associa um número único para cada usuário cadastrado.
    cur = conn.cursor()
    consulta = "CREATE or REPLACE VIER Numero_Usuario AS SELECT -1+ROW_NUMBER() OVER(ORDER BY Pessoa) AS Numero, Login FROM Pessoa;"
    cur.execute(consulta)
    cur.close()


def criaMatrizUsuarioFilme(conn):
    # Cria a matriz no modelo matriz[usuario][filme].
    numero_usuarios = obtemNumeroDeUsuarios(conn)
    numero_filmes = obtemNumeroDeFilmes(conn)
    matriz_usuario_filme = np.zeros((numero_usuarios, numero_filmes))
    return matriz_usuario_filme


def obtemConjuntoFilmes(conn):
    # Cria o conjunto de filmes assistidos para cada usuário
    # Retorna um vetor de tuplas [filme,nota]
