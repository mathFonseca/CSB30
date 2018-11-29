import psycopg2
import numpy as np

# Parte 1 - Conexão com o banco de dados.
try:
    conn = psycopg2.connect("dbname='1802BandoDeDados' user='1802BandoDeDados' host='200.134.10.32' password='803322'")
except Exception as error:
    print("Falha na conexão com o banco de dados.")
    print(error)

# -----
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
    print("Criando a view número por artista")
    cur = conn.cursor()
    consulta = "CREATE OR REPLACE VIEW Numero_Artista AS SELECT -1+ROW_NUMBER() OVER (ORDER BY Artista_Musical) AS Numero, ID FROM Artista_Musical;"
    cur.execute(consulta)
    cur.close()

    # A VIEW criada começa com os números de [1...] e não em [0...]


def criaViewNumeroFilmes(conn):
    # Cria a VIEW que associa um número único para cada filme cadastrado.
    print("Criando a view número por filme")
    cur = conn.cursor()
    consulta = str("CREATE or REPLACE VIEW Numero_filmes AS SELECT -1+ROW_NUMBER() OVER(ORDER BY Filmes) AS Numero, ID FROM Filmes;")
    cur.execute(consulta)
    cur.close()
    # A VIEW criada começa com os números de [1...] e não em [0...]


def criaViewNumeroUsuario(conn):
    # Cria a VIER que associa um número único para cada usuário cadastrado.
    print("Criando a view número por usuário")
    cur = conn.cursor()
    consulta = str("CREATE or REPLACE VIEW Numero_Usuario AS SELECT -1+ROW_NUMBER() OVER(ORDER BY Pessoa) AS Numero, Login FROM Pessoa;")
    cur.execute(consulta)
    cur.close()


def obtemNomeUsuario(user, conn):
    # Obtem o nome do usuario a partir da view numero_usuario
    cur = conn.cursor()
    consulta = str("SELECT P.nome_completo FROM Pessoa P, Numero_usuario N WHERE N.numero = user AND P.login = N.login;")
    consulta = consulta.replace("user", str(user))
    cur.execute(consulta)
    nome_completo = cur.fetchall()
    cur.close()
    return str(nome_completo)


def criaMatrizUsuarioFilme(conn):
    # Cria a matriz no modelo matriz[usuario][filme].
    print("Criando a matriz [usuario][filme]")
    numero_usuarios = obtemNumeroDeUsuarios(conn)
    numero_filmes = obtemNumeroDeFilmes(conn)
    matriz_usuario_filme = np.zeros((numero_usuarios, numero_filmes))
    return matriz_usuario_filme


def criaMatrizUsuarioUsuario(conn):
    # Cria a matriz no modelo matriz[usuario][usuario]
    print("Criando a matriz [usuario][usuario]")
    numero_usuarios = obtemNumeroDeUsuarios(conn)
    matriz_usuario_usuario = np.zeros((numero_usuarios, numero_usuarios))
    return matriz_usuario_usuario


def preencheMatrizUsuarioFilme(matriz_usuario_filme, numero_usuarios, conn):
    # Preenche a matriz (no modeo matriz[usuario][filme]) com as informaçẽos do banco de dados.
    print("Preenchendo a matriz [usuario][filme]")
    cur = conn.cursor()
    for user in range(0, numero_usuarios):
        consulta = "SELECT A.Numero AS Numero_Filme, L.Nota AS Nota FROM Like_Filmes L, Numero_Filmes A, Numero_Usuario U WHERE U.Numero = user AND U.Login = L.login AND L.id = A.id;"
        consulta = consulta.replace("user", str(user))
        cur.execute(consulta)
        nota = cur.fetchall()
        for numero_filme in nota:
            matriz_usuario_filme[user][numero_filme[0]] = numero_filme[1]
    cur.close()
    return matriz_usuario_filme


def normalizaMatrizUsuarioFilme(matriz_usuario_filme, numero_usuarios, numero_filmes, conn):
    # Normaliza os valores da matriz usuario_filme.
    # Se nota 5 ou 4 -> nota = 2;
    # Se nota 3, 2 ou 1 -> nota = 1;
    # Se nota 0 (não viu) -> nota = 0;
    print("Normalizando a matriz [usuario][filme]")
    for i in range(0, numero_usuarios):
        for j in range(0, numero_filmes):
            if matriz_usuario_filme[i][j] == 5 or matriz_usuario_filme[i][j] == 4:
                matriz_usuario_filme[i][j] = 2
            elif matriz_usuario_filme[i][j] == 3 or matriz_usuario_filme[i][j] == 2:
                matriz_usuario_filme[i][j] = 1
    return matriz_usuario_filme


def calculaSimilaridade(usuarioA, usuarioB, matriz_usuarios_filmes, numero_filmes, conn):
    # Busca a similaridade de filmes entre dois usuários.
    # Gostos em comum são a mesma nota (com exceção do 0).
    # Notas diferentes não pontuam.
    filmes_comum = 0
    filmes_total = 0
    for filme in range(0, numero_filmes):
        if(matriz_usuarios_filmes[usuarioA][filme] == matriz_usuarios_filmes[usuarioB][filme]) and (matriz_usuarios_filmes[usuarioA][filme] != 0 or matriz_usuarios_filmes[usuarioB][filme] != 0):
            # Caso as notas são as mesmas (e não são zero)
            filmes_comum += 1
            filmes_total += 1
        if(matriz_usuarios_filmes[usuarioA][filme] != matriz_usuarios_filmes[usuarioB][filme]) and (matriz_usuarios_filmes[usuarioA][filme] != 0 or matriz_usuarios_filmes[usuarioB][filme] != 0):
            # Caso as notas são diferentes (e ao menos uma não é zero)
            filmes_total += 1
    if filmes_total == 0:
        # Quando os usuários não tem nada em comum (Sei lá, acontece algumas vezes)
        return 0

    similaridade = (filmes_comum / filmes_total) * 100
    similaridade = round(similaridade, 3)
    return similaridade


def preencheMatrizUsuarioUsuario(matriz_usuario_usuario, matriz_usuarios_filmes, numero_usuarios, numero_filmes, conn):
    # Preenche a similaridade entre todos os usuários por todos os usuários.
    similaridade = 0
    for i in range(0, numero_usuarios):
        for j in range(i + 1, numero_usuarios):
            # A similaridade entre o mesmo usuáro é 100%.
            matriz_usuario_usuario[i][i] = 1
            similaridade = calculaSimilaridade(i, j, matriz_usuarios_filmes, numero_filmes, conn)
            # print(similaridade)
            matriz_usuario_usuario[i][j] = similaridade
            matriz_usuario_usuario[j][i] = similaridade
    return matriz_usuario_usuario

# ------
# Fim das definições das funções.

# Seção 1 - Criação das Views.


criaViewNumeroFilmes(conn)
criaViewNumeroUsuario(conn)
# criaViewNumeroArtista(conn)
# Por enquanto, desnecessário.

# Seção 2 - Obtem medidas para a criação da matriz.

numero_usuario = obtemNumeroDeUsuarios(conn)
numero_filme = obtemNumeroDeFilmes(conn)
# numero_artista = obtemNumeroDeArtistas(conn)
# Por enquanto, desncessário.

# Seção 3 - Criação das matrizes.

matriz_usuario_filme = criaMatrizUsuarioFilme(conn)
matriz_usuario_usuario = criaMatrizUsuarioUsuario(conn)
# Seção 4 - Preenche as matrizes criadas.

matriz_usuario_filme = preencheMatrizUsuarioFilme(matriz_usuario_filme, numero_usuario, conn)
matriz_usuario_filme = normalizaMatrizUsuarioFilme(matriz_usuario_filme, numero_usuario, numero_filme, conn)
matriz_usuario_usuario = preencheMatrizUsuarioUsuario(matriz_usuario_usuario, matriz_usuario_filme, numero_usuario, numero_filme, conn)

# print(matriz_usuario_usuario[10][4])
# Seção 5 - Seção de Testes
"""for filme in range(0, numero_filme):
    if(matriz_usuario_filme[29][filme] != 0):
        print(filme)"""

menu = True
while(menu):
    print("Bem vindo ao menu. \n1 - Pesquisar similaridade \n2 - Sair \nDigite a opção desejada:")
    option = input()
    if option == "1":
        print("Digite os números dos usuários que se deseja saber a similaridade.")
        userA = input("Usuário A: ")
        userB = input("Usuário B: ")
        similaridade = matriz_usuario_usuario[int(userA)][int(userB)]
        nome_A = obtemNomeUsuario(int(userA), conn)
        nome_B = obtemNomeUsuario(int(userB), conn)
        print("A similariade entre " + nome_A + " e " + nome_B + " é de " + str(similaridade))
    elif(option == "2"):
        print("Saindo.")
        menu = False
