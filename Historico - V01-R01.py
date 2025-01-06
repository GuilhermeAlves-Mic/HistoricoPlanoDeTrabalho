import pandas as pd
import pyodbc
from datetime import datetime
import os
import re

pesos_dos_processos = {
    "Software":{
        "101": 0.05,  
        "102": 0.2,  
        "103": 0.05,
        "104": 0.15,
        "105": 0.4,
        "106": 0.15,},

    "Projeto":{
        "504": 0.3,
        "505": 0.15,
        "506": 0.15,
        "507": 0.2,
        "508": 0.2,
        "710": 0.1,
        "711": 0.25,
        "712": 0.25,
        "713": 0.25,
        "714": 0.15,},
    
    "Controle":{
        "810": 0.6,
        "811": 0.4,
        "820": 0.6,
        "821": 0.25,
        "822": 0.15,
        "830": 0.6,
        "831": 0.2,
        "832": 0.15,
        "833": 0.05,},

    "Supervisão":{
        "402": 0.3,
        "403": 0.2,
        "404": 0.3,
        "405": 0.1,
        "720": 0.6,
        "721": 0.1,
        "722": 0.1,
        "723": 0.2,
        "730": 0.6,
        "731": 0.1,
        "732": 0.05,
        "733": 0.05,
        "734": 0.2,
        "740": 0.8,
        "741": 0.2,
        "742": 0.31,
        "743": 0.32,
        "744": 0.2,
        "750": 0.5,
        "751": 0.15,
        "752": 0.05,
        "753": 0.1,
        "754":0.2,
        "760": 0.4,
        "761": 0.1,
        "762": 0.05,
         
        "1000": 0.1,},

    "G5":{
        "302": 0.35,
        "303": 0.35,
        "304": 0.1,
        "305": 0.2,
        "1000": 0.0,
    },
    'Estudo':{
        "501": 0.1,
        "502": 0.3,
        "503": 0.25,
        "504": 0.15,
        "509": 0.15,
        "510": 0.05,
        "1000":0.0
    }

    # Adicione outros IDs e pesos conforme necessário
}

maiores_porcentagens_processos = {}

def conectar_ao_banco():
    """
    Estabelece a conexão com o banco de dados MySQL usando o pyodbc.
    Retorna o objeto de conexão e o cursor.
    """
    try:
        # Defina os parâmetros de conexão
        conn = pyodbc.connect('DSN=db_bitrix;UID=guilherme.alves;PWD=Gui@2024;DATABASE=db_bitrix')
        cursor = conn.cursor()
        print("Conexão bem-sucedida com o banco de dados!")
        return conn, cursor
    except Exception as e:
        print(f"Erro ao conectar-se ao banco de dados: {e}")
        return None, None
    
def obter_peso_do_processo(comentario, especialidade):
    """
    Extrai o peso do processo com base no ID e na especialidade contidos no comentário.
    """
    match = re.search(r"#(\d+)", comentario)  # Procura o ID do processo no comentário
    if not match:
        #print(f"[DEBUG] Nenhum ID encontrado no comentário: '{comentario}'")
        return 0.2  # Peso padrão para casos sem ID

    processo_id = match.group(1)  # Captura o ID
    if especialidade not in pesos_dos_processos:
        #print(f"[DEBUG] Especialidade '{especialidade}' não encontrada no dicionário.")
        return 0.2  # Peso padrão para especialidade não encontrada

    peso = pesos_dos_processos[especialidade].get(processo_id, 0.2)  # Busca o peso
    #if peso == 0.2:
    #    print(f"[DEBUG] Processo '{processo_id}' não encontrado para especialidade '{especialidade}'.")
    return peso


def extrair_id_e_percentual(comentario):
    """Extrai o ID do processo e a última porcentagem de conclusão do comentário."""
    id_match = re.findall(r"#(\d+)", comentario)
    percent_match = re.findall(r"_(\d+)", comentario)

    if id_match:
        processo_id = id_match[-1]  # Último ID encontrado
        percentual = float(percent_match[-1]) if percent_match else 100.0  # Assume 100% se porcentagem não for encontrada
        return processo_id, percentual
    return None, 100.0  # Assume 100% se nenhum ID e porcentagem forem encontrados


def atualizar_maior_percentual(processo_id, percentual, data, tarefa_nome):
    """Atualiza o maior percentual encontrado para um ID de processo."""
    if processo_id in maiores_porcentagens_processos:
        # Verifica se a nova data é mais recente
        if data > maiores_porcentagens_processos[processo_id]['data']:
            maiores_porcentagens_processos[processo_id] = {'percentual': percentual, 'data': data}
    else:
        # Adiciona o primeiro registro para o ID
        maiores_porcentagens_processos[processo_id] = {'percentual': percentual, 'data': data}

def calcular_horas_restantes(horas_estimadas, comentarios, especialidade, nome_tarefa, datas):
    """
    Calcula as horas restantes considerando o maior percentual de cada ID de processo.
    Debitando as horas das horas estimadas.
    """
    horas_restantes = horas_estimadas
    maiores_percentuais = {}  # Armazena o maior percentual para cada ID de processo

    # 1ª Passagem: Identificar o maior percentual por ID
    for comentario, data in zip(comentarios, datas):
        if isinstance(comentario, str):
            processo_id, percentual = extrair_id_e_percentual(comentario)
            if processo_id is not None:
                # Atualiza apenas se o percentual ou a data for maior
                if processo_id not in maiores_percentuais or data > maiores_percentuais[processo_id]['data']:
                    maiores_percentuais[processo_id] = {'percentual': percentual, 'data': data}
        else:
            #print(f"[DEBUG] Comentário inválido: {comentario}")
            raise ValueError("Elemento de comentarios deve ser string.")

    # 2ª Passagem: Calcular as horas restantes usando os maiores percentuais
    for processo_id, info in maiores_percentuais.items():
        percentual = info['percentual']
        peso = obter_peso_do_processo(f"#{processo_id}", especialidade)  # Simula o comentário para obter o peso
        
        # Calcula as horas a serem debitadas considerando o percentual, peso e horas estimadas
        horas_debitadas = horas_estimadas * peso * (percentual / 100)
        horas_restantes -= horas_debitadas  # Subtrai as horas debitadas das horas estimadas

        print(f"[DEBUG] Tarefa: {nome_tarefa} | Processo: {processo_id} | Percentual: {percentual}% "
              f"| Peso: {peso} | Horas Debitadas: {horas_debitadas} | Horas Restantes Parciais: \n{horas_restantes}")

    print(f"[RESULTADO] Tarefa: {nome_tarefa} | Especialidade: {especialidade} | Horas Restantes: \n{horas_restantes}")
    return horas_restantes

def carregar_dados():
    G5 = "G5"
    query = f"""
    SELECT t.IdTarefa, t.title, 
           t.timeEstimate / 3600 AS horas_estimada, 
           t.deadline,
           tc.CommentText,
           tc.Seconds / 3600,
           tc.DateStart,

           CASE 
               WHEN t.description LIKE '%Software%' OR t.description LIKE '%CONTROLE%' OR t.description LIKE '%Controle%' THEN t.timeEstimate / 3600
               ELSE NULL 
           END AS tempo_estimado_software,

           CASE 
               WHEN t.description LIKE '%Supervisão%' THEN t.timeEstimate / 3600
               ELSE NULL 
           END AS tempo_estimado_supervisao,

           CASE 
               WHEN t.description LIKE '%{G5}%' THEN t.timeEstimate / 3600
               ELSE NULL 
           END AS tempo_estimado_G5,

           CASE 
               WHEN t.description LIKE '%IHM%' THEN t.timeEstimate / 3600
               ELSE NULL 
           END AS tempo_estimado_IHM,

           CASE 
               WHEN t.description LIKE '%Estudo%' OR t.description LIKE '%Projeto%' OR t.description LIKE '%Requisitos%' THEN t.timeEstimate / 3600
               ELSE NULL    
           END AS tempo_estimado_estudo,
           CASE 
               WHEN t.description LIKE '%Software%' OR t.description LIKE '%CONTROLE%' OR t.description LIKE '%Controle%' THEN "Software"
               WHEN t.description LIKE '%Supervisão%' THEN "Supervisão"
               WHEN t.description LIKE '%IHM%' THEN "IHM"
               WHEN t.description LIKE '%G5%' THEN "G5"
               WHEN t.description LIKE '%Estudo%' OR t.description LIKE '%Projeto%' OR t.description LIKE '%Requisitos%' THEN "Estudo"
               ELSE NULL 
           END AS Especialidade,
           WEEKOFYEAR(deadline) AS semana_do_ano,

           CASE 
               WHEN t.tags LIKE '%CAG%' THEN "CAG"
               WHEN t.tags LIKE '%A1%' THEN "A1"
               WHEN t.tags LIKE '%A2%' THEN "A2"
               WHEN t.tags LIKE '%DC%' THEN "DC"
               ELSE NULL
           END AS Eixo

    FROM tarefas t
    LEFT JOIN tempodecorrido tc ON t.IdTarefa = tc.IdTarefa
    WHERE t.timeEstimate IS NOT NULL
      AND t.deadline IS NOT NULL
      AND t.Atual IS NULL
      AND tc.Atual IS NULL
      AND (t.groupId = 796 OR t.groupId = 812)
      AND t.tags NOT LIKE '%OBR%'
      AND t.deadline >= CURDATE();
    """
    conn, cursor = conectar_ao_banco()
    if not conn or not cursor:
        print("Falha na conexão com o banco de dados.")
        return None
    
    cursor.execute(query)
    rows = cursor.fetchall()
    
    if not rows:
        print("Nenhuma linha foi retornada pela consulta SQL.")
        conn.close()
        return None

    tarefas_acumuladas = {}
    for row in rows:
        id_tarefa = row[0]
        comentario = row[4]
        horas_estimada = float(row[2]) if row[2] else None
        data_de_lancamento = row[6]

        # Inicializar as listas de comentários e datas com validação
        comentarios_tarefa = []
        datas_tarefa = []

        # Adicionar comentário apenas se for string válida
        if isinstance(comentario, str) and comentario:
            comentarios_tarefa.append(comentario)

        # Adicionar data apenas se for datetime válida
        if isinstance(data_de_lancamento, datetime):
            datas_tarefa.append(data_de_lancamento)

        # Inicializar as horas para cada especialidade usando o dicionário especialidade_horas
        especialidade_horas = {
            "Software": float(row[7]) if row[7] else None,
            "Supervisão": float(row[8]) if row[8] else None,
            "G5": float(row[9]) if row[9] else None,
            "IHM": float(row[10]) if row[10] else None,
            "Estudo": float(row[11]) if row[11] else None,
        }

        # Inicializa as chaves para cada tarefa e especialidade, caso não existam
        if id_tarefa not in tarefas_acumuladas:
            tarefas_acumuladas[id_tarefa] = {
                'IdTarefa': id_tarefa,
                'Tarefas': row[1],
                'Horas Estimadas': horas_estimada,
                'Deadline': row[3],
                'Semana': row[13],
                'Eixo': row[14],
                'Horas Restantes de Software': 0,
                'Horas Restantes de Supervisão': 0,
                'Horas Restantes de G5': 0,
                'Horas Restantes de IHM': 0,
                'Horas Restantes de Estudo': 0,
            }

        # Agrupa comentários e datas acumulados para a tarefa
        if id_tarefa in tarefas_acumuladas:
            comentarios_tarefa += [
                c for c in tarefas_acumuladas[id_tarefa].get("Comentarios", [])
                if isinstance(c, str) and c
            ]
            datas_tarefa += [
                d for d in tarefas_acumuladas[id_tarefa].get("Datas", [])
                if isinstance(d, datetime)
            ]

        # Salva os comentários e datas acumulados na tarefa
        tarefas_acumuladas[id_tarefa]["Comentarios"] = comentarios_tarefa
        tarefas_acumuladas[id_tarefa]["Datas"] = datas_tarefa

        # Atualizar as horas restantes para cada especialidade
        if especialidade_horas["Software"] is not None:
            horas = calcular_horas_restantes(especialidade_horas["Software"], comentarios_tarefa, "Software", row[1], datas_tarefa)
            #print (F"Tarefas {row[1]} | {horas} ")
            if horas is not None:
                tarefas_acumuladas[id_tarefa]['Horas Restantes de Software'] = horas

        if especialidade_horas["Supervisão"] is not None:
            horas = calcular_horas_restantes(especialidade_horas["Supervisão"], comentarios_tarefa, "Supervisão", row[1], datas_tarefa)
            if horas is not None:
                tarefas_acumuladas[id_tarefa]['Horas Restantes de Supervisão'] = horas

        if especialidade_horas["G5"] is not None:
            horas = calcular_horas_restantes(especialidade_horas["G5"], comentarios_tarefa, "G5", row[1], datas_tarefa)
            if horas is not None:
                tarefas_acumuladas[id_tarefa]['Horas Restantes de G5'] = horas
                #print (F"Tarefas {row[1]} | {horas} ")

        if especialidade_horas["IHM"] is not None:
            horas = calcular_horas_restantes(especialidade_horas["IHM"], comentarios_tarefa, "IHM", row[1], datas_tarefa)
            if horas is not None:
                tarefas_acumuladas[id_tarefa]['Horas Restantes de IHM'] = horas

        if especialidade_horas["Estudo"] is not None:
            horas = calcular_horas_restantes(especialidade_horas["Estudo"], comentarios_tarefa, "Estudo", row[1], datas_tarefa)
            if horas is not None:
                tarefas_acumuladas[id_tarefa]['Horas Restantes de Estudo'] = horas

    # Converter o dicionário de tarefas acumuladas para um DataFrame
    df = pd.DataFrame(list(tarefas_acumuladas.values()))
    
    conn.close()
    return df

def salvar_historico_na_planilha(df_novo):
    # Caminho para a área de trabalho
    area_de_trabalho = os.path.join(os.path.expanduser('~'), 'Desktop')
    nome_arquivo = os.path.join(area_de_trabalho, 'historico_horas_planejadas.xlsx')

    # Adicionar uma coluna com a data de registro
    df_novo['Data de Registro'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Debug: imprimir o DataFrame antes de salvar
    print(f"DataFrame final antes de salvar:\n{df_novo.head()}")

    # Se o arquivo Excel já existe, carregá-lo e preservar semanas anteriores e atuais
    if os.path.exists(nome_arquivo):
        df_existente = pd.read_excel(nome_arquivo, sheet_name='Histórico')

        # Verificar se o DataFrame existente tem dados e a coluna 'Semana' existe
        if not df_existente.empty and 'Semana' in df_existente.columns:
            # Criar chave única para identificar duplicatas
            df_existente['chave_tarefa'] = (
                df_existente['Tarefas'] + '_' +
                df_existente['Semana'].astype(str) + '_' 
            )
            df_novo['chave_tarefa'] = (
                df_novo['Tarefas'] + '_' +
                df_novo['Semana'].astype(str) + '_'
            )

            # Filtrar tarefas no df_novo que não estão no df_existente
            df_novo_filtrado = df_novo[~df_novo['chave_tarefa'].isin(df_existente['chave_tarefa'])]

            # Manter os dados das semanas anteriores
            df_existente_anteriores = df_existente[df_existente['Semana'] < datetime.now().isocalendar()[1]]

            # Concatenar dados antigos com novos dados filtrados
            df_final = pd.concat([df_existente_anteriores, df_novo_filtrado], ignore_index=True)

        else:
            # Se não houver dados ou a coluna 'Semana' não existir, apenas os novos dados
            df_final = df_novo  # Inclui todas as semanas futuras
    else:
        # Se o arquivo não existe, salvar os novos dados diretamente
        df_final = df_novo  # Inclui todas as semanas futuras

    # Escrever os dados no arquivo Excel (substituindo qualquer conteúdo anterior)
    with pd.ExcelWriter(nome_arquivo, mode='w', engine='openpyxl') as writer:
        df_final.to_excel(writer, sheet_name='Histórico', index=False)

    print(f"Histórico salvo com sucesso em {nome_arquivo}")

# Execução principal
if __name__ == "__main__":
    # Buscar horas planejadas da semana atual e posteriores
    df_horas_planejadas = carregar_dados()

    if df_horas_planejadas is not None and not df_horas_planejadas.empty:
        # Salvar o histórico na planilha
        salvar_historico_na_planilha(df_horas_planejadas)
    else:
        print("Nenhum dado encontrado para salvar.")