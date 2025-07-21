import pyodbc
import pandas as pd

# ✅ Configurações de conexão com o banco SQL do Smart
server = 'IP_DO_SERVIDOR'
database = 'NOME_DO_BANCO'  # normalmente Smart ou Smart_Restore
username = 'SEU_USUARIO'
password = 'SUA_SENHA'

# String de conexão
conn_str = (
    f'DRIVER={{SQL Server}};'
    f'SERVER={server};'
    f'DATABASE={database};'
    f'UID={username};'
    f'PWD={password}'
)

# ✅ Conectar ao banco
conn = pyodbc.connect(conn_str)

print("Conexão bem-sucedida ao banco Smart!")

# ✅ Consulta exemplo para obter dados dos pacientes e materiais
query = """
SELECT 
    PAC.PAC_REG AS Registro,
    PAC.PAC_NOME AS Paciente,
    PSV.PSV_NOME AS Medico,
    OSM.OSM_PROCED AS Procedimento,
    ISNULL(SMM.SMM_VLR_UNIT, 0) * ISNULL(SMM.SMM_QT, 0) AS Custo_Material,
    ISNULL(SMK.SMK_VLR, 0) AS Honorario_Medico,
    HSP.HSP_DTHR_ENT AS Data_Entrada,
    HSP.HSP_DTHR_SAI AS Data_Saida
FROM HSP
LEFT JOIN PAC ON PAC.PAC_REG = HSP.HSP_PAC_REG
LEFT JOIN OSM ON OSM.OSM_HSP_NUM = HSP.HSP_NUM
LEFT JOIN SMM ON SMM.SMM_HSP_NUM = HSP.HSP_NUM
LEFT JOIN SMK ON SMK.SMK_HSP_NUM = HSP.HSP_NUM
LEFT JOIN PSV ON PSV.PSV_COD = OSM.OSM_PSV_COD
WHERE HSP.HSP_DTHR_ENT BETWEEN '2025-05-01' AND '2025-05-31'
"""

# ✅ Ler dados da query
df = pd.read_sql(query, conn)

# ✅ Fechar conexão
conn.close()

print("Dados extraídos com sucesso!")

# ✅ Cálculo de tempo de bloco (diferença entre entrada e saída)
df['Tempo_Bloco_Horas'] = (df['Data_Saida'] - df['Data_Entrada']).dt.total_seconds() / 3600

# ✅ Parâmetros adicionais simulados
df['Taxa_Hospitalar'] = 5000  # Valor fixo para exemplo
df['Anestesista'] = 1500
df['OPME'] = 2000
df['Copa_Higiene'] = 100
df['Impostos'] = 500

# ✅ Cálculo dos custos
df['Custo_Total'] = (
    df['Custo_Material'] +
    df['Taxa_Hospitalar'] +
    df['Anestesista'] +
    df['OPME'] +
    df['Copa_Higiene'] +
    df['Impostos']
)

# ✅ Receita Total (Honorário + Taxa Hospitalar)
df['Receita_Total'] = df['Honorario_Medico'] + df['Taxa_Hospitalar']

# ✅ Lucro
df['Lucro'] = df['Receita_Total'] - df['Custo_Total']

# ✅ Ganho por hora de bloco
df['Ganho_Hora_Bloco'] = df['Lucro'] / df['Tempo_Bloco_Horas']

# ✅ Exibir resultado
print(df)

# ✅ Exportar para Excel
df.to_excel("Bussola_Financeira_Saida.xlsx", index=False)

print("\nRelatório 'Bussola_Financeira_Saida.xlsx' gerado com sucesso!")
