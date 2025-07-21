import pandas as pd
import pyodbc
import os
from datetime import datetime
import logging
import tkinter as tk
from tkinter import messagebox
from tkinter import filedialog
from tkcalendar import DateEntry

# 🔧 Configuração de log
logging.basicConfig(
    filename='log_bussola.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# 📦 Conexão com SQL Server
def conectar():
    try:
        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=192.168.0.10;'
            'DATABASE=Smart;'
            'UID=sa;'
            'PWD=Forma1465!@#'
        )
        logging.info("Conexão bem-sucedida.")
        return conn
    except Exception as e:
        logging.error(f"Erro na conexão: {e}")
        messagebox.showerror("Erro", f"Erro na conexão: {e}")
        return None

# 📄 Geração de relatório
def gerar_relatorio(data_ini, data_fim):
    conn = conectar()
    if not conn:
        return

try:
        query = f"""
        WITH Cirurgias AS (
            SELECT 
                RCI_OSM_NUM AS Numero_OSM,
                RCI_OSM_SERIE AS Serie_OSM,
                RCI_SMK_COD AS Codigo_Cirurgia
            FROM RCI
            WHERE RCI_OSM_NUM IS NOT NULL AND RCI_OSM_SERIE IS NOT NULL
            GROUP BY RCI_OSM_NUM, RCI_OSM_SERIE, RCI_SMK_COD
        ),
        Custos_SMM AS (
            SELECT
                SMM.SMM_HSP_NUM AS Atendimento,
                SMM.SMM_OSM AS Numero_OSM,
                SMM.SMM_OSM_SERIE AS Serie_OSM,
                SMM.SMM_COD AS Codigo,
                SMK.SMK_ROT AS Classificacao,
                SUM(SMM.SMM_QT * SMM.SMM_VLR) AS Valor
            FROM SMM
            LEFT JOIN SMK ON SMK.SMK_TIPO = SMM.SMM_TPCOD AND SMK.SMK_COD = SMM.SMM_COD
            WHERE SMM.SMM_DTHR_EXEC BETWEEN '{data_ini}' AND '{data_fim}'
            GROUP BY SMM.SMM_HSP_NUM, SMM.SMM_OSM, SMM.SMM_OSM_SERIE, SMM.SMM_COD, SMK.SMK_ROT
        ),
        Custos_Com_Cirurgia AS (
            SELECT 
                C.*,
                CASE WHEN CIR.Codigo_Cirurgia IS NOT NULL THEN 1 ELSE 0 END AS Eh_Cirurgia
            FROM Custos_SMM C
            LEFT JOIN Cirurgias CIR ON CIR.Numero_OSM = C.Numero_OSM AND CIR.Serie_OSM = C.Serie_OSM AND CIR.Codigo_Cirurgia = C.Codigo
        ),
        Custos_Pivot AS (
            SELECT
                Atendimento,
                Numero_OSM,
                Serie_OSM,
                SUM(CASE WHEN Classificacao = 'PERNOITE' THEN Valor ELSE 0 END) AS Valor_Pernoite,
                SUM(CASE WHEN Classificacao = 'VALOR HOSPITAL' THEN Valor ELSE 0 END) AS Valor_Hospital,
                SUM(CASE WHEN Classificacao = 'ANESTESISTA' THEN Valor ELSE 0 END) AS Valor_Anestesia,
                SUM(CASE WHEN Eh_Cirurgia = 1 THEN Valor ELSE 0 END) AS Valor_Cirurgia
                SUM(CASE WHEN Classificacao = 'CNV' THEN Valor ELSE 0 END) AS Valor_CNV,
                SUM (CASE WHEN Classificacao = 'ANEST_12%' THEN valor else 0 END) As Anest_12,
                SUM (CASE WHEN Classificacao = 'IMPOST_14,33%' THEN valor else 0 END) As Impost_1433,
                SUM(CASE WHEN Classificacao = 'IMPOST_16,5%' THEN Valor ELSE 0 END) AS Impost_1650
            FROM Custos_Com_Cirurgia
            GROUP BY Atendimento, Numero_OSM, Serie_OSM
        ),
        Materiais_Real AS (
            SELECT
                TRY_CAST(s.SMA_HSP_NUM AS INT) AS Atendimento,
                s.SMA_PAC_REG AS Registro,
                SUM(
                  CASE WHEN m.MMA_TIPO_OPERACAO = 'S2' THEN m.MMA_QTD * mat.MAT_VLR_PM
                       WHEN m.MMA_TIPO_OPERACAO = 'E4' THEN -m.MMA_QTD * mat.MAT_VLR_PM
                       ELSE 0 END
                ) AS Valor_Materiais
            FROM MMA m
            JOIN SMA s ON m.MMA_SMA_SERIE = s.SMA_SERIE AND m.MMA_SMA_NUM = s.SMA_NUM
            JOIN MAT mat ON m.MMA_MAT_COD = mat.MAT_COD
            WHERE m.MMA_DATA_MOV BETWEEN '{data_ini}' AND '{data_fim}'
              AND m.MMA_TIPO_OPERACAO IN ('S2','E4')
            GROUP BY TRY_CAST(s.SMA_HSP_NUM AS INT), s.SMA_PAC_REG
        ),
        Cabecalho AS (
            SELECT 
                RCI.RCI_HSP_NUM AS Atendimento,
                RCI.RCI_PAC_REG AS Registro,
                MAX(PAC.PAC_NOME) AS Paciente,
                MAX(CNV.CNV_NOME) AS Convenio,
                MAX(ISNULL(SMK.SMK_ROT, 'SEM PROCEDIMENTO')) AS Procedimento,
                MAX(ISNULL(PSV1.PSV_APEL, ISNULL(PSV2.PSV_APEL, 'SEM MÉDICO'))) AS Medico,
                MIN(RCI.RCI_DTHR_INI) AS Data_Entrada,
                MAX(RCI.RCI_DTHR_FIM) AS Data_Saida,
                RCI.RCI_OSM_NUM AS Numero_OSM,
                RCI.RCI_OSM_SERIE AS Serie_OSM
            FROM RCI
            LEFT JOIN PAC ON PAC.PAC_REG = RCI.RCI_PAC_REG
            LEFT JOIN CNV ON CNV.CNV_COD = RCI.RCI_CNV_COD
            LEFT JOIN OSM ON OSM.OSM_NUM = RCI.RCI_OSM_NUM AND OSM.OSM_SERIE = RCI.RCI_OSM_SERIE
            LEFT JOIN PSV AS PSV1 ON PSV1.PSV_COD = RCI.RCI_PSV_SOLIC
            LEFT JOIN PSV AS PSV2 ON PSV2.PSV_COD = RCI.RCI_PSV_COD
            LEFT JOIN SMK ON SMK.SMK_COD = RCI.RCI_SMK_COD AND SMK.SMK_TIPO = RCI.RCI_SMK_TIPO
            WHERE RCI.RCI_DTHR_INI BETWEEN '{data_ini}' AND '{data_fim}'
            GROUP BY RCI.RCI_HSP_NUM, RCI.RCI_PAC_REG, RCI.RCI_OSM_NUM, RCI.RCI_OSM_SERIE
        )
        SELECT 
            C.Atendimento,
            C.Registro,
            C.Paciente,
            C.Convenio,
            C.Procedimento,
            C.Medico,
            C.Data_Entrada,
            C.Data_Saida,
            ISNULL(CP.Valor_Pernoite, 0) AS Valor_Pernoite,
            ISNULL(CP.Valor_Hospital, 0) AS Valor_Hospital,
            ISNULL(CP.Valor_Anestesia, 0) AS Valor_Anestesia,
            ISNULL(CP.Valor_Cirurgia, 0) AS Valor_Cirurgia,
            ISNULL(MR.Valor_Materiais, 0) AS Valor_Materiais,
            ISNULL(CP.Valor_CNV, 0) AS Valor_CNV,
            ISNULL(CP.Anest_12, 0) AS Anest_12,
            ISNULL(CP.Impost_1433, 0) AS Impost_1433,
            ISNULL(CP.Impost_1650, 0) AS Impost_1650
            
        FROM Cabecalho C
        LEFT JOIN Custos_Pivot CP ON CP.Atendimento = C.Atendimento AND CP.Numero_OSM = C.Numero_OSM AND CP.Serie_OSM = C.Serie_OSM
        LEFT JOIN Materiais_Real MR ON MR.Atendimento = C.Atendimento AND MR.Registro = C.Registro
        ORDER BY C.Atendimento
        """
        df = pd.read_sql(query, conn)

        # 🧮 Regras e cálculos
        df['Valor_Materiais'] = df['Valor_Materiais'].apply(lambda x: 223.66 if x == 0 else x)
        df['Tempo_BC'] = (df['Data_Saida'] - df['Data_Entrada']).dt.total_seconds() / 3600
        df['Hora_BC'] = df['Tempo_BC'].round(2)
        df['Copa_Higiene'] = df['Hora_BC'].apply(lambda x: 100 if x >= 1 else 50)

        df['Imposto'] = (df['Valor_Pernoite'] + df['Valor_Hospital'] + df['Valor_Cirurgia']) * 0.16
        df['Total_Liquido'] = df['Valor_Pernoite'] + df['Valor_Hospital'] + df['Valor_Cirurgia'] + df['Valor_CNV'] + df['Anest_12'] + df['Impost_1433'] +  df['Impost_1650']
        df['Total_Custos'] = df['Valor_Materiais'] + df['Copa_Higiene'] + df['Imposto']
        df['Imposto_Anestesia'] = df.apply(
            lambda row: row['Valor_Anestesia'] * 0.12 if row['Medico'].strip().upper() == 'FERNANDO AMARAL' else 0, axis=1
        )
        df['Total_Ganho'] = df['Total_Liquido'] - df['Total_Custos']
        df['Ganho_Por_Hora'] = df.apply(
            lambda row: (row['Total_Liquido'] / row['Hora_BC']) if row['Hora_BC'] > 0 else 0, axis=1
        )

        # 💾 Exportar
        pasta = os.path.join(os.getcwd(), "relatorios")
        os.makedirs(pasta, exist_ok=True)
        nome = f"Relatorio_Bussola_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        caminho = os.path.join(pasta, nome)

        colunas_final = [
            'Registro', 'Paciente', 'Convenio', 'Procedimento', 'Medico',
    'Valor_Pernoite', 'Valor_Hospital', 'Valor_Cirurgia', 'Valor_CNV', 
    'Imposto', 'Anest_12', 'Impost_1433', 'Impost_1650', # Novas colunas inseridas após 'Imposto'
    'Valor_Materiais', 'Copa_Higiene', 'Valor_Anestesia', 'Imposto_Anestesia',
    'Total_Custos', 'Total_Liquido', 'Total_Ganho',
    'Data_Entrada', 'Data_Saida', 'Tempo_BC', 'Hora_BC', 'Ganho_Por_Hora'
        ]
        df = df[colunas_final]

        # 📊 Exportação com formatação
        try:
            with pd.ExcelWriter(caminho, engine='xlsxwriter') as writer:
                df.to_excel(writer, sheet_name='Resumo', index=False)
                workbook = writer.book
                worksheet = writer.sheets['Resumo']

                formato_real = workbook.add_format({'num_format': 'R$ #,##0.00'})

                for idx, col in enumerate(df.columns):
                    if col not in ['Registro', 'Paciente', 'Convenio', 'Procedimento', 'Medico', 'Data_Entrada', 'Data_Saida', 'Tempo_BC', 'Hora_BC']:
                        worksheet.set_column(idx, idx, 18, formato_real)
                    else:
                        worksheet.set_column(idx, idx, 18)

            logging.info(f"Relatório exportado com êxito: {caminho}")
            messagebox.showinfo("Sucesso", f"Relatório exportado com êxito!\n\nArquivo salvo em:\n{caminho}")

        except Exception as e:
            logging.error(f"Erro na exportação do relatório: {e}")
            messagebox.showerror("Erro", f"Erro na exportação do relatório: {e}")
            
# 🖼️ Interface gráfica
def iniciar_interface():
    janela = tk.Tk()
    janela.title("Bússola Financeira")
    janela.geometry("420x220")

    tk.Label(janela, text="Data Início (DD-MM-AAAA):").pack()
    data_inicio = DateEntry(janela, date_pattern='dd-mm-yyyy')
    data_inicio.pack()

    tk.Label(janela, text="Data Fim (DD-MM-AAAA):").pack()
    data_fim = DateEntry(janela, date_pattern='dd-mm-yyyy')
    data_fim.pack()

    def testar():
        if conectar():
            messagebox.showinfo("Conexão", "Conexão bem sucedida!")

    def executar():
        gerar_relatorio(data_inicio.get_date(), data_fim.get_date())

    def sair():
        if messagebox.askyesno("Sair", "Deseja realmente sair?"):
            janela.destroy()

    tk.Button(janela, text="Testar Conexão", command=testar).pack(pady=5)
    tk.Button(janela, text="Exportar Relatório", command=executar).pack(pady=5)
    tk.Button(janela, text="Sair", command=sair).pack(pady=5)

    janela.mainloop()

# ▶️ Iniciar
if __name__ == "__main__":
    iniciar_interface()