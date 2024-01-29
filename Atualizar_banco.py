#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import datetime as dt
import numpy as np
import psycopg2 as db
import os
import time
import re
import pymsteams as teams
import functools
import operator

hoje = dt.datetime.now()
dt_hoje = hoje.strftime('%Y-%m-%d')
este_mes = hoje.replace(day=1)
dt_este_mes = este_mes.strftime('%Y-%m-%d')

###===================================================CRIA CONEXÃO COM O CANAL TEAMS===================================================================

canal_teams = "{link webhook}"

msg_teams = teams.connectorcard(canal_teams)
msg_teams.text("# **Informativo**")
msg_teams.color('#13273c')
msg_teams1 = teams.connectorcard(canal_teams)
msg_teams1.text("# **Informativo**")
msg_teams1.color('#faaf3b')
msg_teams2 = teams.connectorcard(canal_teams)
msg_teams2.text("# **Informativo**")
msg_teams2.color('#faaf3b')
msg_teams3 = teams.connectorcard(canal_teams)
msg_teams3.text("# **Informativo**")
msg_teams3.color('#00c0b6')
msg_teams4 = teams.connectorcard(canal_teams)
msg_teams4.text("# **Informativo**")
msg_teams4.color('#00c0b6')
msg_teams5 = teams.connectorcard(canal_teams)
msg_teams5.text("# **AVISO**")
msg_teams5.color('#ff0000')

###=====================================LÊ OS ARQUIVOS CSV E CRIA CONEXÃO COM O BANCO=============================================

arquivos = os.listdir(f'{Pasta dos Arquivos}')
cadastro_arq = max([os.fspath(arquivo) for arquivo in arquivos if "Cadastros" in arquivo])
historico_arq = max([os.fspath(arquivo) for arquivo in arquivos if "Historico_Atendimento" in arquivo])
rco_a_arq = max([os.fspath(arquivo) for arquivo in arquivos if "RCO_A" in arquivo])
rco_b_arq = max([os.fspath(arquivo) for arquivo in arquivos if "RCO_B" in arquivo])

con = db.conexao(dbname="banco", user="usuario", password="senha", host="host", port = 5432)
cursor = con.cursor()


### =================================================HISTÓRICO DE ATENDIMENTO==============================================================

historico = pd.read_csv(f'{Pasta dos Arquivos}\\{historico_arq}', sep="|" , dtype=str, decimal=',', encoding='ANSI')
historico = historico[['Franquia','Cliente','Atendente','Data','Tipo','Assunto','Historico','ID']]
historico = historico.apply(lambda x: x.str.strip())

cursor.execute('SELECT * FROM sch_solucoes.tb_historico')
df_con = cursor.fetchall()
historico_conexao = pd.DataFrame(list(df_con), columns = ["chcd_franquia","incd_empcod","binu_cpfatendente","dtdt_atendimento",
                                                           "vcds_tipo","vcds_assunto","vctx_mensagem","inid_atendimentoconnect"])

historico_conexao['inid_atendimentoconnect'] = historico_conexao['inid_atendimentoconnect'].astype(str)
historico_conexao['inid_atendimentoconnect'] = historico_conexao['inid_atendimentoconnect'].str.strip()
historico['ID'] = historico['ID'].astype(str)
historico['ID'] = historico['ID'].str.strip()

historico = historico[~(historico['ID'].isin(historico_conexao['inid_atendimentoconnect'])) | (historico['ID'].isna())]
historico = historico[~historico['Data'].isna()]

historico['Franquia'].fillna("", inplace = True)
historico['Cliente'].fillna(0, inplace = True)
historico['Atendente'].fillna(0, inplace = True)
historico['Tipo'].fillna("", inplace = True)
historico['Assunto'].fillna("", inplace = True)
historico['Historico'].fillna("", inplace = True)

historico['Cliente'] = historico['Cliente'].astype(np.int64)
historico['Atendente'] = historico['Atendente'].astype(np.int64)
historico['Data'] = pd.to_datetime(historico['Data'], errors='ignore', dayfirst = True)
historico['ID'] = historico['ID'].astype(np.int64)
historico.sort_values('Data', ascending = True, inplace = True, ignore_index = True)
historico.drop_duplicates(subset = 'ID', keep = 'first', inplace = True, ignore_index = True)

colunas_historico = list(historico.columns)
historico = historico.to_numpy()

for linha in range(len(historico)):
    if historico[linha][6] != None:
        historico[linha][6] = re.compile("'").sub("&#39;",str(historico[linha][6]))
        historico[linha][6] = re.compile('"').sub("&#34;",str(historico[linha][6]))

historico = pd.DataFrame(historico, columns = colunas_historico)

historico['Cliente'] = historico['Cliente'].astype(np.int64)
historico['Atendente'] = historico['Atendente'].astype(np.int64)
historico['Data'] = pd.to_datetime(historico['Data'], errors='ignore', dayfirst = True)
historico['ID'] = historico['ID'].astype(np.int64)

hist_dias = historico.copy()
registros_hist = len(historico)
hist_dias = hist_dias[['Data', 'ID']]
hist_dias = hist_dias.sort_values('Data', ascending = True, ignore_index = True)
hist_dias['Data'] = hist_dias['Data'].dt.strftime("%d/%m/%Y")
hist_dias = hist_dias.groupby(by='Data', as_index = False, sort = False).count()

erros_hist = {}
lista_hist_2val = []
contador_hist = 0
for reg in range(len(historico)):
    try:
        cursor.execute(f"""INSERT INTO sch_solucoes.tb_historico(chcd_franquia,incd_empcod,binu_cpfatendente,dtdt_atendimento,vcds_tipo,vcds_assunto,vctx_mensagem,inid_atendimentoconnect)
                            VALUES('{historico.loc[reg,'Franquia']}',{historico.loc[reg,'Cliente']},{historico.loc[reg,'Atendente']},'{historico.loc[reg,'Data']}','{historico.loc[reg,'Tipo']}','{historico.loc[reg,'Assunto']}','{historico.loc[reg,'Historico']}',{historico.loc[reg,'ID']})""")
        con.commit()
        contador_hist +=1
    except:
        con.rollback()
        erros_hist[historico.loc[reg,'ID']] = historico.loc[reg,'Cliente']
        lista_hist_2val.append(historico.loc[reg,'ID'])

atualiza_hist = dt.datetime.now()
atualiza_hist1 = atualiza_hist.strftime("às %H:%M:%S do dia %d/%m/%Y")
      
texto_hist = []
card_teams = teams.cardsection()
card_teams.title(">HISTÓRICO DE ATENDIMENTO")
if contador_hist==1:
    card_teams.activityTitle(str(f'{contador_hist} registro inserido.\n\n'))
    for d in range(len(hist_dias)):
        dia = hist_dias['Data'][d]
        atend = hist_dias['ID'][d]
        texto_hist.append(str("- ")+str(dia)+str(": ")+str(atend)+str(" atendimento")+str('\r'))
    texto_limpo = functools.reduce(operator.add, (texto_hist))
    card_teams.activityText(texto_limpo)
elif contador_hist>1:
    card_teams.activityTitle(str(f'{contador_hist} registros inseridos.\n\n'))
    for d in range(len(hist_dias)):
        dia = hist_dias['Data'][d]
        atend = hist_dias['ID'][d]
        texto_hist.append(str("- ")+str(dia)+str(": ")+str(atend)+str(" atendimentos")+str('\r'))
    texto_limpo = functools.reduce(operator.add, (texto_hist))
    card_teams.activityText(texto_limpo)
else:
    card_teams.activityTitle(str(f"""\nNenhum registro a ser inserido.\n\n Histórico Completo!!!\n\n"""))
card_teams.text(str(f'### Atualizado {atualiza_hist1}.\n\n'))


### ========================================================RCO A=====================================================================

rco_a = pd.read_csv(f'{Pasta dos Arquivos}\\{rco_a_arq}', sep="|", dtype=str, decimal=',', encoding='ANSI')
rco_a= rco_a[['Franquia','Codigo','Nome_Fantasia','CPF_CNPJ','Data_Inclusao','Valor','incSerasa','DataDeb','ID','Contrato']]
rco_a = rco_a.apply(lambda x: x.str.strip())

cursor.execute('SELECT * FROM sch_solucoes.tb_rco_a')
df_con = cursor.fetchall()
rco_a_conexao = pd.DataFrame(list(df_con), columns = ["Franquia","Codigo","Nome_Fantasia","CPF_CNPJ","Data_Inclusao",
                                                        "Valor","incSerasa","DataDeb","ID","Contrato"])

rco_a_conexao['ID'] = rco_a_conexao['ID'].astype(str)
rco_a_conexao['ID'] = rco_a_conexao['ID'].str.strip()
rco_a['ID'] = rco_a['ID'].astype(str)
rco_a['ID'] = rco_a['ID'].str.strip()

rco_a = rco_a[~(rco_a['ID'].isin(rco_a_conexao['ID'])) | (rco_a['ID'].isna())]

rco_a['Franquia'].fillna("", inplace = True)
rco_a['Codigo'].fillna(0, inplace = True)
rco_a['Nome_Fantasia'].fillna("", inplace = True)
rco_a['CPF_CNPJ'].fillna(0, inplace = True)
rco_a['Data_Inclusao'].fillna("", inplace = True)
rco_a['Valor'].fillna(0, inplace = True)
rco_a['incSerasa'].fillna("", inplace = True)
rco_a['DataDeb'].fillna("", inplace = True)
rco_a['Contrato'].fillna("", inplace = True)
rco_a.replace("'","&#39;", inplace=True, regex = True)
rco_a.replace('"',"&#34;", inplace=True, regex = True)

rco_a['ID']  = rco_a['ID'].astype(str)
rco_a['ID']  = rco_a['ID'].str.strip()
rco_a['Data_Inclusao']  = rco_a['Data_Inclusao'].astype(str)
rco_a['Data_Inclusao']  = rco_a['Data_Inclusao'].str.strip()
rco_a['DataDeb']  = rco_a['DataDeb'].astype(str)
rco_a['DataDeb']  = rco_a['DataDeb'].str.strip()
rco_a['Data_Inclusao'] = pd.to_datetime(rco_a['Data_Inclusao'], errors='ignore', dayfirst = True)
rco_a['DataDeb'] = pd.to_datetime(rco_a['DataDeb'], errors='ignore', dayfirst = True)
rco_a['Valor'] = rco_a['Valor'].astype(str)
rco_a['Valor'] = rco_a['Valor'].str.strip()
rco_a['Valor'] = rco_a['Valor'].apply(lambda x: str(x).replace(",","."))

rco_a['Codigo'] = rco_a['Codigo'].astype(np.int64)
rco_a['CPF_CNPJ'] = rco_a['CPF_CNPJ'].astype(np.int64)
rco_a['Valor'] = rco_a['Valor'].astype(np.float64)
rco_a['ID'] = rco_a['ID'].astype(np.int64)
rco_a = rco_a.sort_values(by = ['Data_Inclusao','ID'], ascending = True, ignore_index = True)
rco_a.drop_duplicates(subset = 'ID', keep = 'first', inplace = True, ignore_index = True)

registros_rco_a = len(rco_a)
erros_rco_a = {}
lista_rcoa_2val = []
contador_rco_a = 0
for reg in range(len(rco_a)):
    try:
        cursor.execute(f"""INSERT INTO sch_solucoes.tb_rco_a(franquia,codigo,nome_fantasia,cpf_cnpj,data_inclusao,valor,incserasa,datadeb,id,contrato)
                            VALUES('{rco_a.loc[reg,'Franquia']}',{rco_a.loc[reg,'Codigo']},'{rco_a.loc[reg,'Nome_Fantasia']}',{rco_a.loc[reg,'CPF_CNPJ']},'{rco_a.loc[reg,'Data_Inclusao']}',{rco_a.loc[reg,'Valor']},'{rco_a.loc[reg,'incSerasa']}','{rco_a.loc[reg,'DataDeb']}',{rco_a.loc[reg,'ID']},'{rco_a.loc[reg,'Contrato']}')""")
        con.commit()
        contador_rco_a+=1
    except:
        con.rollback()
        erros_rco_a[rco_a.loc[reg,'ID']] = rco_a.loc[reg,'Codigo']
        lista_rcoa_2val.append(rco_a.loc[reg,'ID'])

atualiza_rcoa = dt.datetime.now()
atualiza_rcoa1 = atualiza_rcoa.strftime("às %H:%M:%S do dia %d/%m/%Y")

texto_rco_a = []
card_teams1 = teams.cardsection()
card_teams1.title(">RCO A")
if contador_rco_a==1:
    card_teams1.activityTitle(str(f'{contador_rco_a} registro inserido.'))
elif contador_rco_a>1:
    card_teams1.activityTitle(str(f'{contador_rco_a} registros inseridos.'))
else:
    card_teams1.activityTitle(str(f"""\nNenhum registro a ser inserido.\n\n RCO A Completo!!!\n\n"""))
card_teams1.text(str(f'### Atualizado {atualiza_rcoa1}.\n\n'))


### ==========================================================RCO B=======================================================================

rco_b = pd.read_csv(f'{Pasta dos Arquivos}\\{rco_b_arq}',sep="|",dtype=str,decimal=',',encoding='ANSI')
rco_b = rco_b[['Franquia','Codigo','Nome_Fantasia','CPF_CNPJ','Data_Inclusao','Data_Baixa','Valor','Serasa','Motivo_Cancelamento','ID','ID_CAN','Contrato']]
rco_b = rco_b.apply(lambda x: x.str.strip())

cursor.execute('SELECT * FROM sch_solucoes.tb_rco_b')
df_con = cursor.fetchall()
rco_b_conexao = pd.DataFrame(list(df_con), columns = ['Franquia','Codigo','Nome_Fantasia','CPF_CNPJ','Data_Inclusao','Data_Baixa',
                                                          'Valor','Serasa','Motivo_Cancelamento','ID','ID_CAN','Contrato'])

rco_b_conexao['ID'] = rco_b_conexao['ID'].astype(str)
rco_b_conexao['ID'] = rco_b_conexao['ID'].str.strip()
rco_b['ID'] = rco_b['ID'].astype(str)
rco_b['ID'] = rco_b['ID'].str.strip()

rco_b = rco_b[~(rco_b['ID'].isin(rco_b_conexao['ID'])) | (rco_b['ID'].isna())]

rco_b['Franquia'].fillna("", inplace = True)
rco_b['Codigo'].fillna(0, inplace = True)
rco_b['Nome_Fantasia'].fillna("", inplace = True)
rco_b['CPF_CNPJ'].fillna(0, inplace = True)
rco_b['Valor'].fillna(0, inplace = True)
rco_b['Serasa'].fillna("", inplace = True)
rco_b['Motivo_Cancelamento'].fillna("", inplace = True)
rco_b['ID_CAN'].fillna("", inplace = True)
rco_b['Contrato'].fillna("", inplace = True)
rco_b.replace("'","&#39;", inplace=True, regex = True)
rco_b.replace('"',"&#34;", inplace=True, regex = True)

rco_b['ID']  = rco_b['ID'].astype(str)
rco_b['ID']  = rco_b['ID'].str.strip()
rco_b['Data_Inclusao']  = rco_b['Data_Inclusao'].astype(str)
rco_b['Data_Inclusao']  = rco_b['Data_Inclusao'].str.strip()
rco_b['Data_Baixa']  = rco_b['Data_Baixa'].astype(str)
rco_b['Data_Baixa']  = rco_b['Data_Baixa'].str.strip()
rco_b['Data_Inclusao'] = pd.to_datetime(rco_b['Data_Inclusao'], errors='ignore', dayfirst = True)
rco_b['Data_Baixa'] = pd.to_datetime(rco_b['Data_Baixa'], errors='ignore', dayfirst = True)
rco_b['Valor'] = rco_b['Valor'].astype(str)
rco_b['Valor'] = rco_b['Valor'].str.strip()
rco_b['Valor'] = rco_b['Valor'].apply(lambda x: str(x).replace(",","."))

rco_b['Codigo'] = rco_b['Codigo'].astype(np.int64)
rco_b['CPF_CNPJ'] = rco_b['CPF_CNPJ'].astype(np.int64)
rco_b['Valor'] = rco_b['Valor'].astype(np.float64)
rco_b['ID'] = rco_b['ID'].astype(np.int64)
rco_b = rco_b.sort_values(by = 'ID', ascending = True, ignore_index = True)
rco_b.drop_duplicates(subset = 'ID', keep = 'first', inplace = True, ignore_index = True)

registros_rco_b = len(rco_b)
erros_rco_b = {}
lista_rcob_2val = []
contador_rco_b = 0
for reg in range(len(rco_b)):
    try:
        cursor.execute(f"""INSERT INTO sch_solucoes.tb_rco_b(franquia,codigo,nome_fantasia,cpf_cnpj,data_inclusao,data_baixa,valor,serasa,motivo_cancelamento,id,id_can,contrato)
                            VALUES('{rco_b.loc[reg,'Franquia']}',{rco_b.loc[reg,'Codigo']},'{rco_b.loc[reg,'Nome_Fantasia']}',{rco_b.loc[reg,'CPF_CNPJ']},'{rco_b.loc[reg,'Data_Inclusao']}','{rco_b.loc[reg,'Data_Baixa']}',{rco_b.loc[reg,'Valor']},'{rco_b.loc[reg,'Serasa']}','{rco_b.loc[reg,'Motivo_Cancelamento']}',{rco_b.loc[reg,'ID']},'{rco_b.loc[reg,'ID_CAN']}','{rco_b.loc[reg,'Contrato']}')""")
        con.commit()
        contador_rco_b +=1
    except:
        con.rollback()
        erros_rco_b[rco_b.loc[reg,'ID']] = rco_b.loc[reg,'Codigo']
        lista_rcob_2val.append(rco_b.loc[reg,'ID'])

atualiza_rcob = dt.datetime.now()
atualiza_rcob1 = atualiza_rcob.strftime("às %H:%M:%S do dia %d/%m/%Y")
        
texto_rco_b = []
card_teams2 = teams.cardsection()
card_teams2.title(">RCO B")
if contador_rco_b==1:
    card_teams2.activityTitle(str(f'{contador_rco_b} registro inserido.'))
elif contador_rco_b>1:
    card_teams2.activityTitle(str(f'{contador_rco_b} registros inseridos.'))
else:
    card_teams2.activityTitle(str(f"""\nNenhum registro a ser inserido.\n\n RCO B Completo!!!\n\n"""))
card_teams2.text(str(f'### Atualizado {atualiza_rcob1}.\n\n'))


### ===========================================================CADASTROS========================================================================

cadastro = pd.read_csv(f'{Pasta dos Arquivos}\\{cadastro_arq}',sep="|",dtype=str,decimal=',',encoding='ANSI')
cadastro = cadastro[["frnCod","empCod","empNom","empFil","empDatCan","empTel","empFax","empEma","tipCod","RD",
                     "Situacao_Contrato","mcades","BloqueioAuto","Ativacao_CL","Cancelamento_CL","empCGC","empRaz",
                     "empEnd","empBai","empCid","empUF","empCEP","Documento","Nome","tipo","atv","cel","empCodPai",
                     "CobraTxExtra","NomeTipoContrato","VCM","DOPERACIONAL","DNF","DINFRAESTRUTURA","CONTROLE_LEGAL",
                     "MEPROTEJA","VOUCHER","Tabela","BloqueioManual","CPFCNPJRepresentante","tipoPessoa","NomeRepresentante",
                     "Cargo","empTermoFianca","empNomeFiador","empCPFFiador"]]
cadastro = cadastro.apply(lambda x: x.str.strip())

cursor.execute('SELECT * FROM sch_solucoes.tb_cadastro')
df_con = cursor.fetchall()
cadastro_conexao = pd.DataFrame(list(df_con), columns = ["frnCod","empCod","empNom","empFil","empDatCan","empTel","empFax",
                                                       "empEma","tipCod","RD","Situacao_Contrato","mcades","BloqueioAuto",
                                                       "Ativacao_CL","Cancelamento_CL","empCGC","empRaz","empEnd","empBai",
                                                       "empCid","empUF","empCEP","Documento","Nome","tipo","atv","cel",
                                                       "empCodPai","CobraTxExtra","NomeTipoContrato","VCM","DOPERACIONAL",
                                                       "DNF","DINFRAESTRUTURA","CONTROLE_LEGAL","MEPROTEJA","VOUCHER","Tabela",
                                                       "BloqueioManual","CPFCNPJRepresentante","tipoPessoa","NomeRepresentante",
                                                       "Cargo","empTermoFianca","empNomeFiador","empCPFFiador"])

vendas_novas = cadastro[['empCod', 'empFil']]
vendas_conexao = cadastro_conexao[['empCod', 'empFil']]
vendas_novas['empCod'] = vendas_novas['empCod'].astype(str)
vendas_novas['empCod'] = vendas_novas['empCod'].str.strip()
vendas_conexao['empCod'] = vendas_conexao['empCod'].astype(str)
vendas_conexao['empCod'] = vendas_conexao['empCod'].str.strip()
novos_regs = vendas_novas[~vendas_novas['empCod'].isin(vendas_conexao['empCod'])]
novos_regs['empFil'] = pd.to_datetime(novos_regs['empFil'], errors='ignore', dayfirst = True)
novos_regs = novos_regs.sort_values('empFil', ascending = False, ignore_index = True)
novos_regs['empFil'] = novos_regs['empFil'].dt.strftime("%d/%m/%Y")
novos_regs.rename(columns={'empCod':'CÓDIGO','empFil':'DATA DE CADASTRO'}, inplace = True)


if len(cadastro_conexao)>0:
    try:
        cursor.execute("""DELETE FROM sch_solucoes.tb_cadastro
                                        WHERE empCod > 0;""")
        con.commit()
    except:
        pass

cadastro['empCod'].fillna(0, inplace = True)
cadastro['empTel'].fillna("", inplace = True)
cadastro['empFax'].fillna("", inplace = True)
cadastro['empEma'].fillna("", inplace = True)
cadastro['RD'].fillna("", inplace = True)
cadastro['mcades'].fillna("", inplace = True)
cadastro['empBai'].fillna("", inplace = True)
cadastro['empCid'].fillna("", inplace = True)
cadastro['empUF'].fillna("", inplace = True)
cadastro['empCEP'].fillna("", inplace = True)
cadastro['Documento'].fillna("", inplace = True)
cadastro['Nome'].fillna("", inplace = True)
cadastro['tipo'].fillna("", inplace = True)
cadastro['atv'].fillna("", inplace = True)
cadastro['cel'].fillna("", inplace = True)
cadastro['empCodPai'].fillna("", inplace = True)
cadastro['CobraTxExtra'].fillna("", inplace = True)
cadastro['NomeTipoContrato'].fillna("", inplace = True)
cadastro['Tabela'].fillna("", inplace = True)
cadastro['BloqueioManual'].fillna("", inplace = True)
cadastro['tipoPessoa'].fillna("", inplace = True)
cadastro['NomeRepresentante'].fillna("", inplace = True)
cadastro['Cargo'].fillna("", inplace = True)
cadastro['empTermoFianca'].fillna("", inplace = True)
cadastro['empCPFFiador'].fillna("", inplace = True)

cadastro['empCGC'].fillna(0, inplace = True)
cadastro['VCM'].fillna(0, inplace = True)
cadastro['DOPERACIONAL'].fillna(0, inplace = True)
cadastro['DNF'].fillna(0, inplace = True)
cadastro['DINFRAESTRUTURA'].fillna(0, inplace = True)
cadastro['CONTROLE_LEGAL'].fillna(0, inplace = True)
cadastro['MEPROTEJA'].fillna(0, inplace = True)
cadastro['VOUCHER'].fillna(0, inplace = True)
cadastro['CPFCNPJRepresentante'].fillna(0, inplace = True)

cadastro['empCod'] = cadastro['empCod'].astype(np.int64)
cadastro['empFil'] = pd.to_datetime(cadastro['empFil'], errors='ignore', dayfirst = True)
cadastro['empDatCan'] = pd.to_datetime(cadastro['empDatCan'], errors='ignore', dayfirst = True)
cadastro['Ativacao_CL'] = pd.to_datetime(cadastro['Ativacao_CL'], errors='ignore', dayfirst = True)
cadastro['Cancelamento_CL'] = pd.to_datetime(cadastro['Cancelamento_CL'], errors='ignore', dayfirst = True)
cadastro['empCGC'] = cadastro['empCGC'].astype(np.int64)
cadastro['VCM'] = cadastro['VCM'].astype(np.float64)
cadastro['DOPERACIONAL'] = cadastro['DOPERACIONAL'].astype(np.float64)
cadastro['DNF'] = cadastro['DNF'].astype(np.float64)
cadastro['DINFRAESTRUTURA'] = cadastro['DINFRAESTRUTURA'].astype(np.float64)
cadastro['CONTROLE_LEGAL'] = cadastro['CONTROLE_LEGAL'].astype(np.float64)
cadastro['MEPROTEJA'] = cadastro['MEPROTEJA'].astype(np.float64)
cadastro['VOUCHER'] = cadastro['VOUCHER'].astype(np.float64)
cadastro['CPFCNPJRepresentante'] = cadastro['CPFCNPJRepresentante'].astype(np.int64)
cadastro.replace("'","&#39;", inplace=True, regex = True)
cadastro.replace('"',"&#34;", inplace=True, regex = True)

cadastro['empFil'] = cadastro['empFil'].astype(str)
cadastro['empDatCan'] = cadastro['empDatCan'].astype(str)
cadastro['Ativacao_CL'] = cadastro['Ativacao_CL'].astype(str)
cadastro['Cancelamento_CL'] = cadastro['Cancelamento_CL'].astype(str)
cadastro['empFil'] = cadastro['empFil'].str.replace("NaT", "1899-01-01", regex=True)
cadastro['empDatCan'] = cadastro['empDatCan'].str.replace("NaT", "1899-01-01", regex=True)
cadastro['Ativacao_CL'] = cadastro['Ativacao_CL'].str.replace("NaT", "1899-01-01", regex=True)
cadastro['Cancelamento_CL'] = cadastro['Cancelamento_CL'].str.replace("NaT", "1899-01-01", regex=True)

cadastro['empFil'] = pd.to_datetime(cadastro['empFil'], errors='ignore', dayfirst = True)
cadastro['empDatCan'] = pd.to_datetime(cadastro['empDatCan'], errors='ignore', dayfirst = True)
cadastro['Ativacao_CL'] = pd.to_datetime(cadastro['Ativacao_CL'], errors='ignore', dayfirst = True)
cadastro['Cancelamento_CL'] = pd.to_datetime(cadastro['Cancelamento_CL'], errors='ignore', dayfirst = True)

cadastro.sort_values('empFil', ascending = False, inplace = True, ignore_index = True)
cadastro.drop_duplicates(subset = 'empCod', keep = 'first', inplace = True, ignore_index = True)

cadastro['empNomeFiador'] = cadastro['empNomeFiador'].astype(str)
cadastro['empNomeFiador'] = cadastro['empNomeFiador'].str.replace("nan", "", regex=True)

contador_cadastro = 0
erros_cadastros = {}
lista_cadastros_2val = []
for reg in range(len(cadastro)):
    try:
        cursor.execute(f"""INSERT INTO sch_solucoes.tb_cadastro (frnCod,empCod,empNom,empFil,empDatCan,empTel,empFax,empEma,tipCod,RD,Situacao_Contrato,mcades,BloqueioAuto,Ativacao_CL,Cancelamento_CL,empCGC,empRaz,empEnd,empBai,empCid,empUF,empCEP,Documento,Nome,tipo,atv,cel,empCodPai,CobraTxExtra,NomeTipoContrato,VCM,DOPERACIONAL,DNF,DINFRAESTRUTURA,CONTROLE_LEGAL,MEPROTEJA,VOUCHER,Tabela,BloqueioManual,CPFCNPJRepresentante,tipoPessoa,NomeRepresentante,Cargo,empTermoFianca,empNomeFiador,empCPFFiador)
                                VALUES('{cadastro.loc[reg,'frnCod']}',{cadastro.loc[reg,'empCod']},'{cadastro.loc[reg,'empNom']}','{cadastro.loc[reg,'empFil']}','{cadastro.loc[reg,'empDatCan']}','{cadastro.loc[reg,'empTel']}','{cadastro.loc[reg,'empFax']}','{cadastro.loc[reg,'empEma']}','{cadastro.loc[reg,'tipCod']}','{cadastro.loc[reg,'RD']}','{cadastro.loc[reg,'Situacao_Contrato']}','{cadastro.loc[reg,'mcades']}','{cadastro.loc[reg,'BloqueioAuto']}','{cadastro.loc[reg,'Ativacao_CL']}','{cadastro.loc[reg,'Cancelamento_CL']}',{cadastro.loc[reg,'empCGC']},'{cadastro.loc[reg,'empRaz']}','{cadastro.loc[reg,'empEnd']}','{cadastro.loc[reg,'empBai']}','{cadastro.loc[reg,'empCid']}','{cadastro.loc[reg,'empUF']}','{cadastro.loc[reg,'empCEP']}','{cadastro.loc[reg,'Documento']}','{cadastro.loc[reg,'Nome']}','{cadastro.loc[reg,'tipo']}','{cadastro.loc[reg,'atv']}','{cadastro.loc[reg,'cel']}','{cadastro.loc[reg,'empCodPai']}','{cadastro.loc[reg,'CobraTxExtra']}','{cadastro.loc[reg,'NomeTipoContrato']}',{cadastro.loc[reg,'VCM']},{cadastro.loc[reg,'DOPERACIONAL']},{cadastro.loc[reg,'DNF']},{cadastro.loc[reg,'DINFRAESTRUTURA']},{cadastro.loc[reg,'CONTROLE_LEGAL']},{cadastro.loc[reg,'MEPROTEJA']},{cadastro.loc[reg,'VOUCHER']},'{cadastro.loc[reg,'Tabela']}','{cadastro.loc[reg,'BloqueioManual']}',{cadastro.loc[reg,'CPFCNPJRepresentante']},'{cadastro.loc[reg,'tipoPessoa']}','{cadastro.loc[reg,'NomeRepresentante']}','{cadastro.loc[reg,'Cargo']}','{cadastro.loc[reg,'empTermoFianca']}','{cadastro.loc[reg,'empNomeFiador']}','{cadastro.loc[reg,'empCPFFiador']}')""")
        con.commit()
        contador_cadastro+=1
    except:
        con.rollback()
        erros_cadastros[cadastro.loc[reg,'empCod']] = cadastro.loc[reg,'empNom']
        lista_cadastros_2val.append(cadastro.loc[reg,'empCod'])

time.sleep(10)
        
try:
    cursor.execute("""UPDATE sch_solucoes.tb_cadastro
                                SET empDatCan = NULL
                                WHERE empDatCan = '1899-01-01';""")
    con.commit()
except:
    con.rollback()

time.sleep(0.5)

try:
    cursor.execute("""UPDATE sch_solucoes.tb_cadastro
                                SET Ativacao_CL = NULL
                                WHERE Ativacao_CL = '1899-01-01';""")
    con.commit()
except:
    con.rollback()
    
time.sleep(0.5)

try:
    cursor.execute("""UPDATE sch_solucoes.tb_cadastro
                                SET Cancelamento_CL = NULL
                                WHERE Cancelamento_CL = '1899-01-01';""")
    con.commit()
except:
    con.rollback()

time.sleep(0.5)
    
try:
    cursor.execute("""UPDATE sch_solucoes.tb_cadastro
                                SET frnCod = NULL
                                WHERE frnCod = '';""")
    con.commit()
except:
    con.rollback()

time.sleep(0.5)

try:
    cursor.execute("""UPDATE sch_solucoes.tb_cadastro
                                SET RD = NULL
                                WHERE RD = '';""")
    con.commit()
except:
    con.rollback()

time.sleep(0.5)

atualiza_cad = dt.datetime.now()
atualiza_cad1 = atualiza_cad.strftime("às %H:%M:%S do dia %d/%m/%Y")

texto_cadastro = []
card_teams3 = teams.cardsection()
card_teams3.title(">CADASTROS")
if len(novos_regs)==1:
    card_teams3.activityTitle(str(f'{len(novos_regs)} venda nova.'))
    card_teams3.addFact("DATA DE CADASTRO","CÓDIGO")
    for l in range(len(novos_regs)):
        cadtexto = f"{novos_regs.loc[l,'DATA DE CADASTRO']}"
        codtexto = f"{novos_regs.loc[l,'CÓDIGO']}"
        card_teams3.addFact(cadtexto,codtexto)
    
elif len(novos_regs)>1:
    card_teams3.activityTitle(str(f'{len(novos_regs)} vendas novas.'))
    card_teams3.addFact("DATA DE CADASTRO","CÓDIGO")
    for l in range(len(novos_regs)):
        cadtexto = f"{novos_regs.loc[l,'DATA DE CADASTRO']}"
        codtexto = f"{novos_regs.loc[l,'CÓDIGO']}"
        card_teams3.addFact(cadtexto,codtexto)
else:
    card_teams3.activityTitle(str(f"""\nNenhum registro a ser inserido.\n\n Cadastros Completo!!!\n\n"""))
card_teams3.text(str(f'### Atualizado {atualiza_cad1}.\n\n'))

### =================================================ENVIO DAS MENSAGENS==============================================================

msg_teams.addSection(card_teams)
msg_teams.send()
msg_teams1.addSection(card_teams1)
msg_teams1.send()
msg_teams2.addSection(card_teams2)
msg_teams2.send()
msg_teams3.addSection(card_teams3)
msg_teams3.send()


### =================================================VALIDAÇÃO DE 2ª ETAPA==============================================================

# Esta validação é uma medida para subir os registros caso estes, por algum motivo em específico, não tenham subido corretamente.

if len(erros_hist)>0:
    hist_2val = historico[historico['ID'].isin(lista_hist_2val)]
    hist_2val.to_csv(f'{pasta2}Historico_Atendimento.csv', sep=";", decimal = ".", date_format = '%Y-%m-%d', index=False, encoding='UTF-8')
    time.sleep(10)
    cursor.execute("copy sch_solucoes.tb_historico From '{pasta2}Historico_Atendimento.csv' delimiter ';' csv header encoding 'UTF-8';")
    con.commit()

if len(erros_rco_a)>0:
    rcoa_2val = rco_a[rco_a['ID'].isin(lista_rcoa_2val)]
    rcoa_2val.to_csv('{pasta2}RCO_A.csv', sep=";", decimal = ".", date_format = '%Y-%m-%d', index=False, encoding='UTF-8')
    time.sleep(10)
    cursor.execute("copy sch_solucoes.tb_rco_a From '{pasta2}RCO_A.csv' delimiter ';' csv header encoding 'UTF-8';")
    con.commit()

if len(erros_rco_b)>0:
    rcob_2val = rco_b[rco_b['ID'].isin(lista_rcob_2val)]
    rcob_2val.to_csv('{pasta2}RCO_B.csv', sep=";", decimal = ".", date_format = '%Y-%m-%d', index=False, encoding='UTF-8')
    time.sleep(10)
    cursor.execute("copy sch_solucoes.tb_rco_b From '{pasta2}RCO_B.csv' delimiter ';' csv header encoding 'UTF-8';")
    con.commit()

if len(erros_cadastros)>0:
    cadastros_2val = cadastro[cadastro['empCod'].isin(lista_cadastros_2val)]
    cadastros_2val.to_csv('{pasta2}Cadastros.csv', sep=";", decimal = ".", date_format = '%Y-%m-%d', index=False, encoding='UTF-8')
    time.sleep(10)
    cursor.execute("copy sch_solucoes.tb_cadastro From '{pasta2}Cadastros.csv' delimiter ';' csv header encoding 'UTF-8';")
    con.commit()

atualiza_2etapa = dt.datetime.now()
atualiza_2etapa1 = atualiza_2etapa.strftime("às %H:%M:%S do dia %d/%m/%Y")

if (len(erros_hist)+len(erros_rco_a)+len(erros_rco_b)+len(erros_cadastros))>0:
    msg_teams5 = teams.connectorcard(canal_teams)
    msg_teams5.text("# **AVISO para o Setor:**")
    msg_teams5.color('#ff0000')
    card_teams5 = teams.cardsection()
    card_teams5.title(">REGISTROS INSERIDOS POR IMPORTAÇÃO")
    if len(erros_hist)>0:
        if len(erros_hist)==1:
            card_teams5.activityTitle(str(f'{len(erros_hist)} registro de Histórico inserido.\n\n'))
        else:
            card_teams5.activityTitle(str(f'{len(erros_hist)} registros de Histórico inseridos.\n\n'))

    if len(erros_rco_a)>0:
        if len(erros_rco_a)==1:
            card_teams5.activityTitle(str(f'{len(erros_rco_a)} registro de RCO A inserido.\n\n'))
        else:
            card_teams5.activityTitle(str(f'{len(erros_rco_a)} registros de RCO A inseridos.\n\n'))

    if len(erros_rco_b)>0:
        if len(erros_rco_b)==1:
            card_teams5.activityTitle(str(f'{len(erros_rco_b)} registro de RCO B inserido.\n\n'))
        else:
            card_teams5.activityTitle(str(f'{len(erros_rco_b)} registros de RCO B inseridos.\n\n'))

    if len(erros_cadastros)>0:
        if len(erros_cadastros)==1:
            card_teams5.activityTitle(str(f'{len(erros_cadastros)} registro de Cadastro inserido.\n\n'))
        else:
            card_teams5.activityTitle(str(f'{len(erros_cadastros)} registros de Cadastros inseridos.\n\n'))
    card_teams5.text(str(f'### Atualizado {atualiza_2etapa1}.\n\n'))
    msg_teams5.addSection(card_teams5)
    msg_teams5.send()
