import requests
from flask import abort
from static import config
from database import Database
from service import movimento_financeiro, vinculo_transacao_linha_orcamento


#------------------------------------------------------------------------------------------------#
# Lista transação da base
#------------------------------------------------------------------------------------------------#
def listar(idOrcam, idMovtoFincr, vnclo, sit):

    if idMovtoFincr == 0 and idOrcam == 0:
        abort(400,description="Necessário IdMovimento ou IdOrçamento para Pesquisar Transações")

    resp = {
        'transacao' : [],
        'quantidadeRegistros': 0
    }

    sql = 'SELECT ' \
                'T.ID_MOVTO_FINCR, ' \
                'T.NR_SEQ_TRANS, ' \
                'T.COD_SIT_TRANS, ' \
                'T.DES_TRANS, ' \
                'T.VAL_TRANS, ' \
                'DATE_FORMAT(T.DAT_TRANS, "%Y-%m-%d") AS DAT_TRANS,' \
                'T.DES_OBS_TRANS, ' \
                'M.ID_ORCAM ' \
            'FROM DBMVFN.TRANS_MOVTO_FINCR T ' \
            'INNER JOIN DBMVFN.MOVTO_FINCR M ON M.ID_MOVTO_FINCR = T.ID_MOVTO_FINCR ' \


    # Consulta as todas as transações vinculadas e não vinculadas
    if vnclo == None: 
        'WHERE T.ID_MOVTO_FINCR IS NOT NULL'
    # Consulta apenas transações vinculadas
    elif vnclo == 'sim': 
        sql = sql + ' ' \
	    'INNER JOIN DBMVFN.VNCLO_TRANS_LIN_ORCAM V ' \
		   'ON	V.ID_MOVTO_FINCR = T.ID_MOVTO_FINCR ' \
		   'AND	V.NR_SEQ_TRANS	 = T.NR_SEQ_TRANS ' \
	    'WHERE V.ID_MOVTO_FINCR IS NOT NULL '
    # Consulta apenas transações ainda não vinculadas
    elif vnclo == 'não':
        sql = sql + ' ' \
	    'LEFT JOIN DBMVFN.VNCLO_TRANS_LIN_ORCAM V ' \
		   'ON	V.ID_MOVTO_FINCR = T.ID_MOVTO_FINCR ' \
		   'AND	V.NR_SEQ_TRANS	 = T.NR_SEQ_TRANS ' \
	    'WHERE V.ID_MOVTO_FINCR IS NULL '
    else:
        abort(400,description="Valor Query Param Vínculo Inválido (vnclo=" + vnclo + ")")


    # Consulta as todas as transações pendentes e efetivadas
    if sit == None: 
        None
    # Consulta apenas transação com situação "P" = Pendente ou "E" = Efetivada
    elif sit == 'P' or sit == 'E':
        sql = sql + ' ' \
	    'AND T.COD_SIT_TRANS = "' + sit + '"'
    else: 
        abort(400,description="Valor Query Param Situação Inválido (sit=" + sit + ")")


    # Consulta por Movimento ou por OrçamentoS
    if idMovtoFincr > 0:
        sql = sql + ' ' \
        'AND T.ID_MOVTO_FINCR = ' + str(idMovtoFincr)
    else: 
        sql = sql + ' ' \
        'AND M.ID_ORCAM = ' + str(idOrcam)
    
    c = Database()
    s = c.executaSQLFetchall(sql)

    for row in s:
        resp['quantidadeRegistros'] += 1
        v = vinculo_transacao_linha_orcamento.listar(row['ID_MOVTO_FINCR'], row['NR_SEQ_TRANS'])    
        if v['quantidadeRegistros'] > 0:
            resp['transacao'].append({
                'idMovtoFincr': row['ID_MOVTO_FINCR'],
                'nrSeqTrans': row['NR_SEQ_TRANS'],
                'codSitTrans': row['COD_SIT_TRANS'],
                'desTrans': row['DES_TRANS'],
                'valTrans': str(row['VAL_TRANS']),
                'datTrans': row['DAT_TRANS'],
                'desObsTrans': row['DES_OBS_TRANS'],
                'idOrcam': row['ID_ORCAM'],
                'vinculolinhaorcamento': v['vinculolinhaorcamento']
            })
        else:
            resp['transacao'].append({
                'idMovtoFincr': row['ID_MOVTO_FINCR'],
                'nrSeqTrans': row['NR_SEQ_TRANS'],
                'codSitTrans': row['COD_SIT_TRANS'],
                'desTrans': row['DES_TRANS'],
                'valTrans': str(row['VAL_TRANS']),
                'datTrans': row['DAT_TRANS'],
                'desObsTrans': row['DES_OBS_TRANS'],
                'idOrcam': row['ID_ORCAM']
            })

    return resp


#------------------------------------------------------------------------------------------------#
# Consulta transação movimento financeiro pelo id
#------------------------------------------------------------------------------------------------#
def consulta_id(idMovtoFincr, nrSeqTrans):

    sql = 'SELECT ' \
                'T.ID_MOVTO_FINCR, ' \
                'T.NR_SEQ_TRANS, ' \
                'T.COD_SIT_TRANS, ' \
                'T.DES_TRANS, ' \
                'T.VAL_TRANS, ' \
                'DATE_FORMAT(T.DAT_TRANS, "%Y-%m-%d") AS DAT_TRANS,' \
                'T.DES_OBS_TRANS, ' \
                'M.ID_ORCAM ' \
            'FROM DBMVFN.TRANS_MOVTO_FINCR T ' \
            'INNER JOIN DBMVFN.MOVTO_FINCR M ON M.ID_MOVTO_FINCR = T.ID_MOVTO_FINCR ' \
            'WHERE T.ID_MOVTO_FINCR = ' + str(idMovtoFincr) + ' ' \
            'AND   T.NR_SEQ_TRANS   = ' + str(nrSeqTrans)
    
    c = Database()
    row = c.executaSQLFetchone(sql)
    
    if not row: abort(400,description="Registro Transação Movimento Financeiro Não Encontrado")

    resp = {
        'movimento': {
            'idMovtoFincr': row['ID_MOVTO_FINCR'],
            'nrSeqTrans': row['NR_SEQ_TRANS'],
            'codSitTrans': row['COD_SIT_TRANS'],
            'desTrans': row['DES_TRANS'],
            'valTrans': row['VAL_TRANS'],
            'datTrans': row['DAT_TRANS'],
            'desObsTrans': row['DES_OBS_TRANS'],
            'idOrcam': row['ID_ORCAM']
        }
    }

    return resp


#------------------------------------------------------------------------------------------------#
# Incluir transação base
#------------------------------------------------------------------------------------------------#
# Payload:
#        {
#            "codSitTrans": "P",
#            "desTrans": "",
#            "valTrans": "",
#            "datTrans": "",
#            "desObsTrans": "",
#            "vinculolinhaorcamento": {
#                "idLinOrcam": 0,
#                "dadosadicionaisvinculo": [
#                    {
#                        "idTpoDadoAdcio": 0,
#                        "valTexto": "",
#                        "valIntei": 0,
#                        "valDecml": 0.00,
#                        "valDat": "0001-01-01"
#                    }
#                ]
#            }
#        }
#------------------------------------------------------------------------------------------------#
def incluir(idMovtoFincr, req):

    r = req.get_json()
    r['idMovtoFincr'] = idMovtoFincr

    m = movimento_financeiro.consulta_id(idMovtoFincr)

    c = Database()
    c.conecta()
    c.abreCursor()

    r['nrSeqTrans'] = c.executaSQLFetchoneCursorAberto("SELECT COALESCE(MAX(A.NR_SEQ_TRANS),0)+1 AS NR_SEQ_TRANS FROM DBMVFN.TRANS_MOVTO_FINCR A WHERE ID_MOVTO_FINCR = " + str(idMovtoFincr))['NR_SEQ_TRANS']

    sql = 'INSERT INTO DBMVFN.TRANS_MOVTO_FINCR ' \
        '  (ID_MOVTO_FINCR, NR_SEQ_TRANS, COD_SIT_TRANS, DES_TRANS, VAL_TRANS, DAT_TRANS, DES_OBS_TRANS) VALUES (' \
        ' ' + str(r['idMovtoFincr']) + ',  ' \
        ' ' + str(r['nrSeqTrans'])   + ',  ' \
        '"' + str(r['codSitTrans'])  + '", ' \
        '"' + str(r['desTrans'])     + '", ' \
        ' ' + str(r['valTrans'])     + ',  ' \
        '"' + str(r['datTrans'])     + '", ' \
        '"' + str(r['desObsTrans'])  + '") '
    
    c.executaSQLInsertCursorAberto(sql)

    #-----------------------------------------------------#
    # Insere Vínculo de Transação e Linha de Prçamento
    #-----------------------------------------------------#
    if 'vinculolinhaorcamento' in r:
        lo = requests.get(config.basePathApiOrcamento + str(m['movimento']['idOrcam']) + '/linha/' + str(r['vinculolinhaorcamento']['idLinOrcam'])).json()
        
        sql = 'INSERT INTO DBMVFN.VNCLO_TRANS_LIN_ORCAM ' \
            '(ID_MOVTO_FINCR, NR_SEQ_TRANS, ID_ORCAM, ID_LIN_ORCAM, DAT_VNCLO) VALUES (' \
            ' ' + str(r['idMovtoFincr'])                   + ',  ' \
            ' ' + str(r['nrSeqTrans'])                     + ',  ' \
            ' ' + str(lo['linhaorcamento']['idOrcam'])     + ',  ' \
            ' ' + str(lo['linhaorcamento']['idLinOrcam'])  + ',  ' \
            ' CURRENT_DATE )'
        
        c.executaSQLInsertCursorAberto(sql)

        #-----------------------------------------------------#
        # Insere Dados Adicionais
        #-----------------------------------------------------#
        if 'dadosadicionaisvinculo' in r['vinculolinhaorcamento']:

            for da in r['vinculolinhaorcamento']['dadosadicionaisvinculo']:

                tda = requests.get(config.basePathApiOrcamento + str(lo['linhaorcamento']['idOrcam']) + '/dadoadicional/' + str(da['idTpoDadoAdcio'])).json()          
                rMaxSeq = c.executaSQLFetchoneCursorAberto("SELECT COALESCE(MAX(A.NR_SEQ_DADO_ADCIO),0)+1 AS NR_SEQ_DADO_ADCIO FROM DBMVFN.VNCLO_TRANS_DADO_ADCIO A WHERE ID_MOVTO_FINCR = " + str(r['idMovtoFincr']) + " AND NR_SEQ_TRANS = " + str(r['nrSeqTrans']) + " AND ID_ORCAM = " + str(lo['linhaorcamento']['idOrcam']) + " AND ID_LIN_ORCAM = " + str(lo['linhaorcamento']['idLinOrcam']))['NR_SEQ_DADO_ADCIO']

                sql = 'INSERT INTO DBMVFN.VNCLO_TRANS_DADO_ADCIO ' \
                '(ID_MOVTO_FINCR, NR_SEQ_TRANS, ID_ORCAM, ID_LIN_ORCAM, NR_SEQ_DADO_ADCIO, ID_TPO_DADO_ADCIO, VAL_TEXTO, VAL_INTEI, VAL_DECML, VAL_DAT) VALUES(' \
                ' ' + str(r['idMovtoFincr'])                   + ',  ' \
                ' ' + str(r['nrSeqTrans'])                     + ',  ' \
                ' ' + str(lo['linhaorcamento']['idOrcam'])     + ',  ' \
                ' ' + str(lo['linhaorcamento']['idLinOrcam'])  + ',  ' \
                ' ' + str(rMaxSeq)                             + ',  ' \
                ' ' + str(da['idTpoDadoAdcio'])                + ',  ' 

                if tda['tipodadoadicional']['formatodado']['desFormtDado'] == "Texto":
                    sql = sql                  + ' '   \
                    '"' + str(da['valTexto'])  + '", ' \
                    ' NULL  '                  + ',  ' \
                    ' NULL  '                  + ',  ' \
                    ' NULL  '                  + ')  '

                elif tda['tipodadoadicional']['formatodado']['desFormtDado'] == "Numero inteiro":
                    sql = sql                 + ' '   \
                    ' NULL  '                 + ',  ' \
                    ' ' + str(da['valIntei']) + ',  ' \
                    ' NULL  '                 + ',  ' \
                    ' NULL  '                 + ')  '

                elif tda['tipodadoadicional']['formatodado']['desFormtDado'] == "Numero Decimal com 2 Casas Decimais":
                    sql = sql                 + ' '   \
                    ' NULL  '                 + ',  ' \
                    ' NULL  '                 + ',  ' \
                    ' ' + str(da['valDecml']) + ',  ' \
                    ' NULL  '                 + ')  '

                elif tda['tipodadoadicional']['formatodado']['desFormtDado'] == "Data":
                    sql = sql                 + ' '   \
                    ' NULL  '                 + ',  ' \
                    ' NULL  '                 + ',  ' \
                    ' NULL  '                 + ',  ' \
                    '"' + str(da['valDat'])   + '") ' 

                elif tda['tipodadoadicional']['formatodado']['desFormtDado'] == "Arquivo":
                    sql = sql                  + ' '   \
                    '"' + str(da['valTexto'])  + '", ' \
                    ' NULL  '                  + ',  ' \
                    ' NULL  '                  + ',  ' \
                    ' NULL  '                  + ')  '

                elif tda['tipodadoadicional']['formatodado']['desFormtDado'] == "Link":
                    sql = sql                  + ' '   \
                    '"' + str(da['valTexto'])  + '", ' \
                    ' NULL  '                  + ',  ' \
                    ' NULL  '                  + ',  ' \
                    ' NULL  '                  + ')  '

                elif tda['tipodadoadicional']['formatodado']['desFormtDado'] == "Imagem":
                    sql = sql                  + ' '   \
                    '"' + str(da['valTexto'])  + '", ' \
                    ' NULL  '                  + ',  ' \
                    ' NULL  '                  + ',  ' \
                    ' NULL  '                  + ')  '

                else:
                    abort(400,description="Formato do Dado Adicional Desconhecido para Registro")
                
                c.executaSQLInsertCursorAberto(sql)

    c.commit()
    c.desconecta()

    resp = {
        'mensagem': 'Transação do Movimento Financeiro Incluído com Sucesso',
        'transacao': r
    }

    return resp


#------------------------------------------------------------------------------------------------#
# Excluir definitivamente transação da base
#------------------------------------------------------------------------------------------------#
def excluir(idMovtoFincr, nrSeqTrans):

    t = consulta_id(idMovtoFincr, nrSeqTrans)

    resp = {
        'mensagem': 'Transação Movimento Financeiro Excluído Com Sucesso',
        'transacao': t
    }

    c = Database()
    script = []

    sql = "DELETE FROM DBMVFN.TRANS_MOVTO_FINCR WHERE ID_MOVTO_FINCR = " + str(idMovtoFincr) + " AND NR_SEQ_TRANS = " + str(nrSeqTrans) 
    script.append(sql)

    if c.executaSQLFetchone("SELECT COUNT(*) FROM DBMVFN.VNCLO_TRANS_LIN_ORCAM WHERE ID_MOVTO_FINCR = " + str(idMovtoFincr) + " AND NR_SEQ_TRANS = " + str(nrSeqTrans))['COUNT(*)'] > 0:
        sql = "DELETE FROM DBMVFN.VNCLO_TRANS_LIN_ORCAM WHERE ID_MOVTO_FINCR = " + str(idMovtoFincr) + " AND NR_SEQ_TRANS = " + str(nrSeqTrans)
        script.append(sql)

    if c.executaSQLFetchone("SELECT COUNT(*) FROM DBMVFN.VNCLO_TRANS_DADO_ADCIO WHERE ID_MOVTO_FINCR = " + str(idMovtoFincr) + " AND NR_SEQ_TRANS = " + str(nrSeqTrans))['COUNT(*)'] > 0:
        sql = "DELETE FROM DBMVFN.VNCLO_TRANS_DADO_ADCIO WHERE ID_MOVTO_FINCR = " + str(idMovtoFincr) + " AND NR_SEQ_TRANS = " + str(nrSeqTrans)
        script.append(sql)

    c.executaListaSQLScript(script)

    return resp


#------------------------------------------------------------------------------------------------#
# Atualiza Situação Transação 
#------------------------------------------------------------------------------------------------#
def atualiza_situacao_transacao(idMovtoFincr, nrSeqTrans, situacao):

    if   situacao == 'P': descricao = 'Pendente'
    elif situacao == 'E': descricao = 'Efetivado'
    elif situacao == None:
        abort(400,description="A Query Parm Sit é Obrigatório para Atualização")
    else: 
        abort(400,description="Situação de Transação '"+ situacao +"' Inválida para Atualização.")

    sql = 'UPDATE DBMVFN.TRANS_MOVTO_FINCR ' \
          'SET COD_SIT_TRANS = "' + situacao + '" ' \
          'WHERE ID_MOVTO_FINCR = ' + str(idMovtoFincr) + ' ' \
          'AND   NR_SEQ_TRANS   = ' + str(nrSeqTrans)
    
    c = Database()
    c.executaSQLUpdate(sql)
    
    resp = {
        'mensagem': 'Situação da Transação atualizada para ' + descricao + ' com Sucesso!',
        'idMovtoFincr': str(idMovtoFincr),
        'nrSeqTrans': str(nrSeqTrans),
        'codSitTrans': situacao
    }

    return resp