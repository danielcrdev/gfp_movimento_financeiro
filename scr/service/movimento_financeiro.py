from flask import abort
from database import Database

#------------------------------------------------------------------------------------------------#
# Lista movimento financeiro da base
#------------------------------------------------------------------------------------------------#
def listar(idOrcam):

    resp = {
        'movimento' : [],
        'quantidadeRegistros': 0
    }
    
    sql = 'SELECT ' \
            'ID_MOVTO_FINCR' \
            'FROM DBMVFN.MOVTO_FINCR ' \
            'WHERE ID_ORCAM = ' + str(idOrcam)
    
    c = Database()
    s = c.executaSQLFetchall(sql)

    for row in s:
        resp['quantidadeRegistros'] += 1    
        resp['movimento'].append({
            'idMovtoFincr': row['ID_MOVTO_FINCR'],
            'idOrcam': row['ID_ORCAM']
        })

    return resp

#------------------------------------------------------------------------------------------------#
# Consulta movimento financeiro pelo id
#------------------------------------------------------------------------------------------------#
def consulta_id(idMovtoFincr):

    sql = 'SELECT ' \
            'ID_MOVTO_FINCR, ' \
            'ID_ORCAM ' \
            'FROM DBMVFN.MOVTO_FINCR ' \
            'WHERE ID_MOVTO_FINCR = ' + str(idMovtoFincr)

    c = Database()
    row = c.executaSQLFetchone(sql)
    
    if not row: abort(400,description="Registro Movimento Financeiro Não Encontrado")

    resp = {
        'movimento': {
            'idMovtoFincr': row['ID_MOVTO_FINCR'],
            'idOrcam': row['ID_ORCAM']
        }
    }

    return resp


#------------------------------------------------------------------------------------------------#
# Incluir movimento financeiro base
#------------------------------------------------------------------------------------------------#
def incluir(idOrcam):
    c = Database()
    c.conecta()
    c.abreCursor()

    rMaxId = c.executaSQLFetchoneCursorAberto("SELECT COALESCE(MAX(A.ID_MOVTO_FINCR),0)+1 AS ID_MOVTO_FINCR FROM DBMVFN.MOVTO_FINCR A")['ID_MOVTO_FINCR']

    sql = 'INSERT INTO DBMVFN.MOVTO_FINCR (ID_MOVTO_FINCR, ID_ORCAM) VALUES (' \
        ' ' + str(rMaxId)   + ', ' \
        ' ' + str(idOrcam)  + ') '
    
    c.executaSQLInsertCursorAberto(sql)
    c.commit()
    c.desconecta()

    resp = {
        'mensagem': 'Movimento Financeiro Incluído com Sucesso',
        'movimento': {
            'idMovtoFincr': str(rMaxId),
            'idOrcam': str(idOrcam) 
        }
    }

    return resp

#------------------------------------------------------------------------------------------------#
# Excluir definitivamente movimento financeiro da base
#------------------------------------------------------------------------------------------------#
def excluir(idMovtoFincr):

    o = consulta_id(idMovtoFincr)

    resp = {
        'mensagem': 'Movimento Financeiro Excluído Com Sucesso',
        'movimento': o
    }

    c = Database()
    script = []
    
    sql = "DELETE FROM DBMVFN.MOVTO_FINCR WHERE ID_MOVTO_FINCR = " + str(idMovtoFincr)
    script.append(sql)

    if c.executaSQLFetchone("SELECT COUNT(*) FROM DBMVFN.TRANS_MOVTO_FINCR WHERE ID_MOVTO_FINCR = " + str(idMovtoFincr))['COUNT(*)'] > 0:
        sql = "DELETE FROM DBMVFN.TRANS_MOVTO_FINCR WHERE ID_MOVTO_FINCR = " + str(idMovtoFincr)
        script.append(sql)

    if c.executaSQLFetchone("SELECT COUNT(*) FROM DBMVFN.VNCLO_TRANS_LIN_ORCAM WHERE ID_MOVTO_FINCR = " + str(idMovtoFincr))['COUNT(*)'] > 0:
        sql = "DELETE FROM DBMVFN.VNCLO_TRANS_LIN_ORCAM WHERE ID_MOVTO_FINCR = " + str(idMovtoFincr)
        script.append(sql)

    if c.executaSQLFetchone("SELECT COUNT(*) FROM DBMVFN.VNCLO_TRANS_DADO_ADCIO WHERE ID_MOVTO_FINCR = " + str(idMovtoFincr))['COUNT(*)'] > 0:
        sql = "DELETE FROM DBMVFN.VNCLO_TRANS_DADO_ADCIO WHERE ID_MOVTO_FINCR = " + str(idMovtoFincr)
        script.append(sql)

    c.executaListaSQLScript(script)

    return resp