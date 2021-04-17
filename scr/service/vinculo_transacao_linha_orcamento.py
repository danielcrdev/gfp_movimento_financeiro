from flask import abort
from database import Database

#------------------------------------------------------------------------------------------------#
# Lista Vinculos por Transação
#------------------------------------------------------------------------------------------------#
def listar(idMovtoFincr, nrSeqTrans):

    resp = {
        'quantidadeRegistros': 0,
        'vinculolinhaorcamento' : []
    }

    sql = 'SELECT ' \
            'V.ID_MOVTO_FINCR, ' \
            'V.NR_SEQ_TRANS, ' \
            'V.ID_ORCAM, ' \
            'V.ID_LIN_ORCAM, ' \
            'DATE_FORMAT(V.DAT_VNCLO, "%Y-%m-%d") AS DAT_VNCLO ' \
            'FROM DBMVFN.VNCLO_TRANS_LIN_ORCAM V ' \
            'WHERE V.ID_MOVTO_FINCR = ' + str(idMovtoFincr) + ' ' \
            'AND   V.NR_SEQ_TRANS   = ' + str(nrSeqTrans)

    c = Database()
    s = c.executaSQLFetchall(sql)
    
    for row in s:
        resp['quantidadeRegistros'] += 1    
        resp['vinculolinhaorcamento'].append({
            'idMovtoFincr': row['ID_MOVTO_FINCR'],
            'nrSeqTrans': row['NR_SEQ_TRANS'],
            'idOrcam': row['ID_ORCAM'],
            'idLinOrcam': row['ID_LIN_ORCAM'],
            'datVnclo': row['DAT_VNCLO']
        })

    return resp