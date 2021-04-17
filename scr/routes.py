from flask import Blueprint, request, jsonify
from service import movimento_financeiro, transacao_movimento_financeiro

basePath = '/gfp'
api = Blueprint('api', __name__, url_prefix=basePath)

def get_blueprint():
    return api

#------------------------------------------------------------------------------------------------#
# Movimento Financeiro
#------------------------------------------------------------------------------------------------#

@api.route('/v1/orcamento/<_idOrcam>/movimento', methods=['GET'])
def get_all_movimento_financeiro(_idOrcam):
    return movimento_financeiro.listar(_idOrcam), 200

@api.route('/v1/orcamento/<_idOrcam>/movimento', methods=['POST'])
def post_movimento_financeiro(_idOrcam):
    return movimento_financeiro.incluir(_idOrcam), 201

@api.route('/v1/orcamento/<_idOrcam>/movimento/<int:_idMovtoFincr>', methods=['DELETE'])
def delete_movimento_financeiro(_idOrcam, _idMovtoFincr):
    return movimento_financeiro.excluir(_idMovtoFincr), 200

#------------------------------------------------------------------------------------------------#
# Transação Movimento Financeiro
#------------------------------------------------------------------------------------------------#

@api.route('/v1/orcamento/<_idOrcam>/movimento/<int:_idMovtoFincr>/transacao', methods=['GET'])
def get_all_transacao_movimento_financeiro(_idOrcam, _idMovtoFincr):
    return transacao_movimento_financeiro.listar(_idOrcam, _idMovtoFincr, request.args.get('vnclo'), request.args.get('sit')), 200

@api.route('/v1/orcamento/<_idOrcam>/movimento/<int:_idMovtoFincr>/transacao/<_nrSeqTrans>', methods=['PATCH'])
def patch_situacao_transacao_movimento_financeiro(_idOrcam, _idMovtoFincr, _nrSeqTrans):
    return transacao_movimento_financeiro.atualiza_situacao_transacao(_idMovtoFincr, _nrSeqTrans, request.args.get('sit')), 200

@api.route('/v1/orcamento/<_idOrcam>/movimento/<int:_idMovtoFincr>/transacao', methods=['POST'])
def post_transacao_movimento_financeiro(_idOrcam, _idMovtoFincr):
    return transacao_movimento_financeiro.incluir(_idMovtoFincr, request), 201

#@api.route('/v1/orcamento/<_idOrcam>/movimento/<int:_idMovtoFincr>/transacao/<int:_nrSeqTrans>', methods=['DELETE'])
#def delete_transacao_movimento_financeiro(_idOrcam, _idMovtoFincr):
#    return transacao_movimento_financeiro.excluir(_idMovtoFincr), 200