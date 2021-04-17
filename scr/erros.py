from flask import Blueprint, jsonify
import json, routes


erros = routes.get_blueprint()

@erros.errorhandler(400)
def handle_400_error(e):
    resp = e.get_response()
    resp.data = json.dumps({
        "status_code": e.code,
        "name_status": e.name,
        "description": e.description
    })
    resp.content_type = "application/json"
    return resp