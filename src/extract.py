import requests

url_contratos = 'https://dadosabertos.compras.gov.br/modulo-contratos/1_consultarContratos'
url_contratos_itens = 'https://dadosabertos.compras.gov.br/modulo-contratos/2_consultarContratosItem'

params = {
    'pagina': 20,
    'tamanhoPagina': 500, #min é 10; max é 500
    'dataVigenciaInicialMin': '2024-01-01',
    'dataVigenciaInicialMax': '2024-12-31',
}

def get_from_api():
    pass
    # for pagina in range(1, tamanhoPagina + 1)
    

def get_endpoint_contratos():
    pass

def get_endpoint_contratos_itens():
    pass