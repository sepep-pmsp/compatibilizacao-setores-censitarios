from enum import Enum
from geopandas import (
    read_file,
    GeoDataFrame,
)
from logging import (
    Logger,
    getLogger,
)
from os.path import (
    exists,
    join,
    basename,
)
from os import makedirs
from urllib.parse import urlparse
from urllib.request import urlretrieve

class Censo(Enum):
   CENSO_2010=2010
   CENSO_2022=2022

class Nivel(Enum):
   DISTRITOS='distritos'
   SETORES='setores'

def get_malha_url(censo:Censo, nivel:Nivel) -> str:
    """
    Essa função retorna a url do arquivo georreferenciado de determinado nível geográfico do censo escolhido.

    Parameters
    ----------
    censo : Censo
        O censo de referência.
    nivel : Nivel
        O nível geográfico da malha desejada.

    Returns
    -------
    str
        A URL do arquivo georreferenciado escolhido.
    """
    nivel_str = nivel.value
    if censo == Censo.CENSO_2010:
        if nivel == Nivel.SETORES:
            nivel_str ='setores_censitarios'
        return f'https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/censo_2010/setores_censitarios_shp/sp/sp_{nivel_str}.zip'
    
    if censo == Censo.CENSO_2022:
        if nivel == Nivel.DISTRITOS:
            sufixo = '_Distrito'
        elif nivel == Nivel.SETORES:
            sufixo = ''
        return f'https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios_preliminares/malha_com_atributos/{nivel_str}/json/UF/SP/SP_Malha_Preliminar{sufixo}_2022.zip'

def download_malha(censo:Censo, nivel:Nivel, filtro:str=None, logger:Logger=getLogger(), cache_dir:str='data/cache/', **kwargs) -> GeoDataFrame:
    """
    Baixa a malha no nível especificado para o censo escolhido. Os parâmetros enviados via kwargs são enviados ao método read_file do Geopandas.

    O parâmetro filtro pode ser passado como uma string que será utilizado como filtro para o método GeoDataFrame.query.

    O parâmetro logger pode ser passado como uma instância de Logger para a utilização de um Logger diferente do padrão.

    Parameters
    ----------
    censo : Censo
        O censo de referência.
    nivel : Nivel
        O nível geográfico da malha desejada.
    filtro : str
        Um filtro compatível com o método GeoDataFrame.query. Caso não seja fornecido, o GeoDataFrame é devolvido na íntegra.
    logger : Logger
        Um logger customizado. Caso não seja fornecido, é utilizado o logger padrão.
    **kwargs : dict
        Demais argumentos aceitos pela função Geopandas.read_file.

    Returns
    -------
    GeoDataFrame
        GeoDataFrame com os dados escolhidos.
    """
    url = get_malha_url(censo, nivel)

    parsed_url = urlparse(url)
    filename = basename(parsed_url.path)
    file_dir =join(cache_dir, nivel.value, str(censo.value))
    file_path = join(file_dir, filename)

    if not exists(file_path):
        logger.info(f'Baixando a malha de {nivel.value} do censo de {censo.value} de {url}.')
        makedirs(file_dir, exist_ok=True)
        urlretrieve(url, file_path)
    else:
        logger.info(f'O arquivo {file_path} já foi baixado anteriormente. Usando cache local.')

    gdf = read_file(file_path, **kwargs)

    if filtro:
        gdf = gdf.query(filtro)

    return gdf
        