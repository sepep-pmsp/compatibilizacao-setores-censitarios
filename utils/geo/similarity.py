from geopandas import (
    GeoDataFrame,
    GeoSeries,
    overlay,
)
from pandas import (
    Series,
    concat,
)


def __standartize_gdf(
        gdf:GeoSeries|GeoDataFrame,
        key_col:str=None
) -> GeoDataFrame:
    """
    Esta função recebe um GeoDataFrame ou um GeoSeries e retorna um GeoDataFrame padronizado confome utilizado nas demais funções deste módulo.

    Parâmetros
    ----------
    gdf : GeoDataFrame ou GeoSeries
        O GeoDataFrame ou GeoSeries de entrada.
    key_col : str, opcional
        O nome da coluna no GeoDataFrame ou GeoSeries de entrada que contém os valores-chave.
        Se nenhum valor for fornecido, a função assume que não há coluna de chave e retorna apenas a coluna 'geometry'.

    Retorna
    -------
    GeoDataFrame
        O GeoDataFrame padronizado.

    """
    
    if isinstance(gdf, GeoDataFrame):
        cols = ['geometry']
        if key_col:
            if key_col in gdf.columns:
                cols.insert(0, key_col)
        gdf = gdf[cols].copy()
        
    if isinstance(gdf, GeoSeries):
        gdf = GeoDataFrame({'geometry': gdf.copy()})
        gdf.set_geometry('geometry')
    
    return gdf
    

def __similarity_by_intersection(
    row:GeoSeries,
    other:GeoSeries|GeoDataFrame,
    right_key_col:str=None,
    only_intersections:bool=True
) -> GeoDataFrame:
    """
    Esta função calcula as propriedades de interseção entre uma GeoSeries (representando uma geometria única) e uma GeoSeries (contendo uma série de geometrias) ou GeoDataFrame.

    Parâmetros:
    - row (GeoSeries): Uma GeoSeries representando a geometria para a qual as propriedades de interseção serão calculadas.
    - other (GeoSeries ou GeoDataFrame): Uma GeoSeries ou GeoDataFrame representando as geometrias com as quais 'row' será intersectada.
    - right_key_col (str, opcional): Nome da coluna em 'other' representando o valor da chave. O padrão é None.
    - only_intersections (bool, opcional): Se True, retorna apenas geometrias com área de interseção não nula. O padrão é True.

    Retorna:
    - GeoDataFrame: Um GeoDataFrame contendo as propriedades de interseção, incluindo as geometrias intersectadas, área de interseção e percentual da área de interseção em relação à área da geometria original de 'row'. Também pode incluir valores da chave de 'row' e 'other' se especificados.

    Observação:
    - Se 'other' for um GeoDataFrame, a função assume que ele contém dados de geometria na coluna 'geometry'.
    - O GeoDataFrame resultante conterá geometrias resultantes da interseção entre 'row' e 'other'.
    - A coluna 'inter_area' representa a área das geometrias de interseção.
    - A coluna 'inter_perc' representa o percentual da área de interseção em relação à área da geometria original de 'row'.
    - Se 'right_key_col' for fornecido, a coluna especificada de 'other' será incluída no GeoDataFrame resultante.
    - Se 'only_intersections' for True, o GeoDataFrame resultante conterá apenas geometrias intersectadas (onde 'inter_perc' > 0).
    """
    
    geom = row['geometry']
    
    gdf = __standartize_gdf(other, right_key_col)

    gdf['geometry'] = gdf.intersection(geom)
    gdf['inter_area'] = gdf['geometry'].area
    gdf['inter_perc'] = gdf['inter_area']/geom.area

    if only_intersections:
        gdf = gdf[gdf['inter_perc'] > 0]

    return gdf

def __similarity_by_difference(
    row:GeoSeries,
    other:GeoSeries|GeoDataFrame,
    right_key_col:str=None,
    only_intersections:bool=True
) -> GeoDataFrame:
    """
    Esta função calcula a semelhança entre uma GeoSeries (representando uma geometria única) e outra GeoSeries (contendo uma série de geometrias) ou GeoDataFrame através da determinação da diferença entre as suas geometrias.

    Parâmetros:
    - row (GeoSeries): Uma GeoSeries representando a geometria para a qual a semelhança será calculada.
    - other (GeoSeries ou GeoDataFrame): Uma GeoSeries ou GeoDataFrame representando as geometrias com as quais 'row' será comparada.
    - right_key_col (str, opcional): Nome da coluna em 'other' representando o valor da chave. O padrão é None.
    - only_intersections (bool, opcional): Se True, retorna apenas geometrias com área de diferença não nula. O padrão é True.

    Retorna:
    - GeoDataFrame: Um GeoDataFrame contendo as propriedades da diferença entre as geometrias, incluindo as geometrias resultantes da diferença, a área da diferença e o percentual da área da diferença em relação à área da geometria original de 'row'. Também pode incluir valores da chave de 'row' e 'other' se especificados.

    Observações:
    - Se 'other' for um GeoDataFrame, a função assume que ele contém dados de geometria na coluna 'geometry'.
    - O GeoDataFrame resultante desta função conterá as geometrias resultantes da diferença entre 'row' e 'other'.
    - A coluna 'inter_area' representa a área das geometrias resultantes da diferença.
    - A coluna 'inter_perc' representa o percentual da área da diferença em relação à área da geometria original de 'row'.
    - Se 'right_key_col' for fornecido, a coluna especificada de 'other' será incluída no GeoDataFrame resultante.
    - Se 'only_intersections' for True, o GeoDataFrame resultante conterá apenas geometrias resultantes da diferença (onde 'inter_perc' > 0).
    """
    
    geom = row['geometry']
    
    gdf = __standartize_gdf(other, right_key_col)
    
    gdf['geometry'] = gdf['geometry'].apply(lambda x: geom.difference(x))
    gdf['inter_perc'] = 1 - gdf['geometry'].area/geom.area

    if only_intersections:
        gdf = gdf[gdf['inter_perc'] > 0]

    return gdf

def __row_similarity(
    row:GeoSeries,
    other:GeoSeries|GeoDataFrame,
    left_key_col:str=None,
    right_key_col:str=None,
    only_intersections:bool=True,
    method:str='intersection'
) -> GeoDataFrame:
    """
    Calcula as propriedades de interseção entre uma GeoSeries (representando uma geometria única) e uma GeoSeries (contendo uma série de geometrias) ou GeoDataFrame.

    Parâmetros:
    - row (GeoSeries): Uma GeoSeries representando a geometria para a qual as propriedades de interseção serão calculadas.
    - other (GeoSeries ou GeoDataFrame): Uma GeoSeries ou GeoDataFrame representando as geometrias com as quais 'row' será comparada.
    - left_key_col (str, opcional): Nome da coluna em 'row' representando o valor da chave. O padrão é None.
    - right_key_col (str, opcional): Nome da coluna em 'other' representando o valor da chave. O padrão é None.
    - only_intersections (bool, opcional): Se True, retorna apenas geometrias com área de interseção não nula. O padrão é True.
    - method (str, opcional): Pode receber os valores de 'intersection' ou 'difference'. O padrão é 'intersection'.

    Retorna:
    - GeoDataFrame: Um GeoDataFrame contendo as propriedades de interseção, incluindo as geometrias intersectadas, área de interseção e
                    percentual da área de interseção em relação à geometria original de 'row'. Também pode incluir valores da chave de 'row' e 'other' se especificados.

    Observação:
    - Se 'other' for um GeoDataFrame, a função assume que ele contém dados de geometria na coluna 'geometry'.
    - Se 'other' for uma GeoSeries, ela será convertida em um GeoDataFrame com uma coluna 'geometry'.
    - O GeoDataFrame resultante conterá geometrias resultantes da interseção entre 'row' e 'other'.
    - A coluna 'inter_area' representa a área das geometrias de interseção.
    - A coluna 'inter_perc' representa o percentual da área de interseção em relação à área da geometria original de 'row'.
    - Se 'left_key_col' for fornecido, a coluna especificada de 'row' será incluída no GeoDataFrame resultante.
    - Se 'only_intersections' for True, o GeoDataFrame resultante conterá apenas geometrias intersectadas (onde 'inter_perc' > 0).
    - Se 'method' for 'intersection', a similaridade é calculada pela interseção entre a geometrias de row e other.
    - Se 'difference', primeiro é calculada a diferença das geometrias de row e other e a similaridade é caculada como 1 - diferença.area/row.geometry.area.
    """

    if method=='intersection':
        gdf = __similarity_by_intersection(row, other, right_key_col, only_intersections)
    elif method=='difference':
        gdf = __similarity_by_difference(row, other, right_key_col, only_intersections)
        
    if left_key_col:
        gdf.insert(0, left_key_col, row[left_key_col])

    # Reponderando as interseções para o somatório ser igual a 1
    inter_total = gdf['inter_perc'].sum()
    gdf['inter_perc'] = gdf['inter_perc']/inter_total

    return gdf

def __overlay_similarity(
    gdf:Series|GeoSeries|GeoDataFrame,
    other:GeoSeries|GeoDataFrame,
    left_key_col:str=None,
    right_key_col:str=None,
    min_intersection_radius:int=10
):
    gdf = __standartize_gdf(gdf, left_key_col)
    other = __standartize_gdf(other, right_key_col)
        
    ## Caculate overlay between sets using overlay
    overlay_df = overlay(
        gdf,
        other,
        how='intersection',
        keep_geom_type=True,
    )

    ## Remove despicable geometries
    overlay_df = overlay_df.explode(index_parts=False)
    overlay_df['debuffed'] = overlay_df.buffer(-min_intersection_radius**(1/2))
    overlay_df = overlay_df[overlay_df['debuffed'].is_empty == False]
    overlay_df = overlay_df.drop(columns='debuffed')
    overlay_df = overlay_df.dissolve([left_key_col, right_key_col], as_index=False)

    ## Calculate inter_area
    overlay_df['inter_area'] = overlay_df['geometry'].area

    ## Calculate total_weighted_area
    overlay_df = overlay_df.merge(
        overlay_df
        [[left_key_col, 'inter_area']]
        .groupby(left_key_col)
        .sum()
        .reset_index()
        .rename(columns={'inter_area': 'setor_weighted_area'})
    )

    ## Caculate inter_perc
    overlay_df['inter_perc'] = \
            overlay_df['inter_area']/overlay_df['setor_weighted_area']

    ## Remove total_weighted_area
    overlay_df = overlay_df.drop(columns=['setor_weighted_area'])

    ## return DataFrame
    return overlay_df

def similarity(
    gdf:Series|GeoSeries|GeoDataFrame,
    other:GeoSeries|GeoDataFrame,
    left_key_col:str=None,
    right_key_col:str=None,
    only_intersections:bool=True,
    method:str='intersection',
    min_intersection_radius:int=10
) -> GeoDataFrame:
    """
    Calcula a similaridade entre um GeoDataFrame (representando um conjunto de geometrias) e outro GeoSeries ou GeoDataFrame.

    Parâmetros:
    - gdf (Series, GeoSeries ou GeoDataFrame): Uma Series ou GeoSeries representando uma geometria para a qual as propriedades de interseção serão calculadas.
                                               Ou um GeoDataFrame representando o conjunto de geometrias para o qual a similaridade será calculada.
    - other (GeoSeries ou GeoDataFrame): Uma GeoSeries ou GeoDataFrame representando as geometrias com as quais 'gdf' será comparado.
    - left_key_col (str, opcional): Nome da coluna em 'row' representando o valor da chave. O padrão é None.
    - right_key_col (str, opcional): Nome da coluna em 'other' representando o valor da chave. O padrão é None.
    - only_intersections (bool, opcional): Se True, retorna apenas geometrias com área de interseção não nula. O padrão é True.
    - method (str, opcional): Pode receber os valores de 'intersection', 'difference' ou 'overlay'. O padrão é 'intersection'.
                              'overlay' utiliza a função Geopandas.overlay para melhor performance, utilizando 'intersection'.
    - min_intersection_radius: Largura mínima de uma geometria de interseção para ser considerada. É aplicado um buffer negativo nas geometrias resultantes e
                               geometrias nulas são desconsideradas. Utilizado apenas no método 'overlay'.
                                
    Retorna:
    - GeoDataFrame: Um GeoDataFrame contendo as propriedades de interseção, incluindo as geometrias intersectadas, área de interseção e
                    percentual da área de interseção em relação à geometria original de 'row'. Também pode incluir valores da chave de 'gdf' e 'other' se especificados.

    Observação:
    - Se 'other' for um GeoDataFrame, a função assume que ele contém dados de geometria na coluna 'geometry'.
    - Se 'other' for uma GeoSeries, ela será convertida em um GeoDataFrame com uma coluna 'geometry'.
    - O GeoDataFrame resultante conterá geometrias resultantes da interseção entre 'row' e 'other'.
    - A coluna 'inter_area' representa a área das geometrias de interseção.
    - A coluna 'inter_perc' representa o percentual da área de interseção em relação à área da geometria original de 'row'.
    - Se 'left_key_col' for fornecido, a coluna especificada de 'row' será incluída no GeoDataFrame resultante.
    - Se 'only_intersections' for True, o GeoDataFrame resultante conterá apenas geometrias intersectadas (onde 'inter_perc' > 0).
    - Se 'method' for 'intersection', a similaridade é calculada pela interseção entre a geometrias de row e other.
    - Se 'difference', primeiro é calculada a diferença das geometrias de row e other e a similaridade é caculada como 1 - diferença.area/row.geometry.area.
    """
    if method.lower()=='overlay':
        return __overlay_similarity(gdf, other, left_key_col, right_key_col, min_intersection_radius)

    if isinstance(gdf, GeoSeries) or isinstance(gdf, Series):
        return __row_similarity(gdf, other[[right_key_col, 'geometry']], left_key_col, right_key_col, only_intersections, method)
    
    sim_gdf = gdf.copy()
    sim_gdf = sim_gdf.apply(lambda x: __row_similarity(x, other[[right_key_col, 'geometry']], left_key_col, right_key_col, only_intersections, method), axis=1)
    sim_gdf = concat(sim_gdf.values)
    return sim_gdf