# SPDX-License-Identifier: CC-BY-NC-SA-4.0
# Licença: Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International.

import re
import io
import sys
import time
import json
import argparse
import zipfile
import shutil
import logging
import platform
from logging.handlers import RotatingFileHandler
from importlib.metadata import PackageNotFoundError, version
from datetime import datetime
from dataclasses import dataclass, field
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List, Optional, Sequence

import requests
import pandas as pd
import geopandas as gpd
from tqdm import tqdm

# ==============================================================================
# Configuração geral
# ==============================================================================
FORMATO_TIMESTAMP = "%Y%m%d_%H%M"

# Nomes de colunas usados em várias etapas do pipeline.
COL_VELOCIDADE = "velocidade_download_mbps"
COL_DISPOSITIVOS = "total_dispositivos"
COL_SUBTOTAL_REAL = "subtotal_dl_real"
COL_TRIMESTRE = "ano_trimestre"
COL_GEOMETRIA = "geometry"
COL_COD_MUNICIPIO = "CD_GEOCODI"
COL_NOME_MUNICIPIO = "NOME"
COL_UF = "UF"
COL_RGI = "rgi"
COL_NOME_RGI = "nome_rgi"
COL_RGINT = "rgint"
COL_NOME_RGINT = "nome_rgint"
COL_SIGLA_UF = "sigla_uf"
COL_NOME_UF = "nome_uf"
COL_GRANDE_REGIAO = "grande_regiao"
COL_QUATRO_BRASIS = "quatro_brasis"
COL_LOCALIDADE = "localidade"
COL_DOWNLOAD_KBPS = "avg_d_kbps"
COL_DEVICES_OOKLA = "devices"
COL_SUBTOTAL_DOWNLOAD = "subtotal_download"
COL_SOMA_SUBTOTAL_DL = "soma_subtotal_dl"
COL_SOMA_DEVICES = "soma_devices"
COL_TOTAL_SUBTOTAL = "total_subtotal"
COL_TOTAL_DEVICES = "total_dev"

PADRAO_TRIMESTRE = re.compile(r"^\d{4}Q[1-4]$")
MESES_INICIO_TRIMESTRE = {"1": "01-01", "2": "04-01", "3": "07-01", "4": "10-01"}
TIPOS_CONEXAO = ("fixed", "mobile")
CRS_WGS84 = "EPSG:4326"
CRS_BRASIL_METRICO = "EPSG:5880"
LOCALIDADE_BRASIL = "Brasil"
_AVISO_PYOGRIO_AUSENTE_EMITIDO = False


def gerar_timestamp_execucao() -> str:
    """Gera o identificador temporal usado nos arquivos da execução."""
    return datetime.now().strftime(FORMATO_TIMESTAMP)


def configurar_logging(caminho_log: Path) -> None:
    """Configura logs apenas no processo principal."""
    logger_raiz = logging.getLogger()
    for handler in logger_raiz.handlers[:]:
        logger_raiz.removeHandler(handler)
        handler.close()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            RotatingFileHandler(caminho_log, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def obter_versao_pacote(nome_pacote: str) -> str:
    """Retorna a versão instalada de um pacote Python, quando disponível."""
    try:
        return version(nome_pacote)
    except PackageNotFoundError:
        return "não instalado"


# ==============================================================================
# Configuração e estruturas de apoio
# ==============================================================================
@dataclass(frozen=True)
class PipelineConfig:
    """Parâmetros, caminhos e mapeamentos fixos do pipeline Ookla/IBGE."""
    pasta_raiz: Path = field(default_factory=Path.cwd)
    timestamp_execucao: str = field(default_factory=gerar_timestamp_execucao)
    url_malha_ibge: str = (
        "https://geoftp.ibge.gov.br/organizacao_do_territorio/divisao_regional/"
        "divisao_regional_do_brasil/divisao_regional_do_brasil_em_regioes_geograficas_2017/"
        "shp/RG2017_regioesgeograficas2017_20180911.zip"
    )
    http_timeout: int = 60
    n_workers: int = 1

    br_lon_min: float = -74.0
    br_lon_max: float = -34.0
    br_lat_min: float = -34.0
    br_lat_max: float = 6.0

    mapa_ufs: Dict[int, Dict[str, str]] = field(default_factory=lambda: {
        11: {"sigla": "RO", "nome": "Rondônia"}, 12: {"sigla": "AC", "nome": "Acre"},
        13: {"sigla": "AM", "nome": "Amazonas"}, 14: {"sigla": "RR", "nome": "Roraima"},
        15: {"sigla": "PA", "nome": "Pará"}, 16: {"sigla": "AP", "nome": "Amapá"},
        17: {"sigla": "TO", "nome": "Tocantins"}, 21: {"sigla": "MA", "nome": "Maranhão"},
        22: {"sigla": "PI", "nome": "Piauí"}, 23: {"sigla": "CE", "nome": "Ceará"},
        24: {"sigla": "RN", "nome": "Rio Grande do Norte"}, 25: {"sigla": "PB", "nome": "Paraíba"},
        26: {"sigla": "PE", "nome": "Pernambuco"}, 27: {"sigla": "AL", "nome": "Alagoas"},
        28: {"sigla": "SE", "nome": "Sergipe"}, 29: {"sigla": "BA", "nome": "Bahia"},
        31: {"sigla": "MG", "nome": "Minas Gerais"}, 32: {"sigla": "ES", "nome": "Espírito Santo"},
        33: {"sigla": "RJ", "nome": "Rio de Janeiro"}, 35: {"sigla": "SP", "nome": "São Paulo"},
        41: {"sigla": "PR", "nome": "Paraná"}, 42: {"sigla": "SC", "nome": "Santa Catarina"},
        43: {"sigla": "RS", "nome": "Rio Grande do Sul"}, 50: {"sigla": "MS", "nome": "Mato Grosso do Sul"},
        51: {"sigla": "MT", "nome": "Mato Grosso"}, 52: {"sigla": "GO", "nome": "Goiás"},
        53: {"sigla": "DF", "nome": "Distrito Federal"}
    })

    mapa_grandes_regioes: Dict[int, str] = field(default_factory=lambda: {
        11: "Região Norte", 12: "Região Norte", 13: "Região Norte", 14: "Região Norte",
        15: "Região Norte", 16: "Região Norte", 17: "Região Norte",
        21: "Região Nordeste", 22: "Região Nordeste", 23: "Região Nordeste", 24: "Região Nordeste",
        25: "Região Nordeste", 26: "Região Nordeste", 27: "Região Nordeste", 28: "Região Nordeste",
        29: "Região Nordeste", 31: "Região Sudeste", 32: "Região Sudeste", 33: "Região Sudeste",
        35: "Região Sudeste", 41: "Região Sul", 42: "Região Sul", 43: "Região Sul",
        50: "Região Centro-Oeste", 51: "Região Centro-Oeste", 52: "Região Centro-Oeste", 53: "Região Centro-Oeste"
    })

    mapa_quatro_brasis: Dict[int, str] = field(default_factory=lambda: {
        11: "Região Amazônica", 12: "Região Amazônica", 13: "Região Amazônica",
        14: "Região Amazônica", 15: "Região Amazônica", 16: "Região Amazônica",
        17: "Região Centro-Oeste", 50: "Região Centro-Oeste", 51: "Região Centro-Oeste",
        52: "Região Centro-Oeste", 53: "Região Centro-Oeste",
        21: "Região Nordeste", 22: "Região Nordeste", 23: "Região Nordeste", 24: "Região Nordeste",
        25: "Região Nordeste", 26: "Região Nordeste", 27: "Região Nordeste", 28: "Região Nordeste",
        29: "Região Nordeste", 31: "Região Concentrada", 32: "Região Concentrada",
        33: "Região Concentrada", 35: "Região Concentrada", 41: "Região Concentrada",
        42: "Região Concentrada", 43: "Região Concentrada"
    })

    @property
    def pasta_ookla(self) -> Path:
        return self.pasta_raiz / "dados" / "ookla"

    @property
    def pasta_ibge(self) -> Path:
        return self.pasta_raiz / "dados" / "ibge"

    @property
    def pasta_saida_base(self) -> Path:
        return self.pasta_raiz / f"processa_ookla_ibge_output_{self.timestamp_execucao}"

    def obter_pasta_saida_especifica(self, tipo_conexao: str) -> Path:
        """Retorna a subpasta de saída do tipo de conexão informado."""
        return self.pasta_saida_base / tipo_conexao

    def garantir_diretorios(self) -> None:
        """Cria os diretórios usados pelo pipeline, se ainda não existirem."""
        self.pasta_ookla.mkdir(parents=True, exist_ok=True)
        self.pasta_ibge.mkdir(parents=True, exist_ok=True)
        self.pasta_saida_base.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class ExportacaoConfig:
    """Define uma saída agregada do pipeline."""

    chaves_grupo: Sequence[str]
    nome_base: str
    col_id: str
    col_nome: Optional[str] = None
    col_extra: Optional[Sequence[str]] = None


@dataclass(frozen=True)
class ResultadoTrimestre:
    """Dados e estatísticas de processamento de um trimestre."""

    trimestre: str
    dados: pd.DataFrame
    tiles_lidos: int
    tiles_validos: int
    tiles_no_recorte: int
    tiles_com_municipio: int
    municipios_com_dados: int


def extrair_zip_seguro(arquivo_zip: zipfile.ZipFile, pasta_destino: Path) -> None:
    """Extrai um ZIP sem permitir arquivos fora da pasta de destino."""
    destino_resolvido = pasta_destino.resolve()

    for membro in arquivo_zip.infolist():
        caminho_membro = (pasta_destino / membro.filename).resolve()
        if caminho_membro != destino_resolvido and destino_resolvido not in caminho_membro.parents:
            raise ValueError(f"Arquivo ZIP contém caminho inseguro: {membro.filename}")

    arquivo_zip.extractall(pasta_destino)


def shapefile_completo(caminho_shp: Path) -> bool:
    """Verifica se os arquivos básicos de um shapefile estão presentes."""
    extensoes_obrigatorias = (".shp", ".dbf", ".shx", ".prj")
    return all(caminho_shp.with_suffix(extensao).exists() for extensao in extensoes_obrigatorias)


def shapefile_ookla_completo(pasta_trimestre: Path, tipo_conexao: str) -> bool:
    """Verifica se o cache local contém os arquivos básicos do shapefile da Ookla."""
    return shapefile_completo(pasta_trimestre / f"gps_{tipo_conexao}_tiles.shp")


def validar_colunas_obrigatorias(df: pd.DataFrame, colunas: Sequence[str], contexto: str) -> bool:
    """Registra erro e retorna False quando o DataFrame não contém o esquema mínimo."""
    colunas_ausentes = [col for col in colunas if col not in df.columns]
    if colunas_ausentes:
        logging.error(f"{contexto} Colunas ausentes: {colunas_ausentes}")
        return False
    return True


def calcular_media_ponderada_download(
    df: pd.DataFrame,
    coluna_subtotal: str,
    coluna_pesos: str,
    coluna_saida: str = COL_VELOCIDADE,
) -> pd.DataFrame:
    """Calcula download médio em Mbps, ponderado pelo número de dispositivos."""
    df[coluna_saida] = 0.0
    mascara_valida = df[coluna_pesos] > 0
    df.loc[mascara_valida, coluna_saida] = (
        df.loc[mascara_valida, coluna_subtotal] / df.loc[mascara_valida, coluna_pesos]
    ) / 1000.0
    return df


def normalizar_workers(valor: int) -> int:
    """Retorna pelo menos um worker, mesmo quando a entrada é zero ou negativa."""
    if valor < 1:
        logging.warning("Quantidade de workers inválida (%s). Usando 1 worker.", valor)
        return 1
    return valor


def ler_arquivo_geografico(caminho: Path, **kwargs) -> gpd.GeoDataFrame:
    """Lê um arquivo geográfico com pyogrio, ou com o engine padrão do GeoPandas."""
    global _AVISO_PYOGRIO_AUSENTE_EMITIDO

    try:
        return gpd.read_file(caminho, engine="pyogrio", **kwargs)
    except ModuleNotFoundError as exc:
        if exc.name != "pyogrio":
            raise

        if not _AVISO_PYOGRIO_AUSENTE_EMITIDO:
            logging.warning(
                "Dependência opcional 'pyogrio' não encontrada. "
                "Usando o engine padrão do GeoPandas."
            )
            _AVISO_PYOGRIO_AUSENTE_EMITIDO = True

        return gpd.read_file(caminho, **kwargs)


# ==============================================================================
# Entrada, validação e download
# ==============================================================================
def validar_trimestre(trimestre: str) -> bool:
    """Valida trimestres no formato YYYYQ#."""
    return bool(PADRAO_TRIMESTRE.match(trimestre))


def ordenar_trimestre(trimestre: str) -> int:
    """Converte YYYYQ# para um inteiro comparável."""
    return int(trimestre[:4]) * 4 + int(trimestre[5])


def obter_trimestre_atual() -> str:
    """Retorna o trimestre corrente no formato YYYYQ#."""
    hoje = datetime.now()
    trimestre = ((hoje.month - 1) // 3) + 1
    return f"{hoje.year}Q{trimestre}"


def localizar_shp_ibge(config: PipelineConfig) -> Optional[Path]:
    """Localiza o shapefile da malha regional do IBGE."""
    for arquivo in sorted(config.pasta_ibge.rglob("*.shp")):
        if "regioesgeograficas" in arquivo.name.lower() and shapefile_completo(arquivo):
            return arquivo
    return None


def baixar_malha_ibge(config: PipelineConfig) -> Path:
    """Baixa e extrai a malha regional do IBGE quando ela não existe localmente."""
    config.garantir_diretorios()
    caminho_existente = localizar_shp_ibge(config)
    if caminho_existente:
        logging.info(f"[IBGE] Malha regional encontrada: {caminho_existente}")
        return caminho_existente

    logging.info("[IBGE] Malha regional não encontrada localmente. Baixando arquivo.")
    try:
        response = requests.get(config.url_malha_ibge, timeout=config.http_timeout)
        response.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            extrair_zip_seguro(z, config.pasta_ibge)
        logging.info("[IBGE] Malha regional baixada e extraída.")
    except requests.RequestException as e:
        logging.error(f"[IBGE] Falha ao baixar a malha regional: {e}")
        raise
    except (zipfile.BadZipFile, ValueError) as e:
        logging.error(f"[IBGE] ZIP inválido ou com caminhos inseguros: {e}")
        raise

    shp_novo = localizar_shp_ibge(config)
    if not shp_novo:
        raise FileNotFoundError("Shapefile do IBGE não encontrado após a extração.")
    return shp_novo


def baixar_trimestre_ookla(
    trimestre: str,
    tipo_conexao: str,
    config: PipelineConfig,
    session: requests.Session,
) -> Optional[Path]:
    """Baixa os tiles de performance da Ookla para um trimestre."""
    nome_pasta = f"{trimestre}_performance_{tipo_conexao}_tiles"
    caminho_pasta = config.pasta_ookla / nome_pasta

    if shapefile_ookla_completo(caminho_pasta, tipo_conexao):
        return caminho_pasta

    if caminho_pasta.exists():
        logging.warning(f"[OOKLA] Cache incompleto em {caminho_pasta}. Baixando novamente.")
        shutil.rmtree(caminho_pasta, ignore_errors=True)

    logging.info(f"[OOKLA] Baixando {trimestre} ({tipo_conexao}).")
    ano, q = trimestre[:4], trimestre[5]

    url = (
        "https://ookla-open-data.s3.amazonaws.com/shapefiles/performance/"
        f"type={tipo_conexao}/year={ano}/quarter={q}/"
        f"{ano}-{MESES_INICIO_TRIMESTRE[q]}_performance_{tipo_conexao}_tiles.zip"
    )

    try:
        response = session.get(url, timeout=config.http_timeout)
        response.raise_for_status()
        caminho_pasta.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            extrair_zip_seguro(z, caminho_pasta)
        logging.info(f"[OOKLA] Dados de {trimestre} salvos em: {caminho_pasta}")
        return caminho_pasta
    except requests.RequestException as e:
        logging.warning(f"[OOKLA] Não foi possível baixar {trimestre}: {e}")
        return None
    except (zipfile.BadZipFile, ValueError) as e:
        logging.warning(f"[OOKLA] ZIP inválido em {trimestre}: {e}")
        if caminho_pasta.exists():
            shutil.rmtree(caminho_pasta, ignore_errors=True)
        return None


def gerar_lista_trimestres(inicio: str, fim: str) -> List[str]:
    """Gera a lista contínua de trimestres entre início e fim."""
    if not validar_trimestre(inicio) or not validar_trimestre(fim):
        raise ValueError("Trimestres devem seguir o padrão YYYYQ[1-4].")

    lista = []
    ano_in, q_in = int(inicio[:4]), int(inicio[5])
    ano_fim, q_fim = int(fim[:4]), int(fim[5])

    if (ano_in > ano_fim) or (ano_in == ano_fim and q_in > q_fim):
        raise ValueError(
            f"Intervalo inválido: o trimestre inicial ({inicio}) "
            f"não pode ser posterior ao final ({fim})."
        )

    ano_atual, q_atual = ano_in, q_in
    while (ano_atual < ano_fim) or (ano_atual == ano_fim and q_atual <= q_fim):
        lista.append(f"{ano_atual}Q{q_atual}")
        q_atual += 1
        if q_atual > 4:
            q_atual = 1
            ano_atual += 1
    return lista


# ==============================================================================
# Processamento geoespacial
# ==============================================================================
def higienizar_dados_ookla(gdf_ookla: gpd.GeoDataFrame, trimestre: str) -> Optional[gpd.GeoDataFrame]:
    """Valida esquema mínimo e converte métricas da Ookla para valores numéricos."""
    colunas_obrigatorias = [COL_DOWNLOAD_KBPS, COL_DEVICES_OOKLA, COL_GEOMETRIA]
    contexto = f"[{trimestre}] Esquema inesperado nos dados da Ookla."
    if not validar_colunas_obrigatorias(gdf_ookla, colunas_obrigatorias, contexto):
        return None

    gdf_ookla = gdf_ookla.copy()
    gdf_ookla[COL_DOWNLOAD_KBPS] = pd.to_numeric(gdf_ookla[COL_DOWNLOAD_KBPS], errors="coerce")
    gdf_ookla[COL_DEVICES_OOKLA] = pd.to_numeric(gdf_ookla[COL_DEVICES_OOKLA], errors="coerce")

    total_original = len(gdf_ookla)
    gdf_ookla = gdf_ookla.dropna(subset=[COL_DOWNLOAD_KBPS, COL_DEVICES_OOKLA])
    total_descartado = total_original - len(gdf_ookla)

    if total_descartado:
        logging.info(
            "[%s] %s registros descartados por valores ausentes ou inválidos nas métricas da Ookla.",
            trimestre,
            total_descartado,
        )

    if gdf_ookla.empty:
        logging.warning(f"[{trimestre}] Nenhum registro válido após a higienização dos dados da Ookla.")
        return None

    return gdf_ookla


def preparar_geometrias_ookla(
    gdf_ookla: gpd.GeoDataFrame,
    config: PipelineConfig,
    crs_destino: str,
    trimestre: str,
) -> Optional[gpd.GeoDataFrame]:
    """Recorta os tiles para o Brasil e calcula centroides em CRS métrico."""
    if gdf_ookla.crs is None:
        logging.warning(f"[{trimestre}] Shapefile da Ookla sem CRS declarado. Assumindo {CRS_WGS84}.")
        gdf_ookla = gdf_ookla.set_crs(CRS_WGS84)

    gdf_ookla = gdf_ookla.to_crs(CRS_WGS84)
    gdf_ookla = gdf_ookla.cx[
        config.br_lon_min:config.br_lon_max,
        config.br_lat_min:config.br_lat_max,
    ].copy()

    if gdf_ookla.empty:
        logging.warning(f"[{trimestre}] Nenhum tile da Ookla encontrado no recorte geográfico do Brasil.")
        return None

    gdf_metric = gdf_ookla.to_crs(CRS_BRASIL_METRICO)
    gdf_metric[COL_GEOMETRIA] = gdf_metric.geometry.centroid
    return gdf_metric.to_crs(crs_destino)


def processar_trimestre_isolado(
    trimestre: str,
    tipo_conexao: str,
    config: PipelineConfig,
    df_ibge_reduzido: pd.DataFrame,
    crs_ibge: str,
) -> Optional[ResultadoTrimestre]:
    """Processa um trimestre e retorna métricas agregadas por município."""
    with requests.Session() as session:
        pasta_tri = baixar_trimestre_ookla(trimestre, tipo_conexao, config, session)

    if not pasta_tri:
        return None

    shp_ookla = pasta_tri / f"gps_{tipo_conexao}_tiles.shp"

    if not shp_ookla.exists():
        logging.error(f"[{trimestre}] Shapefile esperado não encontrado: {shp_ookla}")
        return None

    try:
        gdf_ookla = ler_arquivo_geografico(shp_ookla)
    except (FileNotFoundError, ValueError) as err_leitura:
        logging.error(f"[{trimestre}] Shapefile local inválido: {err_leitura}. Removendo cache do trimestre.")
        if pasta_tri.exists():
            shutil.rmtree(pasta_tri, ignore_errors=True)
        return None

    tiles_lidos = len(gdf_ookla)
    gdf_ookla = higienizar_dados_ookla(gdf_ookla, trimestre)
    if gdf_ookla is None:
        return None
    tiles_validos = len(gdf_ookla)

    gdf_ibge_local = gpd.GeoDataFrame(
        df_ibge_reduzido.copy(),
        geometry=COL_GEOMETRIA,
        crs=crs_ibge,
    )
    gdf_ookla = preparar_geometrias_ookla(gdf_ookla, config, crs_ibge, trimestre)
    if gdf_ookla is None:
        return None
    tiles_no_recorte = len(gdf_ookla)

    sjoin_res = gpd.sjoin(
        gdf_ookla,
        gdf_ibge_local[[COL_COD_MUNICIPIO, COL_GEOMETRIA]],
        how="inner",
        predicate="within",
    )
    if sjoin_res.empty:
        return None
    tiles_com_municipio = len(sjoin_res)

    sjoin_res[COL_SUBTOTAL_DOWNLOAD] = sjoin_res[COL_DOWNLOAD_KBPS] * sjoin_res[COL_DEVICES_OOKLA]

    grupo_mun = sjoin_res.groupby(COL_COD_MUNICIPIO).agg(
        soma_subtotal_dl=(COL_SUBTOTAL_DOWNLOAD, "sum"),
        soma_devices=(COL_DEVICES_OOKLA, "sum")
    ).reset_index()

    grupo_mun = calcular_media_ponderada_download(
        grupo_mun,
        coluna_subtotal=COL_SOMA_SUBTOTAL_DL,
        coluna_pesos=COL_SOMA_DEVICES,
    )

    grupo_mun[COL_TRIMESTRE] = trimestre
    grupo_mun = grupo_mun[[COL_TRIMESTRE, COL_COD_MUNICIPIO, COL_VELOCIDADE, COL_SOMA_DEVICES]]
    grupo_mun = grupo_mun.rename(columns={COL_SOMA_DEVICES: COL_DISPOSITIVOS})

    return ResultadoTrimestre(
        trimestre=trimestre,
        dados=grupo_mun,
        tiles_lidos=tiles_lidos,
        tiles_validos=tiles_validos,
        tiles_no_recorte=tiles_no_recorte,
        tiles_com_municipio=tiles_com_municipio,
        municipios_com_dados=len(grupo_mun),
    )


# ==============================================================================
# Agregação e exportação
# ==============================================================================
def agregar_e_exportar(
    df: pd.DataFrame,
    chaves_grupo: Sequence[str],
    nome_base: str,
    col_id: str,
    col_nome: Optional[str] = None,
    col_extra: Optional[Sequence[str]] = None,
    pasta_saida: Optional[Path] = None,
) -> None:
    """Agrega por recorte territorial e exporta CSVs nos formatos long e wide."""
    df_agrupado = df.groupby(list(chaves_grupo)).agg(
        total_subtotal=(COL_SUBTOTAL_REAL, "sum"),
        total_dev=(COL_DISPOSITIVOS, "sum")
    ).reset_index()

    df_agrupado = calcular_media_ponderada_download(
        df_agrupado,
        coluna_subtotal=COL_TOTAL_SUBTOTAL,
        coluna_pesos=COL_TOTAL_DEVICES,
    )
    df_agrupado = df_agrupado.rename(columns={COL_TOTAL_DEVICES: COL_DISPOSITIVOS})

    chaves_ordenacao = [col_id, COL_TRIMESTRE]
    df_agrupado = df_agrupado.sort_values(by=chaves_ordenacao)

    if col_nome is None or col_id == col_nome:
        cols_long = [COL_TRIMESTRE, col_id]
        chaves_index = [col_id]
    else:
        cols_long = [COL_TRIMESTRE, col_id, col_nome]
        chaves_index = [col_id, col_nome]

    if col_extra:
        cols_long.extend(col_extra)
        chaves_index.extend(col_extra)

    cols_long.extend([COL_VELOCIDADE, COL_DISPOSITIVOS])
    df_long = df_agrupado[cols_long].copy()

    if pasta_saida is None:
        pasta_saida = Path.cwd()

    pasta_saida.mkdir(parents=True, exist_ok=True)
    caminho_long = pasta_saida / f"serie_velocidade_{nome_base}_long.csv"
    caminho_wide = pasta_saida / f"serie_velocidade_{nome_base}_wide.csv"

    try:
        df_long.to_csv(caminho_long, index=False, sep=",", decimal=".")
        df_wide = df_long.pivot(index=chaves_index, columns=COL_TRIMESTRE, values=COL_VELOCIDADE).reset_index()
        df_wide.to_csv(caminho_wide, index=False, sep=";", decimal=",")
    except OSError as e:
        logging.error(f"Falha ao gravar arquivos de {nome_base} em {pasta_saida}: {e}")


# ==============================================================================
# Orquestração
# ==============================================================================
def preparar_malha_ibge(gdf_ibge: gpd.GeoDataFrame, config: PipelineConfig) -> gpd.GeoDataFrame:
    """Valida e enriquece a malha IBGE com atributos regionais usados nas saídas."""
    colunas_inteiras = [COL_COD_MUNICIPIO, COL_UF, COL_RGI, COL_RGINT]
    colunas_obrigatorias = [
        *colunas_inteiras,
        COL_NOME_MUNICIPIO,
        COL_NOME_RGI,
        COL_NOME_RGINT,
        COL_GEOMETRIA,
    ]

    if not validar_colunas_obrigatorias(gdf_ibge, colunas_obrigatorias, "[IBGE] Esquema inválido da malha IBGE."):
        raise ValueError("Malha IBGE sem as colunas obrigatórias.")

    gdf_ibge = gdf_ibge.copy()
    if gdf_ibge.crs is None:
        logging.warning(f"[IBGE] Malha IBGE sem CRS declarado. Assumindo {CRS_WGS84}.")
        gdf_ibge = gdf_ibge.set_crs(CRS_WGS84)

    for col in colunas_inteiras:
        gdf_ibge[col] = pd.to_numeric(gdf_ibge[col], errors="raise").astype(int)

    dic_siglas = {k: v["sigla"] for k, v in config.mapa_ufs.items()}
    dic_nomes = {k: v["nome"] for k, v in config.mapa_ufs.items()}

    gdf_ibge[COL_SIGLA_UF] = gdf_ibge[COL_UF].map(dic_siglas)
    gdf_ibge[COL_NOME_UF] = gdf_ibge[COL_UF].map(dic_nomes)
    gdf_ibge[COL_GRANDE_REGIAO] = gdf_ibge[COL_UF].map(config.mapa_grandes_regioes).fillna("Ignorado")
    gdf_ibge[COL_QUATRO_BRASIS] = gdf_ibge[COL_UF].map(config.mapa_quatro_brasis).fillna("Ignorado")

    return gdf_ibge


def registrar_contexto_execucao(
    config: PipelineConfig,
    tipo_conexao: str,
    trimestres_alvo: Sequence[str],
    n_workers: int,
) -> None:
    """Registra parâmetros e versões relevantes da execução."""
    logging.info(
        "Execução: tipo=%s; período=%s a %s; trimestres=%s; workers=%s.",
        tipo_conexao,
        trimestres_alvo[0],
        trimestres_alvo[-1],
        len(trimestres_alvo),
        n_workers,
    )
    logging.info(f"Diretório base: {config.pasta_raiz}")
    logging.info(f"Diretório de saída: {config.obter_pasta_saida_especifica(tipo_conexao)}")
    logging.info(
        "Ambiente: Python %s; pandas %s; GeoPandas %s; Shapely %s.",
        platform.python_version(),
        pd.__version__,
        gpd.__version__,
        obter_versao_pacote("shapely"),
    )


def gravar_manifesto_execucao(
    config: PipelineConfig,
    tipo_conexao: str,
    trimestres_alvo: Sequence[str],
    n_workers: int,
    resultados: Sequence[ResultadoTrimestre],
    total_municipios_ibge: int,
    df_consolidado: pd.DataFrame,
    tempo_total_segundos: float,
    pasta_saida: Path,
) -> None:
    """Grava um resumo da execução junto aos arquivos gerados."""
    municipios_com_dados = int(df_consolidado[COL_COD_MUNICIPIO].nunique())
    manifesto = {
        "timestamp_execucao": config.timestamp_execucao,
        "tipo_conexao": tipo_conexao,
        "trimestres_solicitados": list(trimestres_alvo),
        "trimestres_processados": [resultado.trimestre for resultado in resultados],
        "workers": n_workers,
        "diretorio_base": str(config.pasta_raiz),
        "diretorio_saida": str(pasta_saida),
        "tempo_total_segundos": round(tempo_total_segundos, 2),
        "ambiente": {
            "python": platform.python_version(),
            "pandas": pd.__version__,
            "geopandas": gpd.__version__,
            "shapely": obter_versao_pacote("shapely"),
        },
        "resumo": {
            "municipios_ibge": total_municipios_ibge,
            "municipios_com_dados": municipios_com_dados,
            "municipios_sem_dados_no_periodo": total_municipios_ibge - municipios_com_dados,
            "registros_municipais": int(sum(len(resultado.dados) for resultado in resultados)),
            "tiles_lidos": int(sum(resultado.tiles_lidos for resultado in resultados)),
            "tiles_validos": int(sum(resultado.tiles_validos for resultado in resultados)),
            "tiles_no_recorte_brasil": int(sum(resultado.tiles_no_recorte for resultado in resultados)),
            "tiles_associados_a_municipios": int(sum(resultado.tiles_com_municipio for resultado in resultados)),
        },
        "por_trimestre": [
            {
                "trimestre": resultado.trimestre,
                "tiles_lidos": resultado.tiles_lidos,
                "tiles_validos": resultado.tiles_validos,
                "tiles_no_recorte_brasil": resultado.tiles_no_recorte,
                "tiles_associados_a_municipios": resultado.tiles_com_municipio,
                "municipios_com_dados": resultado.municipios_com_dados,
            }
            for resultado in resultados
        ],
        "arquivos_csv": sorted(arquivo.name for arquivo in pasta_saida.glob("*.csv")),
    }

    caminho_manifesto = pasta_saida / "manifesto_execucao.json"
    with caminho_manifesto.open("w", encoding="utf-8") as arquivo:
        json.dump(manifesto, arquivo, ensure_ascii=False, indent=2)

    logging.info(f"Manifesto da execução gravado em: {caminho_manifesto}")


def executar_pipeline(config: PipelineConfig, tipo_conexao: str, trimestres_alvo: List[str]) -> None:
    """Executa o pipeline e grava as séries agregadas."""
    t_pipe_inicio = time.perf_counter()

    caminho_shp_ibge = localizar_shp_ibge(config)
    if not caminho_shp_ibge:
        logging.error("Shapefile do IBGE não encontrado.")
        return

    logging.info("Carregando malha regional do IBGE.")
    gdf_ibge = preparar_malha_ibge(ler_arquivo_geografico(caminho_shp_ibge), config)
    crs_ibge = str(gdf_ibge.crs)

    df_ibge_reduzido = gdf_ibge[[COL_COD_MUNICIPIO, COL_GEOMETRIA]].copy()

    total_tarefas = len(trimestres_alvo)
    n_workers = min(normalizar_workers(config.n_workers), total_tarefas)
    registrar_contexto_execucao(config, tipo_conexao, trimestres_alvo, n_workers)

    resultados_trimestres: List[ResultadoTrimestre] = []
    if n_workers > 1:
        logging.info(f"Processando trimestres em paralelo ({n_workers} workers).")
        with ProcessPoolExecutor(max_workers=n_workers) as executor:
            futures = {
                executor.submit(
                    processar_trimestre_isolado,
                    tri,
                    tipo_conexao,
                    config,
                    df_ibge_reduzido,
                    crs_ibge,
                ): tri
                for tri in trimestres_alvo
            }
            for fut in tqdm(
                as_completed(futures),
                total=total_tarefas,
                desc="Processando trimestres (paralelo)",
                unit="tri",
            ):
                tri = futures[fut]
                try:
                    res = fut.result()
                except Exception as exc:
                    logging.exception(f"[{tri}] Erro ao processar trimestre no worker: {exc}")
                    continue
                if res is not None:
                    resultados_trimestres.append(res)
                else:
                    logging.warning(f"[{tri}] Trimestre sem dados processados.")
    else:
        logging.info("Processando trimestres sequencialmente.")
        for tri in tqdm(trimestres_alvo, desc="Processando trimestres (sequencial)", unit="tri"):
            try:
                res = processar_trimestre_isolado(tri, tipo_conexao, config, df_ibge_reduzido, crs_ibge)
            except Exception as exc:
                logging.exception(f"[{tri}] Erro ao processar trimestre: {exc}")
                continue
            if res is not None:
                resultados_trimestres.append(res)
            else:
                logging.warning(f"[{tri}] Trimestre sem dados processados.")

    if not resultados_trimestres:
        logging.error("Nenhum dado válido foi obtido para os trimestres informados.")
        return

    resultados_trimestres = sorted(resultados_trimestres, key=lambda resultado: ordenar_trimestre(resultado.trimestre))
    df_base_municipios = pd.concat([resultado.dados for resultado in resultados_trimestres], ignore_index=True)

    # Mantém apenas municípios com dados reais da Ookla no período processado.
    df_consolidado = pd.merge(
        gdf_ibge.drop(columns=COL_GEOMETRIA),
        df_base_municipios,
        on=COL_COD_MUNICIPIO,
        how="inner",
    )

    df_consolidado[COL_SUBTOTAL_REAL] = (df_consolidado[COL_VELOCIDADE] * 1000.0) * df_consolidado[COL_DISPOSITIVOS]

    pasta_saida_final = config.obter_pasta_saida_especifica(tipo_conexao)
    logging.info(f"Gravando arquivos consolidados em: {pasta_saida_final}")

    exportacoes = [
        ExportacaoConfig(
            [COL_TRIMESTRE, COL_COD_MUNICIPIO, COL_NOME_MUNICIPIO, COL_SIGLA_UF],
            "municipios",
            COL_COD_MUNICIPIO,
            COL_NOME_MUNICIPIO,
            [COL_SIGLA_UF],
        ),
        ExportacaoConfig(
            [COL_TRIMESTRE, COL_RGI, COL_NOME_RGI, COL_SIGLA_UF],
            "regioes_imediatas",
            COL_RGI,
            COL_NOME_RGI,
            [COL_SIGLA_UF],
        ),
        ExportacaoConfig(
            [COL_TRIMESTRE, COL_RGINT, COL_NOME_RGINT, COL_SIGLA_UF],
            "regioes_intermediarias",
            COL_RGINT,
            COL_NOME_RGINT,
            [COL_SIGLA_UF],
        ),
        ExportacaoConfig([COL_TRIMESTRE, COL_GRANDE_REGIAO], "grandes_regioes", COL_GRANDE_REGIAO, COL_GRANDE_REGIAO),
        ExportacaoConfig([COL_TRIMESTRE, COL_QUATRO_BRASIS], "quatro_brasis", COL_QUATRO_BRASIS, COL_QUATRO_BRASIS),
    ]

    for exportacao in exportacoes:
        agregar_e_exportar(
            df_consolidado,
            exportacao.chaves_grupo,
            exportacao.nome_base,
            exportacao.col_id,
            exportacao.col_nome,
            exportacao.col_extra,
            pasta_saida_final,
        )

    df_consolidado[COL_LOCALIDADE] = LOCALIDADE_BRASIL
    agregar_e_exportar(
        df_consolidado,
        [COL_TRIMESTRE, COL_LOCALIDADE],
        "brasil_consolidado",
        COL_LOCALIDADE,
        COL_LOCALIDADE,
        pasta_saida=pasta_saida_final,
    )

    t_pipe_fim = time.perf_counter()
    tempo_total = t_pipe_fim - t_pipe_inicio
    total_municipios_ibge = int(gdf_ibge[COL_COD_MUNICIPIO].nunique())
    municipios_com_dados = int(df_consolidado[COL_COD_MUNICIPIO].nunique())
    gravar_manifesto_execucao(
        config=config,
        tipo_conexao=tipo_conexao,
        trimestres_alvo=trimestres_alvo,
        n_workers=n_workers,
        resultados=resultados_trimestres,
        total_municipios_ibge=total_municipios_ibge,
        df_consolidado=df_consolidado,
        tempo_total_segundos=tempo_total,
        pasta_saida=pasta_saida_final,
    )
    logging.info(
        "Resumo: %s/%s trimestres processados; %s/%s municípios com dados.",
        len(resultados_trimestres),
        len(trimestres_alvo),
        municipios_com_dados,
        total_municipios_ibge,
    )
    logging.info(f"Pipeline concluído em {tempo_total:.2f} segundos.")


# ==============================================================================
# Interface de linha de comando
# ==============================================================================
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Processa dados da Ookla e agrega indicadores de download por recortes territoriais do IBGE."
    )
    parser.add_argument("--tipo", type=str, choices=TIPOS_CONEXAO, help="Tipo de conexão: 'fixed' ou 'mobile'")
    parser.add_argument("--inicio", type=str, help="Trimestre inicial no formato YYYYQ# (ex.: 2021Q1)")
    parser.add_argument("--fim", type=str, help="Trimestre final no formato YYYYQ# (ex.: 2022Q4)")
    parser.add_argument("--workers", type=int, default=1, help="Quantidade de processos paralelos (padrão: 1)")

    args = parser.parse_args()
    timestamp_execucao = gerar_timestamp_execucao()
    configurar_logging(Path.cwd() / f"processa_ookla_ibge_log_{timestamp_execucao}.log")

    if not args.tipo or not args.inicio or not args.fim:
        if not sys.stdin.isatty():
            parser.error("Informe os parâmetros --tipo, --inicio e --fim em execuções não interativas.")

        print(
            "Modo interativo. "
            "Para execução automatizada, informe os parâmetros --tipo, --inicio e --fim.\n"
        )
        tipo_conexao = input("Tipo de conexão ('fixed' ou 'mobile') [padrão: fixed]: ").strip().lower()
        if tipo_conexao not in TIPOS_CONEXAO:
            tipo_conexao = "fixed"
        trimestre_inicio = input("Trimestre inicial (ex.: 2021Q1): ").strip().upper()
        trimestre_fim = input("Trimestre final (ex.: 2022Q2): ").strip().upper()
        n_workers = input("Número de processos paralelos [padrão: 1]: ").strip()
        n_workers = normalizar_workers(int(n_workers) if n_workers.isdigit() else 1)
    else:
        tipo_conexao = args.tipo
        trimestre_inicio = args.inicio.strip().upper()
        trimestre_fim = args.fim.strip().upper()
        n_workers = normalizar_workers(args.workers)

    if not validar_trimestre(trimestre_inicio) or not validar_trimestre(trimestre_fim):
        logging.error("Trimestre inválido. Use o formato YYYYQ[1-4], por exemplo 2021Q1.")
        return

    try:
        trimestres_alvo = gerar_lista_trimestres(trimestre_inicio, trimestre_fim)
    except ValueError as val_err:
        logging.error(f"Intervalo de trimestres inválido: {val_err}")
        return

    trimestre_atual = obter_trimestre_atual()
    trimestres_futuros = [
        trimestre for trimestre in trimestres_alvo if ordenar_trimestre(trimestre) > ordenar_trimestre(trimestre_atual)
    ]
    if trimestres_futuros:
        logging.error(
            "A lista contém trimestres futuros (%s). O trimestre corrente é %s.",
            ", ".join(trimestres_futuros),
            trimestre_atual,
        )
        return

    config = PipelineConfig(n_workers=n_workers, timestamp_execucao=timestamp_execucao)

    try:
        baixar_malha_ibge(config)
    except Exception as e:
        logging.critical(f"Execução abortada: não foi possível obter a malha regional do IBGE: {e}")
        return

    try:
        executar_pipeline(config, tipo_conexao, trimestres_alvo)
    except KeyboardInterrupt:
        print("\n")
        logging.warning("Operação cancelada pelo usuário.")

if __name__ == "__main__":
    main()
