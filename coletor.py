# -*- coding: utf-8 -*-
"""
============================================================
  COLETOR v2 — APOLO SOLAR  (Railway / PostgreSQL)
============================================================
Le os inversores de uma usina na API da Chint e grava as
leituras no banco PostgreSQL (schema v2).

NOVIDADES DA v2 (schema v2):
  - A topologia (quantos MPPTs, quantas strings) NAO e mais
    fixa em 12/24. Cada inversor aponta para um MODELO, e o
    coletor le do banco quantos MPPTs/strings aquele modelo
    tem. Assim, inversores de modelos diferentes coexistem.
  - As leituras de canais sao gravadas em DUAS tabelas
    separadas: 'leitura_mppt' e 'leitura_string'. A antiga
    'leitura_canal' (com campo 'tipo' de texto) deixou de
    existir.
  - 'leitura_mppt' e 'leitura_string' guardam o inversor_id
    diretamente; a string guarda tambem o MPPT a que pertence.

CORRECAO (esta versao):
  - A leitura das colunas da resposta da Chint NAO usa mais
    posicoes fixas (IDX_*). Agora cada coluna e localizada
    pelo seu TITULO no cabecalho que a API devolve. Assim,
    se a Chint reordenar ou inserir colunas, o coletor
    continua pegando o dado certo.
  - Os IDX_* antigos viraram apenas FALLBACK: so sao usados
    se a resposta nao trouxer cabecalho. Quando o cabecalho
    existe, ele manda.

COMO RODA:
  - Roda UMA vez e encerra. O Railway repete a cada 5 min
    via cron job.

CREDENCIAIS — lidas de variaveis de ambiente no Railway:
  DATABASE_URL   -> string de conexao do PostgreSQL
  CHINT_TOKEN    -> token de acesso a API da Chint
  CHINT_USER_ID  -> id de usuario da Chint

Dependencia: psycopg. Ver requirements.txt.
============================================================
"""

from urllib.request import urlopen, Request
from datetime import datetime, timedelta, timezone
import json
import os
import re
import sys
import time
import unicodedata

import psycopg


# ============================================================
# CONFIGURACAO — lida do ambiente (Railway)
# ============================================================

DATABASE_URL = os.environ.get("DATABASE_URL")
TOKEN        = os.environ.get("CHINT_TOKEN")
USER_ID      = os.environ.get("CHINT_USER_ID")

# Só aborta por variaveis faltando quando rodado como script principal.
# Assim, outros scripts (ex: recuperar_hoje.py) podem importar este modulo
# para reusar as funcoes sem disparar o exit no momento do import.
if __name__ == "__main__" and (not DATABASE_URL or not TOKEN or not USER_ID):
    print("ERRO: variaveis de ambiente faltando.")
    print("  Configure no Railway: DATABASE_URL, CHINT_TOKEN, CHINT_USER_ID")
    sys.exit(1)

BASE = "https://solar.chintpower.com:8443"

HEADERS = {
    "Authorization":   f"Bearer {TOKEN}",
    "token":           TOKEN,
    "loginuserid":     USER_ID,
    "platformcode":    "2",
    "request-origin":  "web",
    "time-zone":       "America%2FSao_Paulo",
    "accept":          "application/json",
    "accept-language": "pt-PT",
}

# Slug da usina deste coletor (deve existir na tabela 'usina')
USINA_SLUG = "pk"

# Espera no inicio de cada execucao, em segundos, antes de consultar a Chint.
# O cron dispara em horarios multiplos de 5 (XX:00, XX:05, ...), mas a Chint
# leva alguns segundos para propagar a leitura do slot atual entre todos os
# inversores. Esperar antes de consultar evita pegar o slot anterior por
# corrida de timing.
DELAY_SEGUNDOS = 20

# Fuso do Brasil (a Chint publica no fuso de Sao Paulo conforme o header
# 'time-zone' que enviamos; o Railway roda em UTC, entao precisamos converter)
FUSO_BR = timezone(timedelta(hours=-3))

# Imprime o cabecalho (titulos das colunas) da Chint uma vez por execucao,
# para diagnostico/conferencia. Deixe True ate confirmar o mapeamento.
LOG_CABECALHO = True


# ============================================================
# FALLBACK — indices fixos antigos da resposta da API Chint
# ------------------------------------------------------------
# SO usados quando a resposta NAO traz cabecalho de titulos.
# Com cabecalho presente, a resolucao por TITULO (abaixo) tem
# prioridade e estes numeros sao ignorados.
# ============================================================
IDX_DATE   = 0           # carimbo da Chint, ex: "2026-05-27 13:45:16"
IDX_TYIELD = 4
IDX_DYIELD = 5
IDX_PAC    = 10
IDX_FREQ   = 18
IDX_UMPPT1 = 40
IDX_IMPPT1 = 41
IDX_IPV1   = 64
IDX_PDC    = 92
IDX_TMOD   = 115
IDX_TAMB   = 116
IDX_ISO    = 117


# ============================================================
# MAPA DE TITULOS — palavras-chave para casar cada campo
# ------------------------------------------------------------
# Para cada campo, uma lista de "candidatos". Cada candidato e
# uma lista de palavras-chave (ja normalizadas: minusculas, sem
# acento) que TODAS precisam aparecer no titulo da coluna para
# considerar que aquela coluna e o campo.
#
# A ordem importa: o 1o candidato que casar vence. Coloque os
# mais especificos primeiro.
#
# >>> AJUSTE AQUI depois de ver o cabecalho real impresso no log.
#     Os termos abaixo cobrem variacoes comuns (PT/EN), mas
#     confirme com os titulos que a Chint realmente devolve.
# ============================================================
MAPA_TITULOS = {
    "data": [
        ["tempo", "atualiz"], ["data", "hora"], ["update", "time"],
        ["hora"], ["data"], ["time"],
    ],
    "tyield_kwh": [
        ["geracao", "total"], ["energia", "total"],
        ["producao", "total"], ["total", "yield"], ["e", "total"],
    ],
    "dyield_kwh": [
        ["geracao", "dia"], ["geracao", "diaria"], ["energia", "dia"],
        ["energia", "diaria"], ["producao", "dia"], ["daily", "yield"],
        ["e", "hoje"], ["e", "dia"],
    ],
    "pac_kw": [
        ["potencia", "ativa"], ["potencia", "saida"], ["potencia", "ca"],
        ["potencia", "ac"], ["pac"], ["active", "power"],
    ],
    "freq_hz": [
        ["frequencia", "rede"], ["frequencia"], ["freq"], ["frequency"],
    ],
    "pdc_kw": [
        ["potencia", "cc"], ["potencia", "dc"], ["potencia", "entrada"],
        ["pdc"], ["dc", "power"], ["input", "power"],
    ],
    "tmod_c": [
        ["temperatura", "modulo"], ["temp", "modulo"], ["temperatura", "painel"],
        ["module", "temp"], ["tmod"],
    ],
    "tamb_c": [
        ["temperatura", "ambiente"], ["temp", "ambiente"],
        ["ambient", "temp"], ["tamb"],
    ],
    "iso_kohm": [
        ["resistencia", "isolamento"], ["impedancia", "isolamento"],
        ["isolamento"], ["insulation", "resist"], ["iso"],
    ],
}


# ============================================================
# FUNCOES AUXILIARES
# ============================================================

def safe_float(val):
    """Converte para float de forma segura. '--', '', None viram 0.0."""
    try:
        if val in (None, "--", ""):
            return 0.0
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def _norm(texto):
    """Normaliza um titulo para comparar: minusculas, sem acento,
    sem caracteres especiais (vira espaco)."""
    if texto is None:
        return ""
    s = str(texto)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return s.strip()


def _casa_candidato(titulo_norm, candidato):
    """True se TODAS as palavras-chave do candidato aparecem no titulo."""
    return all(kw in titulo_norm for kw in candidato)


def truncar_slot_5min(dt):
    """Trunca um datetime para o multiplo de 5 min anterior, segundos = 0.
    Usado para alinhar o carimbo da Chint (ex: 13:45:16) ao slot (13:45:00)."""
    minuto = (dt.minute // 5) * 5
    return dt.replace(minute=minuto, second=0, microsecond=0)


def parse_data_chint(texto):
    """Converte texto de data em datetime EM UTC (naive). None se falhar.

    Aceita os formatos que a Chint usa atualmente:
      'AAAA-MM-DD HH:MM:SS -0300'  (com offset)
      'AAAA-MM-DD HH:MM:SS'         (sem offset, assume BR)
      'AAAA-MM-DD HH:MM'            (sem segundos, assume BR)

    O retorno e SEMPRE em UTC (sem tzinfo), para coerencia com o banco.
    A API depois converte UTC -> BR para exibir."""
    if not texto:
        return None
    s = str(texto).strip()

    # Detecta offset no final do texto: " +HHMM" ou " -HHMM" (com ou sem espaco antes)
    offset_presente = False
    offset_hh = 0
    offset_mm = 0
    m = re.search(r'\s*([+-])(\d{2})(\d{2})\s*$', s)
    if m:
        offset_presente = True
        sinal = -1 if m.group(1) == "-" else 1
        offset_hh = sinal * int(m.group(2))
        offset_mm = sinal * int(m.group(3))
        s = s[:m.start()].strip()

    dt_local = None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt_local = datetime.strptime(s, fmt)
            break
        except (ValueError, TypeError):
            continue
    if dt_local is None:
        return None

    # Se nao veio offset, assume que e BR (-3h) -- a Chint envia BR mesmo
    # quando omite o offset, conforme o header time-zone que mandamos.
    if not offset_presente:
        offset_hh = -3

    # Converte para UTC subtraindo o offset: BR (-3) -> +3h para virar UTC
    return dt_local - timedelta(hours=offset_hh, minutes=offset_mm)


# ============================================================
# RESOLUCAO DE COLUNAS POR TITULO
# ============================================================

def extrair_cabecalho(data):
    """Tenta achar, na resposta da Chint, a lista de TITULOS das colunas.

    A Chint nomeia esse campo de formas diferentes conforme a versao
    da API. Tentamos as chaves conhecidas, e por ultimo procuramos
    qualquer lista de strings com tamanho coerente.

    Retorna a lista de titulos (list[str]) ou None se nao achar.
    """
    d = data.get("data", {}) if isinstance(data, dict) else {}

    # Chaves candidatas (a 1a que existir e for lista nao-vazia vence)
    for chave in ("titleList", "headList", "titles", "head",
                  "columns", "columnList", "header", "headers",
                  "titleNameList", "colTitles"):
        v = d.get(chave)
        if isinstance(v, list) and v and all(isinstance(x, (str, dict)) for x in v):
            # Alguns formatos trazem [{"title": "..."}], outros ["..."]
            if isinstance(v[0], dict):
                titulos = [x.get("title") or x.get("name") or x.get("label") or ""
                           for x in v]
            else:
                titulos = list(v)
            if any(titulos):
                return titulos

    # Ultimo recurso: procurar uma lista de strings com tamanho parecido
    # com o das linhas de dados (mesma largura do dataList).
    rows = d.get("dataList") or []
    largura = len(rows[0]) if rows and isinstance(rows[0], list) else None
    if largura:
        for v in d.values():
            if (isinstance(v, list) and len(v) == largura
                    and all(isinstance(x, str) for x in v)):
                return v

    return None


def construir_colmap(header):
    """A partir da lista de titulos, monta o mapeamento campo -> indice
    para os campos ESCALARES (pac, dyield, etc.).

    Retorna um dict: {campo: indice}. Campos nao encontrados ficam de
    fora (o chamador usa o fallback IDX_* nesses casos).
    """
    if not header:
        return {}

    titulos_norm = [_norm(t) for t in header]
    colmap = {}

    for campo, candidatos in MAPA_TITULOS.items():
        achou = None
        # Tenta candidato a candidato, na ordem (mais especifico primeiro)
        for cand in candidatos:
            for idx, tnorm in enumerate(titulos_norm):
                if idx in colmap.values():
                    pass  # nao bloqueia: um titulo pode servir 1 campo so
                if _casa_candidato(tnorm, cand):
                    achou = idx
                    break
            if achou is not None:
                break
        if achou is not None:
            colmap[campo] = achou

    return colmap


def mapear_mppts_strings(header, topologia):
    """Localiza, pelo titulo, as colunas de tensao/corrente de cada MPPT
    e de corrente de cada string PV.

    Retorna:
      mppt_v_idx[m]  -> indice da coluna de TENSAO do MPPT m (0-based m)
      mppt_i_idx[m]  -> indice da coluna de CORRENTE do MPPT m
      pv_i_idx[s]    -> indice da coluna de CORRENTE da string PV s (0-based s)
    Indices ausentes ficam como None.

    Heuristica: o titulo de um canal MPPT/PV costuma conter o numero do
    canal e uma palavra que diz se e tensao ou corrente. Ex.:
      "Tensao MPPT1", "Corrente MPPT1", "Tensao PV1", "Corrente PV3",
      "Upv1", "Ipv1", "U MPPT 2", "I PV 5", etc.
    """
    num_mppt   = topologia["num_mppt"]
    num_string = topologia["num_string"]

    mppt_v_idx = [None] * num_mppt
    mppt_i_idx = [None] * num_mppt
    pv_i_idx   = [None] * num_string

    if not header:
        return mppt_v_idx, mppt_i_idx, pv_i_idx

    PAL_TENSAO   = ("tensao", "voltage", "volt", "u ")   # 'u ' p/ Upv/Umppt
    PAL_CORRENTE = ("corrente", "current", "amp", "i ")  # 'i ' p/ Ipv/Imppt

    def eh_tensao(t):
        return (any(p in t for p in ("tensao", "voltage", "volt"))
                or re.search(r"\bu\s*(mppt|pv|str)", t) is not None)

    def eh_corrente(t):
        return (any(p in t for p in ("corrente", "current"))
                or re.search(r"\bi\s*(mppt|pv|str)", t) is not None)

    def numero_de(t):
        """Extrai o numero do canal do titulo (o ultimo numero presente)."""
        nums = re.findall(r"\d+", t)
        return int(nums[-1]) if nums else None

    for idx, titulo in enumerate(header):
        t = _norm(titulo)
        if not t:
            continue
        n = numero_de(t)
        if n is None:
            continue

        eh_mppt = "mppt" in t
        eh_pv   = ("pv" in t) or ("string" in t) or ("str" in t and "mppt" not in t)

        if eh_mppt:
            mi = n - 1  # canal 1-based -> indice 0-based
            if 0 <= mi < num_mppt:
                if eh_tensao(t) and mppt_v_idx[mi] is None:
                    mppt_v_idx[mi] = idx
                elif eh_corrente(t) and mppt_i_idx[mi] is None:
                    mppt_i_idx[mi] = idx
        elif eh_pv:
            si = n - 1
            if 0 <= si < num_string and eh_corrente(t) and pv_i_idx[si] is None:
                pv_i_idx[si] = idx

    return mppt_v_idx, mppt_i_idx, pv_i_idx


def buscar_leituras_validas(asset_id):
    """Busca na Chint as ultimas leituras e retorna TODAS as que sao
    multiplas de 5 (ignorando as 'fora-de-grid'), ja convertidas para UTC.

    Retorna (header, validas):
      header  -> lista de titulos das colunas (ou None se a API nao trouxe)
      validas -> lista de tuplas (ts_utc, row), da mais recente p/ a mais antiga.

    Pede limit=15 para ter margem caso varias leituras sejam fora-de-grid
    e precisem ser descartadas (sobra ainda a mais recente valida)."""
    hoje_br = datetime.now(FUSO_BR).strftime("%Y-%m-%d")
    url = (
        f"{BASE}/openApi/v1/deviceData/deviceDataPageList"
        f"?assetId={asset_id}&startDay={hoje_br}&endDay={hoje_br}"
        f"&dataType=&lang=pt-PT&page=1&limit=15"
    )
    req  = Request(url, headers=HEADERS)
    resp = urlopen(req, timeout=20)
    data = json.loads(resp.read())
    if data.get("code") != "0":
        raise ValueError(f"API Chint retornou erro: {data.get('msg')}")
    rows = data.get("data", {}).get("dataList") or []

    header = extrair_cabecalho(data)

    # Indice da coluna de data: por titulo (se houver), senao fallback IDX_DATE
    idx_data = IDX_DATE
    if header:
        cm = construir_colmap(header)
        if "data" in cm:
            idx_data = cm["data"]

    validas = []
    for row in rows:
        if not row:
            continue
        ts = parse_data_chint(row[idx_data]) if idx_data < len(row) else None
        if ts is None:
            continue
        # Ignora fora-de-grid: so minuto multiplo de 5
        if ts.minute % 5 != 0:
            continue
        validas.append((ts, row))
    return header, validas


def extrair_dados(row, topologia, header=None):
    """Converte uma row da API Chint nos campos principais + canais.

    'topologia' descreve o modelo do inversor:
        {"num_mppt": int,
         "num_string": int,
         "strings_por_mppt": {mppt_n: qtd, ...}}

    'header' (opcional) e a lista de titulos das colunas. Se presente,
    cada coluna e localizada pelo TITULO. Se ausente (None), usa o
    fallback dos indices fixos IDX_*.

    Retorna: (status, campos, mppts, strings)
      mppts   -> lista de {mppt, tensao_v, corrente_a, potencia_w}
      strings -> lista de {string_num, mppt, corrente_a, potencia_w}
    """
    num_mppt = topologia["num_mppt"]

    # ---- resolve indices dos campos escalares ----
    colmap = construir_colmap(header) if header else {}

    # indice efetivo: titulo (se achou) senao fallback fixo
    fallback = {
        "pac_kw":     IDX_PAC,
        "dyield_kwh": IDX_DYIELD,
        "tyield_kwh": IDX_TYIELD,
        "freq_hz":    IDX_FREQ,
        "tmod_c":     IDX_TMOD,
        "tamb_c":     IDX_TAMB,
        "iso_kohm":   IDX_ISO,
        "pdc_kw":     IDX_PDC,
    }

    def ler(campo):
        idx = colmap.get(campo, fallback[campo])
        return safe_float(row[idx]) if idx is not None and idx < len(row) else 0.0

    campos = {
        "pac_kw":     ler("pac_kw")    / 1000.0,   # W -> kW
        "dyield_kwh": ler("dyield_kwh") / 1000.0,  # Wh -> kWh
        "tyield_kwh": ler("tyield_kwh"),           # ja em kWh
        "freq_hz":    ler("freq_hz"),
        "tmod_c":     ler("tmod_c"),
        "tamb_c":     ler("tamb_c"),
        "iso_kohm":   ler("iso_kohm"),
        "pdc_kw":     ler("pdc_kw"),
    }

    # ---- resolve indices dos canais MPPT / string ----
    if header:
        mppt_v_idx, mppt_i_idx, pv_i_idx = mapear_mppts_strings(header, topologia)
    else:
        mppt_v_idx = mppt_i_idx = pv_i_idx = None

    # ---- MPPTs: tensao e corrente ----
    mppt_v = []
    mppts  = []
    for m in range(num_mppt):
        if mppt_v_idx is not None:
            v_idx = mppt_v_idx[m]
            i_idx = mppt_i_idx[m]
        else:
            v_idx = IDX_UMPPT1 + m * 2
            i_idx = IDX_IMPPT1 + m * 2
        v = safe_float(row[v_idx]) if (v_idx is not None and v_idx < len(row)) else 0.0
        i = safe_float(row[i_idx]) if (i_idx is not None and i_idx < len(row)) else 0.0
        mppt_v.append(v)
        mppts.append({
            "mppt": m + 1,
            "tensao_v": v, "corrente_a": i,
            "potencia_w": v * i,
        })

    # ---- Strings PV ----
    # Percorre os MPPTs na ordem; para cada um, le quantas strings ele tem
    # (vem da topologia do modelo). A string nao tem tensao propria: usa a
    # do MPPT pai. Quando ha cabecalho, a corrente de cada string vem da
    # coluna localizada por titulo (pv_i_idx). Sem cabecalho, usa o bloco
    # sequencial a partir de IDX_IPV1.
    strings = []
    string_num = 0          # numero global da string (1..num_string)
    idx_corrente = 0        # deslocamento dentro do bloco IDX_IPV1 (fallback)
    for m in range(num_mppt):
        qtd = topologia["strings_por_mppt"].get(m + 1, 0)
        upv = mppt_v[m] if m < len(mppt_v) else 0.0
        for _ in range(qtd):
            if pv_i_idx is not None:
                i_idx = pv_i_idx[string_num] if string_num < len(pv_i_idx) else None
            else:
                i_idx = IDX_IPV1 + idx_corrente
            ipv = safe_float(row[i_idx]) if (i_idx is not None and i_idx < len(row)) else 0.0
            string_num += 1
            strings.append({
                "string_num": string_num,
                "mppt": m + 1,
                "corrente_a": ipv,
                "potencia_w": ipv * upv,
            })
            idx_corrente += 1

    status = "ONLINE" if campos["pac_kw"] > 0 else "OFFLINE"
    return status, campos, mppts, strings


def canais_zerados(topologia):
    """Campos e canais zerados, para status ERRO/SEM_DADOS."""
    campos = {k: 0.0 for k in
              ("pac_kw", "dyield_kwh", "tyield_kwh", "freq_hz",
               "tmod_c", "tamb_c", "iso_kohm", "pdc_kw")}

    mppts = [{"mppt": m + 1, "tensao_v": 0.0,
              "corrente_a": 0.0, "potencia_w": 0.0}
             for m in range(topologia["num_mppt"])]

    strings = []
    string_num = 0
    for m in range(topologia["num_mppt"]):
        qtd = topologia["strings_por_mppt"].get(m + 1, 0)
        for _ in range(qtd):
            string_num += 1
            strings.append({"string_num": string_num, "mppt": m + 1,
                             "corrente_a": 0.0, "potencia_w": 0.0})
    return campos, mppts, strings


# ============================================================
# LEITURA DA TOPOLOGIA (modelo do inversor)
# ============================================================

def carregar_topologias(cur):
    """Le, para cada modelo de inversor, a sua topologia.
    Retorna um dicionario: modelo_id -> {
        "num_mppt": int, "num_string": int,
        "strings_por_mppt": {mppt_n: qtd, ...}
    }
    """
    # dados gerais do modelo
    cur.execute("SELECT id, num_mppt, num_string FROM modelo_inversor")
    topologias = {}
    for modelo_id, num_mppt, num_string in cur.fetchall():
        topologias[modelo_id] = {
            "num_mppt": num_mppt,
            "num_string": num_string,
            "strings_por_mppt": {},
        }

    # strings de cada MPPT de cada modelo
    cur.execute("SELECT modelo_id, mppt, num_string FROM modelo_mppt")
    for modelo_id, mppt, num_string in cur.fetchall():
        if modelo_id in topologias:
            topologias[modelo_id]["strings_por_mppt"][mppt] = num_string

    return topologias


# ============================================================
# GRAVACAO NO BANCO
# ============================================================

def gravar_leitura(cur, inversor_id, ts, status, campos, mppts, strings):
    """Insere (ou atualiza) uma leitura e seus canais no PostgreSQL.
    Usa UPSERT: se ja existe leitura para (inversor, horario), atualiza."""

    # 1) leitura principal — UPSERT pela chave (inversor_id, data_hora)
    cur.execute(
        """
        INSERT INTO leitura
            (inversor_id, data_hora, status, alarme,
             pac_kw, dyield_kwh, tyield_kwh, freq_hz,
             tmod_c, tamb_c, iso_kohm, pdc_kw)
        VALUES (%s, %s, %s, '', %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (inversor_id, data_hora) DO UPDATE SET
            status     = EXCLUDED.status,
            pac_kw     = EXCLUDED.pac_kw,
            dyield_kwh = EXCLUDED.dyield_kwh,
            tyield_kwh = EXCLUDED.tyield_kwh,
            freq_hz    = EXCLUDED.freq_hz,
            tmod_c     = EXCLUDED.tmod_c,
            tamb_c     = EXCLUDED.tamb_c,
            iso_kohm   = EXCLUDED.iso_kohm,
            pdc_kw     = EXCLUDED.pdc_kw
        RETURNING id
        """,
        (inversor_id, ts, status,
         campos["pac_kw"], campos["dyield_kwh"], campos["tyield_kwh"],
         campos["freq_hz"], campos["tmod_c"], campos["tamb_c"],
         campos["iso_kohm"], campos["pdc_kw"]),
    )
    leitura_id = cur.fetchone()[0]

    # 2) canais MPPT — apaga os antigos desta leitura e reinsere
    cur.execute("DELETE FROM leitura_mppt WHERE leitura_id = %s",
                (leitura_id,))
    cur.executemany(
        """
        INSERT INTO leitura_mppt
            (leitura_id, inversor_id, mppt,
             tensao_v, corrente_a, potencia_w)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        [(leitura_id, inversor_id, c["mppt"],
          c["tensao_v"], c["corrente_a"], c["potencia_w"])
         for c in mppts],
    )

    # 3) canais string — apaga os antigos desta leitura e reinsere
    cur.execute("DELETE FROM leitura_string WHERE leitura_id = %s",
                (leitura_id,))
    cur.executemany(
        """
        INSERT INTO leitura_string
            (leitura_id, inversor_id, string_num, mppt,
             corrente_a, potencia_w)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        [(leitura_id, inversor_id, c["string_num"], c["mppt"],
          c["corrente_a"], c["potencia_w"])
         for c in strings],
    )


# ============================================================
# CICLO DE COLETA — roda UMA vez
# ============================================================

def main():
    # 'agora' no fuso do Brasil — o Railway roda em UTC.
    agora_br = datetime.now(FUSO_BR).replace(tzinfo=None)

    print("=" * 60)
    print(f"COLETOR v2 APOLO SOLAR — agora {agora_br:%Y-%m-%d %H:%M:%S} (BR)")
    print(f"  Aguardando {DELAY_SEGUNDOS}s para a Chint propagar o slot...")

    # Espera antes de consultar a Chint, para que o slot atual ja tenha
    # propagado para todos os inversores. O cron dispara no minuto exato
    # (XX:00, XX:05, ...), mas a Chint demora alguns segundos para
    # disponibilizar a leitura mais recente em todos os inversores.
    time.sleep(DELAY_SEGUNDOS)

    # Re-le 'agora' apos o sleep, para o log de pos-espera
    agora_br = datetime.now(FUSO_BR).replace(tzinfo=None)
    print(f"  Iniciando coleta as {agora_br:%H:%M:%S} (BR)")

    conn = psycopg.connect(DATABASE_URL, connect_timeout=15)
    try:
        cur = conn.cursor()

        # Carrega as topologias de todos os modelos cadastrados
        topologias = carregar_topologias(cur)
        if not topologias:
            print("ERRO: nenhum modelo de inversor cadastrado.")
            print("Rode o schema_v2.sql no banco primeiro.")
            sys.exit(1)

        # Busca os inversores desta usina, com o modelo de cada um
        cur.execute(
            """
            SELECT i.id, i.nome, i.asset_id, i.modelo_id
            FROM inversor i
            JOIN usina u ON u.id = i.usina_id
            WHERE u.slug = %s
            ORDER BY i.idx
            """,
            (USINA_SLUG,),
        )
        inversores = cur.fetchall()

        if not inversores:
            print(f"ERRO: nenhum inversor para a usina '{USINA_SLUG}'.")
            print("Rode o schema_v2.sql no banco primeiro.")
            sys.exit(1)

        total_pac = 0.0
        n_online  = 0
        n_pulado  = 0
        n_erro    = 0
        cabecalho_logado = False

        for inv_id, nome, asset_id, modelo_id in inversores:
            topo = topologias.get(modelo_id)
            if topo is None:
                print(f"  [{nome}] ERRO: modelo {modelo_id} sem topologia.")
                n_erro += 1
                continue

            try:
                header, validas = buscar_leituras_validas(asset_id)
            except Exception as e:
                print(f"  [{nome}] ERRO de API: {e}")
                n_erro += 1
                continue

            # Diagnostico: imprime o cabecalho real UMA vez por execucao.
            if LOG_CABECALHO and not cabecalho_logado:
                cabecalho_logado = True
                if header:
                    print("  --- CABECALHO DA CHINT (titulos das colunas) ---")
                    for j, titulo in enumerate(header):
                        print(f"      [{j:3d}] {titulo}")
                    cm = construir_colmap(header)
                    print("  --- MAPEAMENTO RESOLVIDO (campo -> indice) ---")
                    for campo in ("data", "pac_kw", "dyield_kwh", "tyield_kwh",
                                  "freq_hz", "pdc_kw", "tmod_c", "tamb_c",
                                  "iso_kohm"):
                        idx = cm.get(campo)
                        marca = "" if idx is not None else "  (NAO ACHOU -> fallback fixo)"
                        ttl = header[idx] if idx is not None and idx < len(header) else "-"
                        print(f"      {campo:12s} -> idx {str(idx):>4s}  [{ttl}]{marca}")
                    vmap, imap, pvmap = mapear_mppts_strings(header, topo)
                    print(f"      MPPT tensao  idx: {vmap}")
                    print(f"      MPPT corrente idx: {imap}")
                    print(f"      String corrente idx: {pvmap}")
                    print("  ------------------------------------------------")
                else:
                    print("  AVISO: resposta da Chint SEM cabecalho de titulos; "
                          "usando indices fixos (fallback IDX_*).")

            if not validas:
                print(f"  [{nome}] sem leitura valida (Chint sem multiplo de 5)")
                n_pulado += 1
                continue

            # Opcao A: pega a leitura mais recente multipla de 5
            # (validas[0] e a mais recente; ja filtradas pelo fora-de-grid)
            ts_chint, row = validas[0]
            ts_grava = truncar_slot_5min(ts_chint)

            status, campos, mppts, strings = extrair_dados(row, topo, header)
            gravar_leitura(cur, inv_id, ts_grava, status,
                           campos, mppts, strings)
            total_pac += campos["pac_kw"]
            if status == "ONLINE":
                n_online += 1

            ts_chint_br = ts_chint - timedelta(hours=3)
            ts_grava_br = ts_grava - timedelta(hours=3)
            print(f"  [{nome}] {status} | Chint {ts_chint_br:%H:%M:%S} BR -> "
                  f"slot {ts_grava_br:%H:%M} BR | Pac: {campos['pac_kw']:.2f} kW")

        # Confirma TODAS as gravacoes do ciclo de uma vez
        conn.commit()
        print("-" * 60)
        print(f"  Potencia total: {total_pac:.2f} kW | "
              f"Online: {n_online}/{len(inversores)} | "
              f"Sem dado: {n_pulado} | Erros: {n_erro}")
        print("  Ciclo concluido.")

    except Exception as e:
        conn.rollback()
        print(f"ERRO no ciclo — nada foi gravado: {e}")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
