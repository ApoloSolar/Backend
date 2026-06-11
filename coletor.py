# -*- coding: utf-8 -*-
"""
============================================================
  COLETOR v2 — APOLO SOLAR  (Railway / PostgreSQL)
============================================================
Le os inversores de uma usina na API da Chint e grava as
leituras no banco PostgreSQL (schema v2).

CORRECAO (esta versao) — coleta por TITULO, nao por posicao:
  - Cada coluna da resposta da Chint e localizada pelo seu
    TITULO no cabecalho que a API devolve. Se a Chint
    reordenar/inserir colunas, o coletor continua pegando o
    dado certo.
  - A coluna do carimbo de hora e detectada PELO VALOR: e a
    coluna cujos valores realmente parseiam como data e que
    variam entre as leituras (o carimbo real muda a cada 5
    min; colunas-isca de hora fixa sao descartadas).
  - Os IDX_* antigos viraram apenas FALLBACK: so sao usados
    quando a resposta nao trouxer cabecalho / titulo do campo.

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
DELAY_SEGUNDOS = 20

# Fuso do Brasil (a Chint publica no fuso de Sao Paulo; o Railway roda em UTC)
FUSO_BR = timezone(timedelta(hours=-3))

# Imprime o cabecalho (titulos + valor de amostra) uma vez por execucao,
# para diagnostico/conferencia. Deixe True ate confirmar o mapeamento.
LOG_CABECALHO = True


# ============================================================
# FALLBACK — indices fixos antigos da resposta da API Chint
# ------------------------------------------------------------
# SO usados quando a resposta NAO traz cabecalho ou o titulo de
# um campo nao casa. Com cabecalho, a resolucao por TITULO vence.
# ============================================================
IDX_DATE   = 0
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
# acento, CamelCase quebrado) que TODAS precisam aparecer no
# titulo da coluna. A ordem importa: o 1o candidato que casar
# vence — coloque os mais especificos primeiro.
#
# Observado no log da Chint (estilo compacto com unidade):
#   Pac(W)  Freq(Hz)  Pdc(kW)  Tamb(°C)  ISO(kΩ)  PVInputMode  TimeSet
# Geracao diaria/total e Tmod ainda nao confirmados nominalmente;
# os candidatos abaixo cobrem o padrao usual (Eday/Etoday/Etotal,
# Tmod). Se algum cair em fallback, o log mostra o titulo real.
# ============================================================
MAPA_TITULOS = {
    # 'data' aqui e so reforco; a deteccao principal e por VALOR.
    "data": [
        ["update", "time"], ["collect", "time"], ["data", "hora"],
    ],
    "tyield_kwh": [
        ["etotal"], ["e", "total"], ["total", "yield"],
        ["energia", "total"], ["geracao", "total"], ["lifetime"],
    ],
    "dyield_kwh": [
        ["etoday"], ["eday"], ["e", "today"], ["e", "day"],
        ["daily", "yield"], ["geracao", "diaria"], ["geracao", "dia"],
        ["energia", "dia"], ["today"],
    ],
    "pac_kw": [
        ["pac"], ["potencia", "ativa"], ["potencia", "saida"],
        ["active", "power"],
    ],
    "freq_hz": [
        ["freq"], ["frequencia"], ["frequency"],
    ],
    "pdc_kw": [
        ["pdc"], ["potencia", "cc"], ["potencia", "dc"], ["dc", "power"],
    ],
    "tmod_c": [
        ["tmod"], ["temp", "mod"], ["mod", "temp"],
        ["temperatura", "modulo"], ["module", "temp"],
    ],
    "tamb_c": [
        ["tamb"], ["temp", "amb"], ["temperatura", "ambiente"],
        ["ambient", "temp"],
    ],
    "iso_kohm": [
        ["iso"], ["resistencia", "isolamento"],
        ["impedancia", "isolamento"], ["insulation"],
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
    """Normaliza um titulo para comparar: sem acento, CamelCase quebrado,
    minusculas, sem caracteres especiais.

    A quebra de CamelCase evita colisoes: sem ela, 'PVInputMode' vira
    'pvinputmode' e a palavra-chave 'tmod' casaria dentro de 'inpuTMODe'.
    Com ela, vira 'pv input mode' e nao casa mais."""
    if texto is None:
        return ""
    s = str(texto)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    # quebra CamelCase: minuscula/digito seguido de maiuscula, e
    # maiuscula seguida de Maiuscula+minuscula
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", s)
    s = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", " ", s)
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return s.strip()


def _casa_candidato(titulo_norm, candidato):
    """True se TODAS as palavras-chave do candidato aparecem no titulo."""
    return all(kw in titulo_norm for kw in candidato)


def truncar_slot_5min(dt):
    """Trunca um datetime para o multiplo de 5 min anterior, segundos = 0."""
    minuto = (dt.minute // 5) * 5
    return dt.replace(minute=minuto, second=0, microsecond=0)


def parse_data_chint(texto):
    """Converte texto de data em datetime EM UTC (naive). None se falhar.

    Aceita:
      'AAAA-MM-DD HH:MM:SS -0300'  (com offset)
      'AAAA-MM-DD HH:MM:SS'         (sem offset, assume BR)
      'AAAA-MM-DD HH:MM'            (sem segundos, assume BR)
    Retorno SEMPRE em UTC (sem tzinfo)."""
    if not texto:
        return None
    s = str(texto).strip()

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

    if not offset_presente:
        offset_hh = -3

    return dt_local - timedelta(hours=offset_hh, minutes=offset_mm)


# ============================================================
# RESOLUCAO DE COLUNAS POR TITULO
# ============================================================

def extrair_cabecalho(data):
    """Tenta achar, na resposta da Chint, a lista de TITULOS das colunas.
    Retorna list[str] ou None se nao achar."""
    d = data.get("data", {}) if isinstance(data, dict) else {}

    for chave in ("titleList", "headList", "titles", "head",
                  "columns", "columnList", "header", "headers",
                  "titleNameList", "colTitles"):
        v = d.get(chave)
        if isinstance(v, list) and v and all(isinstance(x, (str, dict)) for x in v):
            if isinstance(v[0], dict):
                titulos = [x.get("title") or x.get("name") or x.get("label") or ""
                           for x in v]
            else:
                titulos = list(v)
            if any(titulos):
                return titulos

    rows = d.get("dataList") or []
    largura = len(rows[0]) if rows and isinstance(rows[0], list) else None
    if largura:
        for v in d.values():
            if (isinstance(v, list) and len(v) == largura
                    and all(isinstance(x, str) for x in v)):
                return v
    return None


def detectar_coluna_data(rows, max_amostra=8):
    """Acha, PELO VALOR, qual coluna contem o carimbo de hora da leitura.

    Estrategia: para cada coluna, verifica se em todas as linhas da amostra
    o valor parseia como data. Entre as colunas validas, prefere aquela cujos
    valores VARIAM entre as linhas (o carimbo real muda de leitura para
    leitura; uma coluna-isca de hora fixa nao varia). Se nenhuma variar
    (ex.: so 1 leitura), usa a mais a esquerda.

    Retorna o indice da coluna, ou None se nenhuma parsear como data."""
    if not rows:
        return None
    amostra = [r for r in rows[:max_amostra] if r]
    if not amostra:
        return None

    largura = max(len(r) for r in amostra)
    candidatos = []  # (indice, varia_bool)
    for c in range(largura):
        valores = []
        ok = True
        for r in amostra:
            if c >= len(r) or parse_data_chint(r[c]) is None:
                ok = False
                break
            valores.append(r[c])
        if ok:
            candidatos.append((c, len(set(valores)) > 1))

    if not candidatos:
        return None
    variaveis = [c for c, varia in candidatos if varia]
    if variaveis:
        return min(variaveis)
    return candidatos[0][0]


def construir_colmap(header):
    """A partir dos titulos, monta {campo: indice} para os campos ESCALARES.
    Um indice ja usado por um campo nao e reaproveitado por outro
    (evita dois campos casarem na mesma coluna). Campos nao achados
    ficam de fora (o chamador usa o fallback IDX_*)."""
    if not header:
        return {}

    titulos_norm = [_norm(t) for t in header]
    colmap = {}
    usados = set()

    for campo, candidatos in MAPA_TITULOS.items():
        achou = None
        for cand in candidatos:
            for idx, tnorm in enumerate(titulos_norm):
                if idx in usados:
                    continue
                if _casa_candidato(tnorm, cand):
                    achou = idx
                    break
            if achou is not None:
                break
        if achou is not None:
            colmap[campo] = achou
            usados.add(achou)
    return colmap


def mapear_mppts_strings(header, topologia):
    """Localiza, pelo titulo, as colunas de tensao/corrente de cada MPPT e
    de corrente de cada string PV.

    Retorna (mppt_v_idx, mppt_i_idx, pv_i_idx) — listas de indices (0-based
    por canal), com None onde nao achou.

    Heuristica: o titulo de um canal contem o numero do canal e uma palavra
    que diz se e tensao ou corrente. Ex.: 'Umppt1', 'Impp1', 'Upv1', 'Ipv1',
    'Tensao MPPT1', 'Corrente PV3', etc."""
    num_mppt   = topologia["num_mppt"]
    num_string = topologia["num_string"]

    mppt_v_idx = [None] * num_mppt
    mppt_i_idx = [None] * num_mppt
    pv_i_idx   = [None] * num_string

    if not header:
        return mppt_v_idx, mppt_i_idx, pv_i_idx

    def eh_tensao(t):
        return (any(p in t for p in ("tensao", "voltage", "volt"))
                or re.search(r"\bu\s*(mppt|pv|str)", t) is not None)

    def eh_corrente(t):
        return (any(p in t for p in ("corrente", "current"))
                or re.search(r"\bi\s*(mppt|pv|str)", t) is not None)

    def numero_de(t):
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
            mi = n - 1
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
    """Busca na Chint as ultimas leituras multiplas de 5 (ignora fora-de-grid),
    ja convertidas para UTC.

    Retorna (header, idx_data, validas):
      header   -> lista de titulos (ou None)
      idx_data -> indice da coluna do carimbo de hora (detectado por valor)
      validas  -> lista de (ts_utc, row), da mais recente p/ a mais antiga."""
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

    # Carimbo de hora: detecta por VALOR (robusto a reordenacao e a colunas-isca
    # como 'TimeSet'). Fallbacks: titulo, depois IDX_DATE.
    idx_data = detectar_coluna_data(rows)
    if idx_data is None and header:
        idx_data = construir_colmap(header).get("data")
    if idx_data is None:
        idx_data = IDX_DATE

    validas = []
    for row in rows:
        if not row:
            continue
        ts = parse_data_chint(row[idx_data]) if idx_data < len(row) else None
        if ts is None:
            continue
        if ts.minute % 5 != 0:   # ignora fora-de-grid
            continue
        validas.append((ts, row))
    return header, idx_data, validas


def extrair_dados(row, topologia, header=None):
    """Converte uma row da API Chint nos campos principais + canais.

    'header' (opcional): titulos das colunas. Se presente, cada coluna e
    localizada pelo TITULO; senao, usa o fallback dos indices fixos IDX_*.

    Retorna (status, campos, mppts, strings)."""
    num_mppt = topologia["num_mppt"]

    colmap = construir_colmap(header) if header else {}

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

    if header:
        mppt_v_idx, mppt_i_idx, pv_i_idx = mapear_mppts_strings(header, topologia)
    else:
        mppt_v_idx = mppt_i_idx = pv_i_idx = None

    # ---- MPPTs ----
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
    strings = []
    string_num = 0
    idx_corrente = 0
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
    """Le, para cada modelo de inversor, a sua topologia."""
    cur.execute("SELECT id, num_mppt, num_string FROM modelo_inversor")
    topologias = {}
    for modelo_id, num_mppt, num_string in cur.fetchall():
        topologias[modelo_id] = {
            "num_mppt": num_mppt,
            "num_string": num_string,
            "strings_por_mppt": {},
        }

    cur.execute("SELECT modelo_id, mppt, num_string FROM modelo_mppt")
    for modelo_id, mppt, num_string in cur.fetchall():
        if modelo_id in topologias:
            topologias[modelo_id]["strings_por_mppt"][mppt] = num_string

    return topologias


# ============================================================
# GRAVACAO NO BANCO
# ============================================================

def gravar_leitura(cur, inversor_id, ts, status, campos, mppts, strings):
    """Insere (ou atualiza) uma leitura e seus canais. UPSERT por
    (inversor_id, data_hora)."""
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

    cur.execute("DELETE FROM leitura_mppt WHERE leitura_id = %s", (leitura_id,))
    cur.executemany(
        """
        INSERT INTO leitura_mppt
            (leitura_id, inversor_id, mppt, tensao_v, corrente_a, potencia_w)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        [(leitura_id, inversor_id, c["mppt"],
          c["tensao_v"], c["corrente_a"], c["potencia_w"])
         for c in mppts],
    )

    cur.execute("DELETE FROM leitura_string WHERE leitura_id = %s", (leitura_id,))
    cur.executemany(
        """
        INSERT INTO leitura_string
            (leitura_id, inversor_id, string_num, mppt, corrente_a, potencia_w)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        [(leitura_id, inversor_id, c["string_num"], c["mppt"],
          c["corrente_a"], c["potencia_w"])
         for c in strings],
    )


# ============================================================
# DIAGNOSTICO — imprime cabecalho, valores de amostra e mapeamento
# ============================================================

def logar_diagnostico(header, idx_data, sample_row, topo):
    """Imprime, uma vez por execucao, os titulos da Chint com um valor de
    amostra ao lado, o carimbo de hora detectado e o mapeamento resolvido.
    Util para confirmar/ajustar o MAPA_TITULOS."""
    if not header:
        print("  AVISO: resposta da Chint SEM cabecalho de titulos; "
              "usando indices fixos (fallback IDX_*).")
        print(f"  Carimbo de hora detectado por valor -> indice {idx_data}")
        return

    def amostra(j):
        if sample_row and j < len(sample_row):
            return str(sample_row[j])
        return "-"

    print("  --- CABECALHO DA CHINT (indice | titulo = valor) ---")
    for j, titulo in enumerate(header):
        print(f"      [{j:3d}] {titulo} = {amostra(j)}")

    print(f"  --- CARIMBO DE HORA detectado por valor -> indice {idx_data} "
          f"({header[idx_data] if idx_data < len(header) else '?'}) ---")

    cm = construir_colmap(header)
    print("  --- MAPEAMENTO RESOLVIDO (campo -> indice) ---")
    for campo in ("pac_kw", "dyield_kwh", "tyield_kwh", "freq_hz",
                  "pdc_kw", "tmod_c", "tamb_c", "iso_kohm"):
        idx = cm.get(campo)
        if idx is not None:
            ttl = header[idx] if idx < len(header) else "?"
            print(f"      {campo:12s} -> idx {idx:>4d}  [{ttl}] = {amostra(idx)}")
        else:
            fb = {"pac_kw": IDX_PAC, "dyield_kwh": IDX_DYIELD,
                  "tyield_kwh": IDX_TYIELD, "freq_hz": IDX_FREQ,
                  "pdc_kw": IDX_PDC, "tmod_c": IDX_TMOD,
                  "tamb_c": IDX_TAMB, "iso_kohm": IDX_ISO}[campo]
            ttl = header[fb] if fb < len(header) else "?"
            print(f"      {campo:12s} -> idx None  (NAO ACHOU; fallback {fb} "
                  f"[{ttl}] = {amostra(fb)})")

    vmap, imap, pvmap = mapear_mppts_strings(header, topo)
    print(f"      MPPT tensao   idx: {vmap}")
    print(f"      MPPT corrente idx: {imap}")
    print(f"      String corr.  idx: {pvmap}")
    print("  ----------------------------------------------------")


# ============================================================
# CICLO DE COLETA — roda UMA vez
# ============================================================

def main():
    agora_br = datetime.now(FUSO_BR).replace(tzinfo=None)

    print("=" * 60)
    print(f"COLETOR v2 APOLO SOLAR — agora {agora_br:%Y-%m-%d %H:%M:%S} (BR)")
    print(f"  Aguardando {DELAY_SEGUNDOS}s para a Chint propagar o slot...")
    time.sleep(DELAY_SEGUNDOS)

    agora_br = datetime.now(FUSO_BR).replace(tzinfo=None)
    print(f"  Iniciando coleta as {agora_br:%H:%M:%S} (BR)")

    conn = psycopg.connect(DATABASE_URL, connect_timeout=15)
    try:
        cur = conn.cursor()

        topologias = carregar_topologias(cur)
        if not topologias:
            print("ERRO: nenhum modelo de inversor cadastrado.")
            print("Rode o schema_v2.sql no banco primeiro.")
            sys.exit(1)

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
        diagnostico_feito = False

        for inv_id, nome, asset_id, modelo_id in inversores:
            topo = topologias.get(modelo_id)
            if topo is None:
                print(f"  [{nome}] ERRO: modelo {modelo_id} sem topologia.")
                n_erro += 1
                continue

            try:
                header, idx_data, validas = buscar_leituras_validas(asset_id)
            except Exception as e:
                print(f"  [{nome}] ERRO de API: {e}")
                n_erro += 1
                continue

            if LOG_CABECALHO and not diagnostico_feito:
                diagnostico_feito = True
                sample = validas[0][1] if validas else None
                logar_diagnostico(header, idx_data, sample, topo)

            if not validas:
                print(f"  [{nome}] sem leitura valida (Chint sem multiplo de 5)")
                n_pulado += 1
                continue

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
