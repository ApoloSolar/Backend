# -*- coding: utf-8 -*-
"""
============================================================
  COLETOR v3 — APOLO SOLAR  (Railway / PostgreSQL)
============================================================
Le os inversores de uma usina na API da Chint e grava as
leituras no banco PostgreSQL (schema v2).

O QUE MUDOU DA v2 PARA A v3 (e POR QUE):
  - A v2 resolvia cada coluna pelo TITULO do cabecalho. Esse
    casamento usava substring (kw in titulo), com palavras-
    chave curtissimas — inclusive a letra solta "e" para
    'etotal'/'etoday'. "e" e substring de quase qualquer
    titulo (esta dentro de "power", "current", etc.), entao
    os campos casavam com a coluna ERRADA. Pior: o primeiro
    indice que casava ficava TRAVADO (set 'usados'), e a
    coluna certa nao podia mais ser reivindicada. Resultado:
    dados gravados trocados, mesmo com a Chint mantendo as
    colunas fixas. O problema NUNCA foi a Chint reordenar —
    foi o matcher por titulo escolhendo errado em silencio.
  - A v3 volta ao esquema da v1: cada dado e lido pelo seu
    INDICE FIXO na resposta da Chint (IDX_*). Sem casamento
    por titulo, sem deteccao por valor. Determinístico.
  - Opcional: DIAGNOSTICO_TITULOS imprime, uma vez, o titulo
    que esta em cada indice fixo. Serve so para conferir/
    ajustar os IDX_* caso a Chint mude o layout no futuro.
    NAO altera a leitura — e puramente informativo.

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
# 'time-zone'; o Railway roda em UTC, entao convertemos para gravar em UTC).
FUSO_BR = timezone(timedelta(hours=-3))

# Diagnostico opcional: se True, imprime UMA vez o titulo de cada indice fixo
# (quando a resposta da Chint traz cabecalho). Util para conferir os IDX_*.
# NAO interfere na leitura — os dados continuam vindo pelo indice fixo.
DIAGNOSTICO_TITULOS = False


# ============================================================
# INDICES FIXOS DOS CAMPOS NA RESPOSTA DA API CHINT
# ------------------------------------------------------------
# Cada dado e lido SEMPRE pela mesma posicao na 'row' que a
# Chint devolve em data.dataList. Se algum dia a Chint mudar a
# ordem das colunas, ajuste os numeros abaixo (ligue o
# DIAGNOSTICO_TITULOS para ver qual titulo esta em cada indice).
# ============================================================
IDX_DATE   = 0           # carimbo da Chint, ex: "2026-05-27 13:45:16"
IDX_TYIELD = 4           # energia total (kWh)
IDX_DYIELD = 5           # energia do dia (Wh -> /1000 = kWh)
IDX_PAC    = 10          # potencia ativa (W -> /1000 = kW)
IDX_FREQ   = 18          # frequencia (Hz)
IDX_UMPPT1 = 40          # tensao do MPPT 1; demais: +2 por MPPT
IDX_IMPPT1 = 41          # corrente do MPPT 1; demais: +2 por MPPT
IDX_IPV1   = 64          # corrente da string PV 1; demais: +1 por string
IDX_PDC    = 92          # potencia CC (kW)
IDX_TMOD   = 115         # temperatura do modulo (C)
IDX_TAMB   = 116         # temperatura ambiente (C)
IDX_ISO    = 117         # resistencia de isolamento (kOhm)


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

    # Detecta offset no final: " +HHMM" ou " -HHMM" (com ou sem espaco antes)
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

    # Sem offset: assume BR (-3h). A Chint envia BR mesmo quando omite o offset,
    # conforme o header time-zone que mandamos.
    if not offset_presente:
        offset_hh = -3

    # Converte para UTC subtraindo o offset (BR -3 vira UTC somando 3h).
    return dt_local - timedelta(hours=offset_hh, minutes=offset_mm)


def _diagnosticar_indices(data):
    """Imprime, para conferencia, o titulo que esta em cada indice fixo.
    So roda quando DIAGNOSTICO_TITULOS = True e a Chint traz cabecalho.
    Puramente informativo: NAO e usado para ler os dados."""
    d = data.get("data", {}) if isinstance(data, dict) else {}
    header = None
    for chave in ("titleList", "headList", "titles", "head",
                  "columns", "columnList", "header", "headers",
                  "titleNameList", "colTitles"):
        v = d.get(chave)
        if isinstance(v, list) and v:
            if isinstance(v[0], dict):
                header = [x.get("title") or x.get("name") or x.get("label") or ""
                          for x in v]
            elif all(isinstance(x, str) for x in v):
                header = list(v)
            if header:
                break
    if not header:
        print("  [diagnostico] resposta sem cabecalho — nada a conferir.")
        return

    rotulos = {
        IDX_DATE: "DATE", IDX_TYIELD: "TYIELD", IDX_DYIELD: "DYIELD",
        IDX_PAC: "PAC", IDX_FREQ: "FREQ", IDX_UMPPT1: "UMPPT1",
        IDX_IMPPT1: "IMPPT1", IDX_IPV1: "IPV1", IDX_PDC: "PDC",
        IDX_TMOD: "TMOD", IDX_TAMB: "TAMB", IDX_ISO: "ISO",
    }
    print("  [diagnostico] titulo em cada indice fixo:")
    for idx in sorted(rotulos):
        titulo = header[idx] if idx < len(header) else "<fora do cabecalho>"
        print(f"    IDX_{rotulos[idx]:<7} = {idx:>3}  ->  {titulo}")


# ============================================================
# BUSCA NA CHINT
# ============================================================

def buscar_leituras_validas(asset_id):
    """Busca na Chint as ultimas leituras e retorna TODAS as que sao
    multiplas de 5 (ignorando as 'fora-de-grid'), ja convertidas para UTC.

    Retorna uma lista de tuplas (ts_utc, row), ordenada da mais recente
    para a mais antiga (a Chint ja devolve assim).

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

    if DIAGNOSTICO_TITULOS:
        _diagnosticar_indices(data)

    validas = []
    for row in rows:
        if not row:
            continue
        ts = parse_data_chint(row[IDX_DATE]) if IDX_DATE < len(row) else None
        if ts is None:
            continue
        # Ignora fora-de-grid: so minuto multiplo de 5
        if ts.minute % 5 != 0:
            continue
        validas.append((ts, row))
    return validas


# ============================================================
# EXTRACAO DOS DADOS (por INDICE FIXO)
# ============================================================

def extrair_dados(row, topologia):
    """Converte uma row da API Chint nos campos principais + canais,
    lendo cada valor pelo seu INDICE FIXO.

    'topologia' descreve o modelo do inversor:
        {"num_mppt": int,
         "num_string": int,
         "strings_por_mppt": {mppt_n: qtd, ...}}

    Retorna: (status, campos, mppts, strings)
      mppts   -> lista de {mppt, tensao_v, corrente_a, potencia_w}
      strings -> lista de {string_num, mppt, corrente_a, potencia_w}
    """
    num_mppt = topologia["num_mppt"]

    def ler(idx):
        """Le um indice fixo com seguranca (0.0 se faltar na row)."""
        return safe_float(row[idx]) if idx < len(row) else 0.0

    campos = {
        "pac_kw":     ler(IDX_PAC)    / 1000.0,   # W -> kW
        "dyield_kwh": ler(IDX_DYIELD) / 1000.0,   # Wh -> kWh
        "tyield_kwh": ler(IDX_TYIELD),            # ja em kWh
        "freq_hz":    ler(IDX_FREQ),
        "tmod_c":     ler(IDX_TMOD),
        "tamb_c":     ler(IDX_TAMB),
        "iso_kohm":   ler(IDX_ISO),
        "pdc_kw":     ler(IDX_PDC),
    }

    # ---- MPPTs: tensao e corrente ----
    # Layout fixo: U,I,U,I,... a partir de IDX_UMPPT1/IDX_IMPPT1, +2 por MPPT.
    mppt_v = []
    mppts  = []
    for m in range(num_mppt):
        v_idx = IDX_UMPPT1 + m * 2
        i_idx = IDX_IMPPT1 + m * 2
        v = ler(v_idx)
        i = ler(i_idx)
        mppt_v.append(v)
        mppts.append({
            "mppt": m + 1,
            "tensao_v": v, "corrente_a": i,
            "potencia_w": v * i,
        })

    # ---- Strings PV ----
    # Percorre os MPPTs na ordem; para cada um, le quantas strings ele tem
    # (vem da topologia do modelo). A string nao tem tensao propria: usa a do
    # MPPT pai. A corrente avanca sequencialmente a partir de IDX_IPV1.
    strings = []
    string_num = 0          # numero global da string (1..num_string)
    idx_corrente = 0        # deslocamento dentro do bloco IDX_IPV1
    for m in range(num_mppt):
        qtd = topologia["strings_por_mppt"].get(m + 1, 0)
        upv = mppt_v[m] if m < len(mppt_v) else 0.0
        for _ in range(qtd):
            ipv = ler(IDX_IPV1 + idx_corrente)
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
    Retorna: modelo_id -> {
        "num_mppt": int, "num_string": int,
        "strings_por_mppt": {mppt_n: qtd, ...}
    }"""
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
# CICLO DE COLETA — roda UMA vez
# ============================================================

def main():
    agora_br = datetime.now(FUSO_BR).replace(tzinfo=None)

    print("=" * 60)
    print(f"COLETOR v3 APOLO SOLAR — agora {agora_br:%Y-%m-%d %H:%M:%S} (BR)")
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

        for inv_id, nome, asset_id, modelo_id in inversores:
            topo = topologias.get(modelo_id)
            if topo is None:
                print(f"  [{nome}] ERRO: modelo {modelo_id} sem topologia.")
                n_erro += 1
                continue

            try:
                validas = buscar_leituras_validas(asset_id)
            except Exception as e:
                print(f"  [{nome}] ERRO de API: {e}")
                n_erro += 1
                continue

            if not validas:
                print(f"  [{nome}] sem leitura valida (Chint sem multiplo de 5)")
                n_pulado += 1
                continue

            ts_chint, row = validas[0]
            ts_grava = truncar_slot_5min(ts_chint)

            status, campos, mppts, strings = extrair_dados(row, topo)
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
