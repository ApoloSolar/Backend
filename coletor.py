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
import sys

import psycopg


# ============================================================
# CONFIGURACAO — lida do ambiente (Railway)
# ============================================================

DATABASE_URL = os.environ.get("DATABASE_URL")
TOKEN        = os.environ.get("CHINT_TOKEN")
USER_ID      = os.environ.get("CHINT_USER_ID")

if not DATABASE_URL or not TOKEN or not USER_ID:
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

# Fuso do Brasil (a Chint publica no fuso de Sao Paulo conforme o header
# 'time-zone' que enviamos; o Railway roda em UTC, entao precisamos converter)
FUSO_BR = timezone(timedelta(hours=-3))

# ---- indices dos campos na resposta da API Chint ----
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


def slot_alvo(agora_br):
    """Dado o 'agora' no fuso BR, retorna o slot multiplo de 5 anterior.
    Ex.: 13:47:23 -> 13:45:00. Segundos e microssegundos zerados."""
    minuto = (agora_br.minute // 5) * 5
    return agora_br.replace(minute=minuto, second=0, microsecond=0)


def truncar_slot_5min(dt):
    """Trunca um datetime para o multiplo de 5 min anterior, segundos = 0.
    Usado para alinhar o carimbo da Chint (ex: 13:45:16) ao slot (13:45:00)."""
    minuto = (dt.minute // 5) * 5
    return dt.replace(minute=minuto, second=0, microsecond=0)


def parse_data_chint(texto):
    """Converte 'AAAA-MM-DD HH:MM:SS' em datetime. None se falhar."""
    if not texto:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(texto, fmt)
        except (ValueError, TypeError):
            continue
    return None


def buscar_inversor(asset_id, alvo_br):
    """Busca na API da Chint a leitura cujo carimbo, truncado para 5 min,
    coincide com 'alvo_br' (slot que queremos preencher).

    Retorna (row, ts_chint) ou (None, None) se nao houver leitura para o
    slot. 'ts_chint' e o datetime original devolvido pela Chint (segundos
    incluidos); o slot truncado e calculado novamente em main().

    Estrategia: pede as ultimas 10 leituras do dia (limit=10) e varre.
    A Chint pode publicar a leitura de 13:45 com alguns segundos de atraso,
    entao se o cron rodou as 13:47 e ela ainda nao publicou, a leitura
    nao estara na lista e devolvemos None — o proximo ciclo pega."""
    dia = alvo_br.strftime("%Y-%m-%d")
    url = (
        f"{BASE}/openApi/v1/deviceData/deviceDataPageList"
        f"?assetId={asset_id}&startDay={dia}&endDay={dia}"
        f"&dataType=&lang=pt-PT&page=1&limit=10"
    )
    req  = Request(url, headers=HEADERS)
    resp = urlopen(req, timeout=20)
    data = json.loads(resp.read())
    if data.get("code") != "0":
        raise ValueError(f"API Chint retornou erro: {data.get('msg')}")
    rows = data.get("data", {}).get("dataList") or []

    for row in rows:
        if not row:
            continue
        ts = parse_data_chint(row[IDX_DATE]) if IDX_DATE < len(row) else None
        if ts is None:
            continue

        # A Chint as vezes publica leituras "fora-de-grid" (ex: 11:07:24).
        # So aceitamos leituras cujo MINUTO seja multiplo exato de 5; as
        # fora-de-grid sao ignoradas (o usuario confirmou que sao raras).
        if ts.minute % 5 != 0:
            continue

        # Compara o slot da leitura (minuto+hora+dia) com o alvo.
        slot_leitura = ts.replace(second=0, microsecond=0)
        if slot_leitura == alvo_br.replace(tzinfo=None):
            return row, ts

    return None, None


def extrair_dados(row, topologia):
    """Converte uma row da API Chint nos campos principais + canais.

    'topologia' descreve o modelo do inversor:
        {"num_mppt": int,
         "num_string": int,
         "strings_por_mppt": {mppt_n: qtd, ...}}

    Retorna: (status, campos, mppts, strings)
      mppts   -> lista de {mppt, tensao_v, corrente_a, potencia_w}
      strings -> lista de {string_num, mppt, corrente_a, potencia_w}
    """
    num_mppt = topologia["num_mppt"]

    campos = {
        "pac_kw":     safe_float(row[IDX_PAC])    / 1000.0,   # W -> kW
        "dyield_kwh": safe_float(row[IDX_DYIELD]) / 1000.0,   # Wh -> kWh
        "tyield_kwh": safe_float(row[IDX_TYIELD]),            # ja em kWh
        "freq_hz":    safe_float(row[IDX_FREQ]),
        "tmod_c":     safe_float(row[IDX_TMOD]),
        "tamb_c":     safe_float(row[IDX_TAMB]),
        "iso_kohm":   safe_float(row[IDX_ISO]),
        "pdc_kw":     safe_float(row[IDX_PDC]),
    }

    # ---- MPPTs: tensao e corrente ----
    mppt_v = []
    mppts  = []
    for m in range(num_mppt):
        v_idx = IDX_UMPPT1 + m * 2
        i_idx = IDX_IMPPT1 + m * 2
        v = safe_float(row[v_idx]) if v_idx < len(row) else 0.0
        i = safe_float(row[i_idx]) if i_idx < len(row) else 0.0
        mppt_v.append(v)
        mppts.append({
            "mppt": m + 1,
            "tensao_v": v, "corrente_a": i,
            "potencia_w": v * i,
        })

    # ---- Strings PV ----
    # Percorre os MPPTs na ordem; para cada um, le quantas strings
    # ele tem (vem da topologia do modelo). A string nao tem tensao
    # propria: usa a do MPPT pai. O indice da corrente na row da API
    # avanca sequencialmente, string apos string.
    strings = []
    string_num = 0          # numero global da string (1..num_string)
    idx_corrente = 0        # deslocamento dentro do bloco IDX_IPV1
    for m in range(num_mppt):
        qtd = topologia["strings_por_mppt"].get(m + 1, 0)
        upv = mppt_v[m] if m < len(mppt_v) else 0.0
        for _ in range(qtd):
            i_idx = IDX_IPV1 + idx_corrente
            ipv = safe_float(row[i_idx]) if i_idx < len(row) else 0.0
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
    # Isso corrige o bug latente da janela 21h-00h, em que datetime.now()
    # do servidor virava o dia antes do Brasil.
    agora_br = datetime.now(FUSO_BR).replace(tzinfo=None)
    alvo = slot_alvo(agora_br)

    print("=" * 60)
    print(f"COLETOR v2 APOLO SOLAR — agora {agora_br:%Y-%m-%d %H:%M:%S} "
          f"(BR) | slot alvo {alvo:%H:%M}")

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

        for inv_id, nome, asset_id, modelo_id in inversores:
            topo = topologias.get(modelo_id)
            if topo is None:
                print(f"  [{nome}] ERRO: modelo {modelo_id} sem topologia.")
                n_erro += 1
                continue

            try:
                row, ts_chint = buscar_inversor(asset_id, alvo)
            except Exception as e:
                print(f"  [{nome}] ERRO de API: {e}")
                n_erro += 1
                continue

            if row is None:
                # A Chint ainda nao publicou a leitura deste slot. Nao gravamos
                # nada — o proximo ciclo do cron pega esse slot, sem poluir o
                # banco com SEM_DADOS.
                print(f"  [{nome}] aguardando slot {alvo:%H:%M} (Chint ainda nao publicou)")
                n_pulado += 1
                continue

            # Grava usando o timestamp da Chint truncado para o slot (segundos=0).
            # O UNIQUE(inversor_id, data_hora) garante que nao haja duplicacao.
            ts_grava = truncar_slot_5min(ts_chint)

            status, campos, mppts, strings = extrair_dados(row, topo)
            gravar_leitura(cur, inv_id, ts_grava, status,
                           campos, mppts, strings)
            total_pac += campos["pac_kw"]
            if status == "ONLINE":
                n_online += 1
            print(f"  [{nome}] {status} | {ts_chint:%H:%M:%S} -> {ts_grava:%H:%M} | "
                  f"Pac: {campos['pac_kw']:.2f} kW")

        # Confirma TODAS as gravacoes do ciclo de uma vez
        conn.commit()
        print("-" * 60)
        print(f"  Potencia total: {total_pac:.2f} kW | "
              f"Online: {n_online}/{len(inversores)} | "
              f"Aguardando: {n_pulado} | Erros: {n_erro}")
        print("  Ciclo concluido.")

    except Exception as e:
        conn.rollback()
        print(f"ERRO no ciclo — nada foi gravado: {e}")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
