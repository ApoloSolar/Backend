# -*- coding: utf-8 -*-
"""
============================================================
  COLETOR — APOLO SOLAR  (Railway / PostgreSQL)
============================================================
Le os 8 inversores da usina Presidente Kennedy na API da
Chint e grava as leituras no banco PostgreSQL.

DIFERENCA PARA O Proxy_SQL_teste.py:
  - Grava em PostgreSQL (nuvem), nao em SQLite (arquivo local)
  - Roda UMA vez e encerra (sem 'while True')
  - O Railway repete a execucao a cada 5 min, via cron job

CREDENCIAIS — nunca ficam no codigo. Sao lidas de variaveis
de ambiente, configuradas no painel do Railway:
  DATABASE_URL  -> string de conexao do PostgreSQL
  CHINT_TOKEN   -> token de acesso a API da Chint
  CHINT_USER_ID -> id de usuario da Chint

Dependencia: psycopg (driver PostgreSQL). Ver requirements.txt.
============================================================
"""

from urllib.request import urlopen, Request
from datetime import datetime
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

# Se faltar alguma credencial, encerra com mensagem clara.
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

# ---- indices dos campos na resposta da API Chint ----
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

NUM_MPPT = 12
NUM_STR  = 24


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


def slot_5min(dt):
    """Arredonda (trunca) o datetime para o multiplo de 5 minutos."""
    minuto = (dt.minute // 5) * 5
    return dt.replace(minute=minuto, second=0, microsecond=0)


def buscar_inversor(asset_id):
    """Busca a leitura mais recente de um inversor na API da Chint.
    Retorna a row (lista) ou None. Pode levantar excecao em erro de rede."""
    hoje = datetime.now().strftime("%Y-%m-%d")
    url = (
        f"{BASE}/openApi/v1/deviceData/deviceDataPageList"
        f"?assetId={asset_id}&startDay={hoje}&endDay={hoje}"
        f"&dataType=&lang=pt-PT&page=1&limit=1"
    )
    req  = Request(url, headers=HEADERS)
    resp = urlopen(req, timeout=20)
    data = json.loads(resp.read())
    if data.get("code") != "0":
        raise ValueError(f"API Chint retornou erro: {data.get('msg')}")
    rows = data.get("data", {}).get("dataList") or []
    return rows[0] if rows else None


def extrair_dados(row):
    """Converte uma row da API Chint nos campos principais + canais."""
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

    # MPPTs: tensao e corrente
    mppt_v, mppt_i = [], []
    for m in range(NUM_MPPT):
        v_idx = IDX_UMPPT1 + m * 2
        i_idx = IDX_IMPPT1 + m * 2
        mppt_v.append(safe_float(row[v_idx]) if v_idx < len(row) else 0.0)
        mppt_i.append(safe_float(row[i_idx]) if i_idx < len(row) else 0.0)

    canais = []
    for m in range(NUM_MPPT):
        canais.append({
            "tipo": "MPPT", "canal": m + 1,
            "tensao_v": mppt_v[m], "corrente_a": mppt_i[m],
            "potencia_w": mppt_v[m] * mppt_i[m],
        })

    # Strings PV: corrente individual; tensao = a do MPPT correspondente
    for s in range(NUM_STR):
        i_idx  = IDX_IPV1 + s
        mppt_n = s // 2
        ipv    = safe_float(row[i_idx]) if i_idx < len(row) else 0.0
        upv    = mppt_v[mppt_n] if mppt_n < len(mppt_v) else 0.0
        canais.append({
            "tipo": "PV", "canal": s + 1,
            "tensao_v": upv, "corrente_a": ipv,
            "potencia_w": ipv * upv,
        })

    status = "ONLINE" if campos["pac_kw"] > 0 else "OFFLINE"
    return status, campos, canais


def linha_zerada():
    """Campos e canais zerados, para status ERRO/SEM_DADOS."""
    campos = {k: 0.0 for k in
              ("pac_kw", "dyield_kwh", "tyield_kwh", "freq_hz",
               "tmod_c", "tamb_c", "iso_kohm", "pdc_kw")}
    canais = []
    for m in range(NUM_MPPT):
        canais.append({"tipo": "MPPT", "canal": m + 1,
                        "tensao_v": 0.0, "corrente_a": 0.0, "potencia_w": 0.0})
    for s in range(NUM_STR):
        canais.append({"tipo": "PV", "canal": s + 1,
                        "tensao_v": 0.0, "corrente_a": 0.0, "potencia_w": 0.0})
    return campos, canais


# ============================================================
# GRAVACAO NO BANCO
# ============================================================

def gravar_leitura(cur, inversor_id, ts, status, campos, canais):
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

    # 2) canais — apaga os antigos desta leitura e reinsere
    cur.execute("DELETE FROM leitura_canal WHERE leitura_id = %s", (leitura_id,))
    cur.executemany(
        """
        INSERT INTO leitura_canal
            (leitura_id, tipo, canal, tensao_v, corrente_a, potencia_w)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        [(leitura_id, c["tipo"], c["canal"],
          c["tensao_v"], c["corrente_a"], c["potencia_w"]) for c in canais],
    )


# ============================================================
# CICLO DE COLETA — roda UMA vez
# ============================================================

def main():
    ts = slot_5min(datetime.now())
    print("=" * 60)
    print(f"COLETOR APOLO SOLAR — {ts:%Y-%m-%d %H:%M}")

    # Conecta ao PostgreSQL
    conn = psycopg.connect(DATABASE_URL, connect_timeout=15)
    try:
        cur = conn.cursor()

        # Busca os inversores cadastrados desta usina
        cur.execute(
            """
            SELECT i.id, i.nome, i.asset_id
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
            print("Rode o schema.sql no banco primeiro.")
            sys.exit(1)

        total_pac = 0.0
        n_online  = 0
        n_erro    = 0

        for inv_id, nome, asset_id in inversores:
            try:
                row = buscar_inversor(asset_id)
            except Exception as e:
                print(f"  [{nome}] ERRO de API: {e}")
                campos, canais = linha_zerada()
                gravar_leitura(cur, inv_id, ts, "ERRO", campos, canais)
                n_erro += 1
                continue

            if row is None:
                print(f"  [{nome}] SEM_DADOS")
                campos, canais = linha_zerada()
                gravar_leitura(cur, inv_id, ts, "SEM_DADOS", campos, canais)
                continue

            status, campos, canais = extrair_dados(row)
            gravar_leitura(cur, inv_id, ts, status, campos, canais)
            total_pac += campos["pac_kw"]
            if status == "ONLINE":
                n_online += 1
            print(f"  [{nome}] {status} | Pac: {campos['pac_kw']:.2f} kW")

        # Confirma TODAS as gravacoes do ciclo de uma vez
        conn.commit()
        print("-" * 60)
        print(f"  Potencia total: {total_pac:.2f} kW | "
              f"Online: {n_online}/{len(inversores)} | Erros: {n_erro}")
        print("  Ciclo gravado com sucesso.")

    except Exception as e:
        conn.rollback()
        print(f"ERRO no ciclo — nada foi gravado: {e}")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
