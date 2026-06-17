# -*- coding: utf-8 -*-
"""
============================================================
  COLETOR v6 — APOLO SOLAR  (Railway / PostgreSQL)
============================================================
Le os inversores na API da Chint e grava as leituras no banco
PostgreSQL (schema v2).

MUDANCA DA v6 (multi-usina num so coletor):
  - Antes o coletor processava UMA usina por ciclo (USINA_SLUG
    fixo em "pk"), entao Ibiracu nunca era coletada.
  - Agora, por padrao, percorre TODAS as usinas cadastradas
    num unico ciclo. Cada inversor e buscado na Chint pelo seu
    proprio asset_id, entao um coletor so atende PK, Ibiracu e
    qualquer usina futura.
  - Continua possivel restringir a uma usina via env
    (USINA_SLUG=pk), util para depuracao.

COMO A v5/v6 RESOLVE AS COLUNAS (a mudanca-chave da v5):
  - Cada dado e pedido pelo TITULO da coluna ("Pac(W)",
    "Tmod(C)", "Umppt4(V)", "Ipv7(A)", "ISO(kOhm)"...), e nao
    por uma posicao fixa. Assim, se a ordem das colunas muda,
    o valor certo continua sendo pego.
  - PORQUE PRECISA DE TABELAS DE TITULO PRONTAS: a API da
    Chint NAO envia cabecalho — ela so manda a lista de
    valores. Os titulos so existem no portal (no JavaScript).
    Logo, o coletor nao tem um cabecalho para ler no feed.
    A v5 contorna isso guardando a LISTA REAL DE TITULOS de
    cada layout conhecido (capturada do portal) e, a cada
    leitura, escolhe a lista certa pelo numero de colunas
    (unico sinal que o feed oferece). Dentro da lista
    escolhida, cada campo e localizado pelo TITULO exato.
  - Hoje ha dois layouts: o padrao de 125 colunas e o de 181
    colunas (que aparece quando o inversor entra em erro). Os
    MPPTs no de 181 vem partidos (1-3 e 4-12 em blocos
    separados); resolver por titulo ("Umppt4(V)") acerta sem
    se importar com isso.

EXTENSAO: se surgir um layout novo (outra contagem de coluna),
  basta capturar a lista de titulos dele no portal e cola-la
  abaixo em LAYOUTS_TITULOS. Tudo passa a resolver por titulo
  automaticamente. Enquanto um layout for desconhecido, o
  coletor PULA aquele inversor e avisa, em vez de gravar errado.

COMO RODA:
  - Roda UMA vez e encerra. O Railway repete a cada 5 min
    via cron job.

CREDENCIAIS — variaveis de ambiente no Railway:
  DATABASE_URL, CHINT_TOKEN, CHINT_USER_ID
  USINA_SLUG (opcional): "todas" (default) ou um slug especifico.

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

# USINA_SLUG controla QUAIS usinas o coletor processa num ciclo:
#   - "todas" (ou vazio)        -> coleta TODAS as usinas cadastradas.
#                                  Modo recomendado: um coletor so atende
#                                  PK, Ibiracu e qualquer usina futura.
#   - um slug ("pk", "ibiracu") -> coleta apenas aquela usina (depuracao).
# IMPORTANTE: se voce tinha USINA_SLUG=pk setado no Railway, troque para
# "todas" (ou remova a variavel) para passar a coletar Ibiracu tambem.
USINA_SLUG     = (os.environ.get("USINA_SLUG", "todas") or "todas").strip().lower()
_COLETAR_TODAS = USINA_SLUG in ("", "todas", "all", "*")
DELAY_SEGUNDOS = 20
FUSO_BR        = timezone(timedelta(hours=-3))

# Limiar de "sol" para o tempo de insolacao: um slot de 5 min conta como
# "com sol" quando a potencia TOTAL da usina passa deste valor (kW). 0.5 kW
# corta ruido/standby sem perder geracao real. Ajuste se necessario.
LIMIAR_SOL_KW = 0.5

# Janela retroativa (em dias) para buscar alarmes da Chint a cada ciclo.
# O ON CONFLICT (id) deduplica, entao reconsultar a mesma janela e barato.
DIAS_ALARMES = 3

# A coluna do carimbo de hora ('Date') e a 0 em todos os layouts.
IDX_DATE = 0

# Se a contagem de colunas nao casar exatamente com nenhum layout, ainda usa
# o mais proximo se a diferenca for <= isto; acima, considera desconhecido.
TOLERANCIA_COLUNAS = 6


# ============================================================
# LISTAS DE TITULO POR LAYOUT (capturadas do portal Chint)
# ------------------------------------------------------------
# Cada lista e o cabecalho REAL de um layout, na ordem das
# colunas. O coletor escolhe a lista pelo numero de colunas da
# leitura e localiza cada campo pelo TITULO dentro dela.
# Para acrescentar um layout novo: capture o cabecalho dele no
# portal e cole a lista aqui.
# ============================================================

TITULOS_125 = [
    "Date", "DSP", "LCD", "TimeSet", "TYield(kWh)", "DYield(Wh)", "Eff(%)",
    "PF", "Pmax(kW)", "RunT(min)", "Pac(W)", "Sac(kVA)", "Uab(V)", "Ubc(V)",
    "Uca(V)", "Ia(A)", "Ib(A)", "Ic(A)", "Freq(Hz)", "Ua(V)", "Ub(V)", "Uc(V)",
    "Voltage harmonics(L1)(%)", "Voltage harmonics(L2)(%)",
    "Voltage harmonics(L3)(%)", "Current harmonics(Thd L1)(%)",
    "Current harmonics(Thd L2)(%)", "Current harmonics(Thd L3)(%)",
    "Mode", "Time", "PFault", "Warn", "Fault0", "Fault1", "Fault2", "Fault3",
    "Fault4", "Fault5", "Fault6", "Warn1",
    "Umppt1(V)", "Imppt1(A)", "Umppt2(V)", "Imppt2(A)", "Umppt3(V)", "Imppt3(A)",
    "Umppt4(V)", "Imppt4(A)", "Umppt5(V)", "Imppt5(A)", "Umppt6(V)", "Imppt6(A)",
    "Umppt7(V)", "Imppt7(A)", "Umppt8(V)", "Imppt8(A)", "Umppt9(V)", "Imppt9(A)",
    "Umppt10(V)", "Imppt10(A)", "Umppt11(V)", "Imppt11(A)", "Umppt12(V)",
    "Imppt12(A)",
    "Ipv1(A)", "Ipv2(A)", "Ipv3(A)", "Ipv4(A)", "Ipv5(A)", "Ipv6(A)", "Ipv7(A)",
    "Ipv8(A)", "Ipv9(A)", "Ipv10(A)", "Ipv11(A)", "Ipv12(A)", "Ipv13(A)",
    "Ipv14(A)", "Ipv15(A)", "Ipv16(A)", "Ipv17(A)", "Ipv18(A)", "Ipv19(A)",
    "Ipv20(A)", "Ipv21(A)", "Ipv22(A)", "Ipv23(A)", "Ipv24(A)",
    "Qac(kvar)", "MajorVer", "BusCapacitance(uF)", "AcCapacitance(uF)",
    "Pdc(kW)", "PmaxLim(kW)", "SmaxLim(kVA)", "DspSafetyVer",
    "DspCertifiedVersionEn", "ProductCode", "GridConnectionRule",
    "NeutralLineSetting", "PVInputMode", "OptnPvDectBrd", "RegUnitFlag1",
    "DspCertifiedVersion", "ExHMIAppVer", "PidPWM(%)", "PidBusPEVoltRel(V)",
    "PidBusPEVoltMax(V)", "PidFlag1", "PidBusLowVolt(V)", "InvCtrlSta1",
    "GFCI(mA)", "BoostTemprt(°C)", "McuEnvrTemprt(°C)", "McuRelayTemprt(°C)",
    "Tmod(°C)", "Tamb(°C)", "ISO(kΩ)", "DCIA(mA)", "DCIB(mA)", "DCIC(mA)",
    "UbusPst(V)", "UbusNgt(V)", "UbusPstNgt(V)", "UsampIso(V)",
]

TITULOS_181 = [
    "Date", "DSP", "LCD", "TYield(kWh)", "DYield(Wh)", "Eff(%)", "PF",
    "Pmax(kW)", "RunT(min)", "Pac(W)", "Sac(kVA)", "Uab(V)", "Ubc(V)", "Uca(V)",
    "Ia(A)", "Ib(A)", "Ic(A)",
    "Umppt1(V)", "Imppt1(A)", "Umppt2(V)", "Imppt2(A)", "Umppt3(V)", "Imppt3(A)",
    "Freq(Hz)", "Tmod(°C)", "Tamb(°C)", "Tcoil(°C)", "Mode", "Time",
    "Fault Code", "Qac(kVA)", "PIDboxEnable", "PIDbox Voltage(V)",
    "PIDbox Current(mA)", "Reserved for pidbox", "Reserved for pidbox",
    "MajorVer", "PVdetection", "BusCapacitance(uF)", "AcCapacitance(uF)",
    "Pdc(kW)", "PmaxLim(kW)", "SmaxLim(kVA)", "DspSafetyVer",
    "Reserve", "Reserve", "Reserve", "Reserve", "Reserve", "Reserve", "Reserve",
    "Reserve", "Reserve", "Reserve",
    "Umppt4(V)", "Imppt4(A)", "Umppt5(V)", "Imppt5(A)", "Umppt6(V)", "Imppt6(A)",
    "Umppt7(V)", "Imppt7(A)", "Umppt8(V)", "Imppt8(A)", "Umppt9(V)", "Imppt9(A)",
    "Umppt10(V)", "Imppt10(A)", "Umppt11(V)", "Imppt11(A)", "Umppt12(V)",
    "Imppt12(A)", "Fault2", "Umppt13(V)", "Imppt13(A)", "Umppt14(V)",
    "Imppt14(A)", "Umppt15(V)", "Imppt15(A)",
    "AuxDsp Code", "MaxLimbit", "AnGfxSN", "OnOff", "TimeSet", "AntiRefluxEn",
    "ProductCode", "GridConnectionRule", "NeutralLineSetting", "PVInputMode",
    "OptnPvDectBrd", "RegUnitFlag1", "LogoSel", "MeterType", "PidPWM(%)",
    "PidBusPEVoltRel(V)", "PidBusPEVoltMax(V)", "PidFlag1", "PidBusLowVolt(V)",
    "ABF_Grid_TotalBuyEnergy(kWh)", "ABF_Grid_TotalSellEnergy(kWh)",
    "ABF_GridUa(V)", "ABF_GridUb(V)", "ABF_GridUc(V)", "ABF_GridIa(A)",
    "ABF_GridIb(A)", "ABF_GridIc(A)", "ABF_GridPt(W)", "ABF_GridPa(W)",
    "ABF_GridPb(W)", "ABF_GridPc(W)", "ABF_Gird_TodayBuyEnergy(kWh)",
    "ABF_Grid_TodaySellEnergy(kWh)", "ABF_LoadPa(W)", "ABF_LoadPb(W)",
    "ABF_LoadPc(W)", "ABF_Load_TodayEnergy(kWh)", "ABF_Load_TotalEnergy(kWh)",
    "Ua(V)", "Ub(V)", "Uc(V)", "Voltage harmonics(L1)(%)",
    "Voltage harmonics(L2)(%)", "Voltage harmonics(L3)(%)",
    "Current harmonics(L1)(%)", "Current harmonics(L2)(%)",
    "Current harmonics(L3)(%)",
    "Ipv1(A)", "Ipv2(A)", "Ipv3(A)", "Ipv4(A)", "Ipv5(A)", "Ipv6(A)", "Ipv7(A)",
    "Ipv8(A)", "Ipv9(A)", "Ipv10(A)", "Ipv11(A)", "Ipv12(A)", "Ipv13(A)",
    "Ipv14(A)", "Ipv15(A)", "Ipv16(A)", "Ipv17(A)", "Ipv18(A)", "Ipv19(A)",
    "Ipv20(A)", "Ipv21(A)", "Ipv22(A)", "Ipv23(A)", "Ipv24(A)", "Ipv25(A)",
    "Ipv26(A)", "Ipv27(A)", "Ipv28(A)", "Ipv29(A)", "Ipv30(A)", "Ipv0(A)",
    "Mode", "ISO(kΩ)", "GFCI(mA)", "UbusPst(V)", "UbusNgt(V)", "UbusPstNgt(V)",
    "PowerBoardTemp(°C)", "Boost Temp(°C)", "ExTamb(°C)", "McuEnvrTemprt(°C)",
    "McuRelayTemprt(°C)", "InvCtrlSta1",
    "Debug parameter 18", "Debug parameter 19", "Debug parameter 20",
    "Debug parameter 21", "Debug parameter 22", "Debug parameter 23",
    "Debug parameter 24", "Debug parameter 25", "Debug parameter 26",
    "Debug parameter 27", "Debug parameter 28", "Debug parameter 29",
]

LAYOUTS_TITULOS = [TITULOS_125, TITULOS_181]


# ============================================================
# CAMPOS QUE QUEREMOS — pela CHAVE do titulo
# ------------------------------------------------------------
# A 'chave' de um titulo e a parte antes do '(' (ex.: "Pac(W)"
# -> "pac"; "Tmod(°C)" -> "tmod"; "ISO(kΩ)" -> "iso"). Isso
# casa o campo de forma EXATA (token inteiro), sem o risco de
# substring que existia na v2 (onde "Sac" podia virar "Pac").
# ============================================================

CAMPO_CHAVE = {
    "tyield_kwh": "tyield",   # TYield(kWh)  -> ja em kWh
    "dyield_kwh": "dyield",   # DYield(Wh)   -> /1000
    "pac_kw":     "pac",      # Pac(W)       -> /1000
    "freq_hz":    "freq",     # Freq(Hz)
    "pdc_kw":     "pdc",      # Pdc(kW)
    "tmod_c":     "tmod",     # Tmod(°C)
    "tamb_c":     "tamb",     # Tamb(°C)
    "iso_kohm":   "iso",      # ISO(kΩ)
}

# Divisor de unidade por campo (1.0 = sem conversao).
CAMPO_DIVISOR = {
    "pac_kw":     1000.0,     # W  -> kW
    "dyield_kwh": 1000.0,     # Wh -> kWh
}


# ============================================================
# RESOLUCAO POR TITULO
# ============================================================

def chave_titulo(titulo):
    """Reduz um titulo a sua 'chave' (parte antes do '(', minuscula).
    'Pac(W)'->'pac', 'Umppt4(V)'->'umppt4', 'Ipv7(A)'->'ipv7',
    'ISO(kΩ)'->'iso'. Casa o token inteiro, nunca por substring."""
    s = str(titulo).strip().lower()
    return s.split("(", 1)[0].strip()


def montar_mapa(titulos):
    """A partir da lista de titulos, monta {chave: indice}.
    Se uma chave repetir, vence a primeira ocorrencia."""
    mapa = {}
    for i, t in enumerate(titulos):
        k = chave_titulo(t)
        if k and k not in mapa:
            mapa[k] = i
    return mapa


def escolher_titulos(row):
    """Escolhe a lista de titulos cujo tamanho mais se aproxima de len(row).
    Retorna (titulos, exato, diferenca)."""
    n = len(row)
    titulos = min(LAYOUTS_TITULOS, key=lambda T: abs(len(T) - n))
    diff = abs(len(titulos) - n)
    return titulos, (diff == 0), diff


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
    """Trunca um datetime para o multiplo de 5 min anterior, segundos = 0."""
    minuto = (dt.minute // 5) * 5
    return dt.replace(minute=minuto, second=0, microsecond=0)


def parse_data_chint(texto):
    """Converte texto de data em datetime EM UTC (naive). None se falhar."""
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
# BUSCA NA CHINT
# ============================================================

def buscar_leituras_validas(asset_id):
    """Busca na Chint as ultimas leituras multiplas de 5 (ignora fora-de-grid),
    ja convertidas para UTC. Retorna lista de (ts_utc, row), mais recente
    primeiro. O carimbo de hora ('Date') esta no indice 0 em todos os layouts."""
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

    validas = []
    for row in rows:
        if not row:
            continue
        ts = parse_data_chint(row[IDX_DATE]) if IDX_DATE < len(row) else None
        if ts is None:
            continue
        if ts.minute % 5 != 0:
            continue
        validas.append((ts, row))
    return validas


# ============================================================
# EXTRACAO DOS DADOS (tudo pelo TITULO)
# ============================================================

def extrair_dados(row, topologia, titulos):
    """Converte uma row da API Chint nos campos + canais, localizando cada
    coluna pelo TITULO dentro da lista 'titulos' escolhida para esta leitura.

    Retorna: (status, campos, mppts, strings)."""
    num_mppt = topologia["num_mppt"]
    mapa = montar_mapa(titulos)

    def ler_chave(chave):
        idx = mapa.get(chave)
        return safe_float(row[idx]) if (idx is not None and idx < len(row)) else 0.0

    campos = {}
    for campo, chave in CAMPO_CHAVE.items():
        valor = ler_chave(chave)
        campos[campo] = valor / CAMPO_DIVISOR.get(campo, 1.0)

    # ---- MPPTs: tensao 'Umppt{n}(V)' e corrente 'Imppt{n}(A)' ----
    mppt_v = []
    mppts  = []
    for m in range(num_mppt):
        v = ler_chave(f"umppt{m + 1}")
        i = ler_chave(f"imppt{m + 1}")
        mppt_v.append(v)
        mppts.append({
            "mppt": m + 1,
            "tensao_v": v, "corrente_a": i,
            "potencia_w": v * i,
        })

    # ---- Strings PV: corrente 'Ipv{n}(A)' ----
    # A numeracao global da string (1..num_string) bate com o numero no titulo
    # Ipv{n} nos dois layouts. A tensao da string e a do MPPT pai.
    strings = []
    string_num = 0
    for m in range(num_mppt):
        qtd = topologia["strings_por_mppt"].get(m + 1, 0)
        upv = mppt_v[m] if m < len(mppt_v) else 0.0
        for _ in range(qtd):
            string_num += 1
            ipv_a = ler_chave(f"ipv{string_num}")
            strings.append({
                "string_num": string_num,
                "mppt": m + 1,
                "corrente_a": ipv_a,
                "potencia_w": ipv_a * upv,
            })

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
# RESUMO DO DIA — alimenta resumo_dia / resumo_dia_inversor
# ------------------------------------------------------------
# Recalcula o resumo do DIA DE HOJE (por usina) a partir das
# leituras cruas e faz UPSERT. Roda a cada ciclo (5 em 5 min),
# entao os modos MENSAL e ANUAL do dashboard ficam ao vivo: o
# valor do dia em curso atualiza junto com a coleta.
#
# Como nada e recalculado para dias passados (so hoje), o custo
# e baixo (algumas milhares de linhas, resumidas NO banco).
#
# FUSOS: leitura.data_hora e gravado em UTC (naive). O Brasil e
# UTC-3, entao:
#   - 00:00 BR  ->  03:00 UTC
#   - 06:00 BR  ->  09:00 UTC   (inicio da janela de luz)
#   - 18:00 BR  ->  21:00 UTC   (fim da janela de luz)
# A hora do pico e devolvida ja em BR ((data_hora - 3h)::time).
#
# DEFINICOES (espelham o endpoint /dia da API):
#   energia_kwh = soma do MAX(dyield_kwh) de cada inversor no dia
#   pico_kw     = maior potencia INSTANTANEA (soma dos inversores)
#   pac_medio   = media da potencia na janela 06-18h BR
#   disponibilidade = slots gerando / slots com leitura (06-18h)
#   insolacao_h = NULL aqui (nao ha sensor de irradiancia; se um
#                 processo externo preencher, este UPSERT NAO
#                 sobrescreve a insolacao existente).
# ============================================================

def resumir_dia(cur, agora_br_naive):
    """UPSERT do resumo do dia de hoje, por usina, em resumo_dia
    e resumo_dia_inversor. Le as leituras cruas e resume no banco."""
    dia = agora_br_naive.date()
    base_br = datetime(dia.year, dia.month, dia.day)   # 00:00 BR (naive)
    params = {
        "dia":     dia,
        "ini":     base_br + timedelta(hours=3),       # 00:00 BR -> UTC
        "fim":     base_br + timedelta(days=1, hours=3),
        "luz_ini": base_br + timedelta(hours=6 + 3),   # 06:00 BR -> UTC
        "luz_fim": base_br + timedelta(hours=18 + 3),  # 18:00 BR -> UTC
        "limiar":  LIMIAR_SOL_KW,
    }

    # ---- POR INVERSOR ----
    cur.execute(
        """
        WITH ld AS (
            SELECT l.inversor_id, l.data_hora, l.pac_kw, l.dyield_kwh, l.tmod_c
            FROM leitura l
            WHERE l.data_hora >= %(ini)s AND l.data_hora < %(fim)s
        ),
        slots_luz AS (   -- slots (por usina) com qualquer leitura na luz
            SELECT i.usina_id, COUNT(DISTINCT ld.data_hora) AS n_slots
            FROM ld JOIN inversor i ON i.id = ld.inversor_id
            WHERE ld.data_hora >= %(luz_ini)s AND ld.data_hora < %(luz_fim)s
            GROUP BY i.usina_id
        ),
        energia AS (
            SELECT inversor_id, MAX(dyield_kwh) AS energia_kwh,
                   COUNT(*) AS leituras,
                   MAX(tmod_c) AS tmod_max, AVG(tmod_c) AS tmod_med
            FROM ld GROUP BY inversor_id
        ),
        pico AS (
            SELECT DISTINCT ON (inversor_id) inversor_id,
                   pac_kw AS pico_kw,
                   (data_hora - interval '3 hours')::time AS pico_hora
            FROM ld ORDER BY inversor_id, pac_kw DESC, data_hora
        ),
        gerando AS (
            SELECT inversor_id, COUNT(DISTINCT data_hora) AS n
            FROM ld
            WHERE data_hora >= %(luz_ini)s AND data_hora < %(luz_fim)s
              AND pac_kw > 0.05
            GROUP BY inversor_id
        )
        INSERT INTO resumo_dia_inversor
            (inversor_id, data, energia_kwh, pico_kw, pico_hora,
             tmod_max, tmod_med, disponibilidade, leituras)
        SELECT e.inversor_id, %(dia)s::date,
               COALESCE(e.energia_kwh, 0),
               COALESCE(p.pico_kw, 0),
               p.pico_hora,
               e.tmod_max,
               e.tmod_med,
               CASE WHEN COALESCE(sl.n_slots, 0) > 0
                    THEN LEAST(100.0,
                         COALESCE(g.n, 0)::numeric / sl.n_slots * 100)
                    ELSE 0 END,
               COALESCE(e.leituras, 0)
        FROM energia e
        JOIN inversor i      ON i.id = e.inversor_id
        LEFT JOIN pico p     ON p.inversor_id = e.inversor_id
        LEFT JOIN gerando g  ON g.inversor_id = e.inversor_id
        LEFT JOIN slots_luz sl ON sl.usina_id = i.usina_id
        ON CONFLICT (inversor_id, data) DO UPDATE SET
            energia_kwh     = EXCLUDED.energia_kwh,
            pico_kw         = EXCLUDED.pico_kw,
            pico_hora       = EXCLUDED.pico_hora,
            tmod_max        = EXCLUDED.tmod_max,
            tmod_med        = EXCLUDED.tmod_med,
            disponibilidade = EXCLUDED.disponibilidade,
            leituras        = EXCLUDED.leituras
        """,
        params,
    )

    # ---- POR USINA (planta) ----
    cur.execute(
        """
        WITH ld AS (
            SELECT i.usina_id, l.inversor_id, l.data_hora,
                   l.pac_kw, l.dyield_kwh
            FROM leitura l
            JOIN inversor i ON i.id = l.inversor_id
            WHERE l.data_hora >= %(ini)s AND l.data_hora < %(fim)s
        ),
        pac_slot AS (    -- potencia total da usina por instante
            SELECT usina_id, data_hora, SUM(pac_kw) AS pac_total
            FROM ld GROUP BY usina_id, data_hora
        ),
        pico AS (        -- maior potencia instantanea + hora (BR)
            SELECT DISTINCT ON (usina_id) usina_id,
                   pac_total AS pico_kw,
                   (data_hora - interval '3 hours')::time AS pico_hora
            FROM pac_slot ORDER BY usina_id, pac_total DESC, data_hora
        ),
        media AS (       -- pac media da usina na janela 06-18h
            SELECT usina_id, AVG(pac_total) AS pac_medio
            FROM pac_slot
            WHERE data_hora >= %(luz_ini)s AND data_hora < %(luz_fim)s
            GROUP BY usina_id
        ),
        sol AS (         -- tempo de insolacao = slots com sol * 5min, em horas
            SELECT usina_id, COUNT(*) * 5.0 / 60.0 AS insolacao_h
            FROM pac_slot
            WHERE pac_total > %(limiar)s
            GROUP BY usina_id
        ),
        energia AS (     -- energia = soma do MAX(dyield) por inversor
            SELECT usina_id, SUM(maxdy) AS energia_kwh FROM (
                SELECT usina_id, inversor_id, MAX(dyield_kwh) AS maxdy
                FROM ld GROUP BY usina_id, inversor_id
            ) s GROUP BY usina_id
        ),
        ninv AS (
            SELECT usina_id, COUNT(DISTINCT inversor_id) AS n,
                   COUNT(*) AS leituras
            FROM ld GROUP BY usina_id
        )
        INSERT INTO resumo_dia
            (usina_id, data, energia_kwh, pac_medio_kw, pico_kw,
             pico_hora, insolacao_h, inversores_no_dia, leituras_totais)
        SELECT e.usina_id, %(dia)s::date,
               COALESCE(e.energia_kwh, 0),
               COALESCE(m.pac_medio, 0),
               COALESCE(p.pico_kw, 0),
               p.pico_hora,
               COALESCE(so.insolacao_h, 0),
               COALESCE(n.n, 0),
               COALESCE(n.leituras, 0)
        FROM energia e
        LEFT JOIN media m ON m.usina_id = e.usina_id
        LEFT JOIN sol so  ON so.usina_id = e.usina_id
        LEFT JOIN pico p  ON p.usina_id = e.usina_id
        LEFT JOIN ninv n  ON n.usina_id = e.usina_id
        ON CONFLICT (usina_id, data) DO UPDATE SET
            energia_kwh       = EXCLUDED.energia_kwh,
            pac_medio_kw      = EXCLUDED.pac_medio_kw,
            pico_kw           = EXCLUDED.pico_kw,
            pico_hora         = EXCLUDED.pico_hora,
            insolacao_h       = EXCLUDED.insolacao_h,
            inversores_no_dia = EXCLUDED.inversores_no_dia,
            leituras_totais   = EXCLUDED.leituras_totais
        """,
        params,
    )


# ============================================================
# ALARMES — Chint (event/list) + falhas de leitura do coletor
# ------------------------------------------------------------
# Grava na tabela 'alarmes', unificando duas origens:
#   origem='chint'   -> alarmes reais da Chint (/api/toolbox/event/list).
#                       Chave = o id da Chint (dedup via ON CONFLICT).
#   origem='coletor' -> falhas de leitura (erro de API ou inversor sem
#                       leitura no slot) = possivel queda de internet na
#                       usina. Modelado como EPISODIO: abre quando falha,
#                       fecha (fim_em) quando o inversor volta a ler.
# ============================================================

def _parse_iso_utc(s):
    """'2026-06-16T09:17:15.000+00:00' -> datetime naive em UTC."""
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def buscar_alarmes_chint(site_id, begin, end, limit=100):
    """Busca todos os alarmes de uma usina no intervalo [begin, end]
    (YYYY-MM-DD), paginando. Retorna a lista de registros crus."""
    todos = []
    page = 1
    while True:
        url = (
            f"{BASE}/api/toolbox/event/list"
            f"?key=&siteId={site_id}&begin={begin}&end={end}"
            f"&errorType=&checked=&errorReasonType=1&assetId="
            f"&page={page}&limit={limit}&lifetime=false"
        )
        req  = Request(url, headers=HEADERS)
        resp = urlopen(req, timeout=20)
        data = json.loads(resp.read())
        if data.get("code") != "0":
            raise ValueError(f"event/list retornou erro: {data.get('msg')}")
        lote = data.get("data") or []
        todos.extend(lote)
        total = data.get("count") or 0
        if len(lote) < limit or len(todos) >= total or page >= 50:
            break
        page += 1
    return todos


def _gravar_alarmes_chint(cur, usina_id, registros, mapa_sn):
    """UPSERT dos alarmes da Chint. mapa_sn: deviceSn -> inversor_id."""
    for rec in registros:
        aid = rec.get("id")
        if not aid:
            continue
        sn     = rec.get("deviceSn") or rec.get("assetAlias")
        inv_id = mapa_sn.get(sn)
        ocorr  = _parse_iso_utc(rec.get("actionDate"))
        descr  = (rec.get("errorDescriptron") or rec.get("content")
                  or rec.get("description"))
        cur.execute(
            """
            INSERT INTO alarmes
                (id, origem, usina_id, inversor_id, device_sn, gateway_id,
                 modbus_id, site_name, ocorrido_em, inicio_em, fim_em,
                 action_type, error_type, codigo, descricao, model,
                 code_count, checked, dados_brutos)
            VALUES (%s,'chint',%s,%s,%s,%s,%s,%s,%s,%s,NULL,%s,%s,%s,%s,%s,
                    %s,%s,%s::jsonb)
            ON CONFLICT (id) DO UPDATE SET
                checked       = EXCLUDED.checked,
                code_count    = EXCLUDED.code_count,
                descricao     = EXCLUDED.descricao,
                dados_brutos  = EXCLUDED.dados_brutos,
                inversor_id   = COALESCE(alarmes.inversor_id, EXCLUDED.inversor_id),
                atualizado_em = now()
            """,
            (
                aid, usina_id, inv_id, sn, rec.get("gatewayId"),
                rec.get("modbusId"), rec.get("siteName"), ocorr, ocorr,
                rec.get("actionType"), rec.get("errorType"),
                rec.get("description"), descr, rec.get("model"),
                rec.get("codeCount"), bool(rec.get("checked")),
                json.dumps(rec, ensure_ascii=False),
            ),
        )


def _registrar_falhas_coletor(cur, inv_falha, inv_ok, slot_utc):
    """Episodios de falha de leitura. inv_falha: lista (inv_id, codigo,
    descricao). inv_ok: set de inversores que LERAM neste ciclo."""
    for inv_id, codigo, descricao in inv_falha:
        cur.execute(
            """
            SELECT id FROM alarmes
            WHERE origem='coletor' AND inversor_id=%s AND fim_em IS NULL
            ORDER BY inicio_em DESC LIMIT 1
            """,
            (inv_id,),
        )
        aberto = cur.fetchone()
        if aberto:   # ja ha episodio em aberto -> estende
            cur.execute(
                """
                UPDATE alarmes SET ocorrido_em=%s, ocorrencias=ocorrencias+1,
                       codigo=%s, descricao=%s, atualizado_em=now()
                WHERE id=%s
                """,
                (slot_utc, codigo, descricao, aberto[0]),
            )
        else:        # abre episodio novo
            novo_id = f"coletor:{inv_id}:{slot_utc:%Y%m%dT%H%M}"
            cur.execute(
                """
                INSERT INTO alarmes
                    (id, origem, inversor_id, usina_id, ocorrido_em, inicio_em,
                     fim_em, error_type, codigo, descricao, ocorrencias)
                SELECT %s,'coletor',%s,i.usina_id,%s,%s,NULL,'LEITURA',%s,%s,1
                FROM inversor i WHERE i.id=%s
                ON CONFLICT (id) DO NOTHING
                """,
                (novo_id, inv_id, slot_utc, slot_utc, codigo, descricao, inv_id),
            )

    # Fecha episodios dos inversores que voltaram a ler
    if inv_ok:
        cur.execute(
            """
            UPDATE alarmes SET fim_em=%s, atualizado_em=now()
            WHERE origem='coletor' AND fim_em IS NULL
              AND inversor_id = ANY(%s)
            """,
            (slot_utc, list(inv_ok)),
        )


def coletar_alarmes(cur, inv_falha, inv_ok, slot_utc, agora_br):
    """Roda ao fim do ciclo: episodios de falha (sempre) + alarmes da
    Chint (para cada usina que tiver site_id)."""
    # 1) falhas de leitura — nao dependem da Chint
    _registrar_falhas_coletor(cur, inv_falha, inv_ok, slot_utc)

    # 2) alarmes da Chint, por usina com site_id
    cur.execute("SELECT id, slug, site_id FROM usina "
                "WHERE site_id IS NOT NULL AND site_id <> ''")
    usinas = cur.fetchall()
    if not usinas:
        return

    # mapa deviceSn -> inversor_id (melhor esforco, via asset_id)
    cur.execute("SELECT id, asset_id FROM inversor WHERE asset_id IS NOT NULL")
    mapa_sn = {a: i for (i, a) in cur.fetchall()}

    fim   = agora_br.date()
    begin = fim - timedelta(days=DIAS_ALARMES)
    for usina_id, slug, site_id in usinas:
        try:
            regs = buscar_alarmes_chint(site_id, begin.isoformat(), fim.isoformat())
            _gravar_alarmes_chint(cur, usina_id, regs, mapa_sn)
            print(f"  Alarmes Chint [{slug}]: {len(regs)} registro(s).")
        except Exception as e:
            print(f"  AVISO: alarmes Chint [{slug}] falhou: {e}")


# ============================================================
# CICLO DE COLETA — roda UMA vez
# ============================================================

def main():
    agora_br = datetime.now(FUSO_BR).replace(tzinfo=None)

    print("=" * 60)
    print(f"COLETOR v6 APOLO SOLAR — agora {agora_br:%Y-%m-%d %H:%M:%S} (BR)")
    print(f"  Usinas: {'TODAS' if _COLETAR_TODAS else USINA_SLUG}")
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

        # Seleciona os inversores a coletar. No modo "todas" (default), pega
        # de TODAS as usinas num so ciclo — cada inversor e buscado na Chint
        # pelo seu asset_id, entao funciona para qualquer usina. O slug da
        # usina vem junto so para diferenciar no log PK x Ibiracu (ambos tem
        # "Inversor 1", "Inversor 2"...).
        if _COLETAR_TODAS:
            cur.execute(
                """
                SELECT i.id, i.nome, i.asset_id, i.modelo_id, u.slug AS usina
                FROM inversor i
                JOIN usina u ON u.id = i.usina_id
                ORDER BY u.id, i.idx
                """
            )
        else:
            cur.execute(
                """
                SELECT i.id, i.nome, i.asset_id, i.modelo_id, u.slug AS usina
                FROM inversor i
                JOIN usina u ON u.id = i.usina_id
                WHERE u.slug = %s
                ORDER BY u.id, i.idx
                """,
                (USINA_SLUG,),
            )
        inversores = cur.fetchall()

        if not inversores:
            alvo = "qualquer usina" if _COLETAR_TODAS else f"a usina '{USINA_SLUG}'"
            print(f"ERRO: nenhum inversor para {alvo}.")
            print("Rode o schema_v2.sql no banco primeiro.")
            sys.exit(1)

        total_pac = 0.0
        n_online  = 0
        n_pulado  = 0
        n_erro    = 0
        inv_ok    = set()   # inversores que LERAM neste ciclo
        inv_falha = []      # (inv_id, codigo, descricao) p/ alarme de leitura

        for inv_id, nome, asset_id, modelo_id, usina in inversores:
            etq = f"{usina}/{nome}"   # ex.: "ibiracu/Inversor 1"

            topo = topologias.get(modelo_id)
            if topo is None:
                print(f"  [{etq}] ERRO: modelo {modelo_id} sem topologia.")
                n_erro += 1
                continue

            if not asset_id:
                print(f"  [{etq}] ERRO: sem asset_id cadastrado (nao da pra "
                      f"buscar na Chint).")
                n_erro += 1
                continue

            try:
                validas = buscar_leituras_validas(asset_id)
            except Exception as e:
                print(f"  [{etq}] ERRO de API: {e}")
                n_erro += 1
                # falha de conexao com a Chint -> possivel queda de internet
                inv_falha.append((inv_id, "ERRO_API", str(e)[:200]))
                continue

            if not validas:
                print(f"  [{etq}] sem leitura valida (Chint sem multiplo de 5)")
                n_pulado += 1
                inv_falha.append((inv_id, "SEM_LEITURA",
                                  "Chint sem leitura no slot"))
                continue

            ts_chint, row = validas[0]
            ts_grava = truncar_slot_5min(ts_chint)

            titulos, exato, diff = escolher_titulos(row)
            if diff > TOLERANCIA_COLUNAS:
                print(f"  [{etq}] AVISO: {len(row)} colunas nao casa com nenhum "
                      f"layout conhecido (mais proximo: {len(titulos)}, diff "
                      f"{diff}). Pulado para nao gravar errado. Capture o "
                      f"cabecalho desse inversor e adicione em LAYOUTS_TITULOS.")
                n_erro += 1
                continue
            if not exato:
                print(f"  [{etq}] AVISO: {len(row)} colunas; usando layout de "
                      f"{len(titulos)} (diff {diff}).")

            status, campos, mppts, strings = extrair_dados(row, topo, titulos)
            gravar_leitura(cur, inv_id, ts_grava, status,
                           campos, mppts, strings)
            total_pac += campos["pac_kw"]
            inv_ok.add(inv_id)   # leu com sucesso -> fecha episodio de falha
            if status == "ONLINE":
                n_online += 1

            ts_chint_br = ts_chint - timedelta(hours=3)
            ts_grava_br = ts_grava - timedelta(hours=3)
            print(f"  [{etq}] {status} | {len(titulos)}col | "
                  f"Chint {ts_chint_br:%H:%M:%S} BR -> slot {ts_grava_br:%H:%M} BR | "
                  f"Pac: {campos['pac_kw']:.2f} kW")

        conn.commit()   # leituras cruas ja estao salvas

        # Resumo do dia (alimenta mensal/anual ao vivo). Vai num commit
        # SEPARADO: se falhar, as leituras acima permanecem gravadas.
        try:
            resumir_dia(cur, agora_br)
            conn.commit()
            print("  Resumo do dia atualizado (resumo_dia / por inversor).")
        except Exception as e:
            conn.rollback()
            print(f"  AVISO: resumo do dia falhou (leituras OK): {e}")

        # Alarmes (Chint + falhas de leitura). Commit SEPARADO tambem.
        try:
            slot_utc = truncar_slot_5min(agora_br + timedelta(hours=3))  # BR->UTC
            coletar_alarmes(cur, inv_falha, inv_ok, slot_utc, agora_br)
            conn.commit()
            print("  Alarmes atualizados (Chint + falhas de leitura).")
        except Exception as e:
            conn.rollback()
            print(f"  AVISO: alarmes falharam (leituras/resumo OK): {e}")

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
