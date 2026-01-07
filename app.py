import os
import json
import time
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timezone, timedelta
from dateutil import parser as dtparser
import requests
import msal
import re

CFG_PATH = "/app/config.json"
STATE_PATH = "/app/state.json"

GRAPH = "https://graph.microsoft.com/v1.0"
CLOCKIFY_DETAILED_REPORT_URL = (
    "https://reports.api.clockify.me/v1/workspaces/{wid}/reports/detailed"
)


# ----------------------------
# Utils
# ----------------------------


def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, data: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_dt(s: str | None) -> datetime | None:
    return dtparser.isoparse(s) if s else None


def round_up_to_block(minutes: int, block: int) -> int:
    if minutes <= 0:
        return 0
    return ((minutes + block - 1) // block) * block


def safe_float_km(text) -> float:
    if text is None:
        return 0.0
    t = str(text).strip().replace(",", ".")
    if not t:
        return 0.0
    try:
        return float(t)
    except Exception:
        return 0.0


def minutes_diff(a: datetime, b: datetime) -> float:
    return abs((a - b).total_seconds()) / 60.0


def split_emails(value: str) -> list[str]:
    if not value:
        return []
    parts = [p.strip() for p in value.replace(",", ";").split(";")]
    return [p for p in parts if p]


def env_flag(name: str, default: str = "0") -> bool:
    return (os.environ.get(name, default) or default).strip().lower() in (
        "1",
        "true",
        "yes",
        "y",
        "on",
    )


def is_truthy(v) -> bool:
    if v is True:
        return True
    if v is False or v is None:
        return False
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "sim", "y", "on")


def normalize_item_id(item_id) -> str:
    s = str(item_id).strip()
    if not s.isdigit():
        raise RuntimeError(f"item_id inválido devolvido pelo Graph: {s}")
    return s

def normalize_group_key(v: str) -> str:
    """
    Normaliza GrupoContrato para um formato consistente e legível.
    Ex:
      'Anglotex2026'    -> 'anglotex-2026'
      'Anglotex 2026'   -> 'anglotex-2026'
      'anglotex__2026'  -> 'anglotex-2026'
      ' Anglotex - VIP' -> 'anglotex-vip'
    """
    s = (v or "").strip().lower()
    if not s:
        return ""

    s = re.sub(r"[\s_]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")

    m = re.match(r"^(.*?)(\d{4})$", s)
    if m:
        left = m.group(1).rstrip("-")
        year = m.group(2)
        if left:
            return f"{left}-{year}"
        return year

    return s

# ----------------------------
# State helpers
# ----------------------------


def state_get_processed(state: dict) -> dict:
    return state.get("processed_assist_ids", {}) or {}


def state_mark_processed(state: dict, assist_id: str) -> None:
    processed = state_get_processed(state)
    processed[str(assist_id)] = iso_z(utc_now())
    state["processed_assist_ids"] = processed


def state_prune_processed(state: dict, keep_days: int = 7) -> None:
    processed = state_get_processed(state)
    if not processed:
        return
    cutoff = utc_now() - timedelta(days=keep_days)
    newp = {}
    for k, v in processed.items():
        dt = parse_dt(v)
        if dt and dt >= cutoff:
            newp[k] = v
    state["processed_assist_ids"] = newp


def state_is_processed_recent(
    state: dict, assist_id: str, within_days: int = 7
) -> bool:
    processed = state_get_processed(state)
    ts = processed.get(str(assist_id))
    if not ts:
        return False
    dt = parse_dt(ts)
    if not dt:
        return False
    return dt >= (utc_now() - timedelta(days=within_days))


def state_get_sent_emails(state: dict) -> dict:
    return state.get("sent_email_items", {}) or {}


def state_mark_email_sent(state: dict, item_id: str) -> None:
    sent = state_get_sent_emails(state)
    sent[str(item_id)] = iso_z(utc_now())
    state["sent_email_items"] = sent


def state_is_email_sent_recent(
    state: dict, item_id: str, within_days: int = 30
) -> bool:
    sent = state_get_sent_emails(state)
    ts = sent.get(str(item_id))
    if not ts:
        return False
    dt = parse_dt(ts)
    if not dt:
        return False
    return dt >= (utc_now() - timedelta(days=within_days))


def state_prune_sent_emails(state: dict, keep_days: int = 90) -> None:
    sent = state_get_sent_emails(state)
    if not sent:
        return
    cutoff = utc_now() - timedelta(days=keep_days)
    newd = {}
    for k, v in sent.items():
        dt = parse_dt(v)
        if dt and dt >= cutoff:
            newd[k] = v
    state["sent_email_items"] = newd

def state_get_alerts(state: dict) -> dict:
    return state.get("sent_alerts", {}) or {}

def state_mark_alert_sent(state: dict, alert_key: str) -> None:
    d = state_get_alerts(state)
    d[str(alert_key)] = iso_z(utc_now())
    state["sent_alerts"] = d

def state_is_alert_sent_recent(state: dict, alert_key: str, within_days: int = 365) -> bool:
    d = state_get_alerts(state)
    ts = d.get(str(alert_key))
    if not ts:
        return False
    dt = parse_dt(ts)
    if not dt:
        return False
    return dt >= (utc_now() - timedelta(days=within_days))

def state_prune_alerts(state: dict, keep_days: int = 400) -> None:
    d = state_get_alerts(state)
    if not d:
        return
    cutoff = utc_now() - timedelta(days=keep_days)
    newd = {}
    for k, v in d.items():
        dt = parse_dt(v)
        if dt and dt >= cutoff:
            newd[k] = v
    state["sent_alerts"] = newd

# ----------------------------
# Resilient HTTP helpers (retry/backoff)
# ----------------------------


def _request_with_retry(
    method: str,
    url: str,
    *,
    headers: dict | None = None,
    json_body: dict | None = None,
    params: dict | None = None,
    timeout_s: int = 45,
    max_attempts: int = 6,
    retry_statuses: tuple[int, ...] = (429, 502, 503, 504),
    backoff_initial_s: int = 2,
    backoff_max_s: int = 60,
):
    attempt = 0
    backoff = backoff_initial_s

    while True:
        attempt += 1
        r = requests.request(
            method,
            url,
            headers=headers,
            json=json_body,
            params=params,
            timeout=timeout_s,
        )

        if r.status_code in retry_statuses:
            if attempt >= max_attempts:
                try:
                    print(
                        f"[ERRO-HTTP] {r.status_code} {method} {url} -> {json.dumps(r.json(), ensure_ascii=False)}"
                    )
                except Exception:
                    print(f"[ERRO-HTTP] {r.status_code} {method} {url} -> {r.text}")
                r.raise_for_status()

            retry_after = r.headers.get("Retry-After")
            if retry_after and str(retry_after).isdigit():
                wait_s = int(retry_after)
            else:
                wait_s = backoff

            print(
                f"[WARN] HTTP {r.status_code} {method} {url} (tentativa {attempt}/{max_attempts}). Aguardando {wait_s}s."
            )
            time.sleep(wait_s)
            backoff = min(backoff * 2, backoff_max_s)
            continue

        if r.status_code >= 400:
            try:
                print(
                    f"[ERRO-HTTP] {r.status_code} {method} {url} -> {json.dumps(r.json(), ensure_ascii=False)}"
                )
            except Exception:
                print(f"[ERRO-HTTP] {r.status_code} {method} {url} -> {r.text}")

        r.raise_for_status()
        return r


# ----------------------------
# Email (SMTP) with CC
# ----------------------------


def send_email_smtp(to_addr: str, cc_addrs: list[str], subject: str, html: str) -> None:
    host = os.environ["SMTP_HOST"]
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ["SMTP_USERNAME"]
    pwd = os.environ["SMTP_PASSWORD"]
    from_addr = os.environ.get("SMTP_FROM", user)

    msg = MIMEText(html, "html", "utf-8")
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_addr
    if cc_addrs:
        msg["Cc"] = "; ".join(cc_addrs)

    recipients = [to_addr] + (cc_addrs or [])

    server = smtplib.SMTP(host, port, timeout=30)
    try:
        server.starttls()
        server.login(user, pwd)
        server.sendmail(from_addr, recipients, msg.as_string())
    finally:
        server.quit()


def _fmt_duration(minutos: int) -> str:
    # 90 -> "1h30"
    h = int(minutos) // 60
    m = int(minutos) % 60
    if h <= 0:
        return f"{m} min"
    if m == 0:
        return f"{h}h"
    return f"{h}h{m:02d}"


def format_email(
    nome_cliente: str,
    projeto: str,
    tecnico: str,
    tipo: str,
    data_inicio: datetime,
    data_fim: datetime,
    minutos_arred: int,
    kms: float,
    descricao: str,
    contrato_info: str = "",
    tipo_cobranca: str = "",
) -> str:
    # Mostramos só a data para evitar confusão com arredondamentos
    data_str = data_inicio.astimezone().strftime("%d/%m/%Y")

    kms_html = ""
    if tipo == "Local":
        kms_html = f"Kms: {kms:.1f} km<br>"

    desc_html = (descricao or "").replace("\r\n", "\n").replace("\n", "<br>")
    cobranca_html = ""
    if tipo_cobranca:
        cobranca_html = f"<br>Tipo de cobrança: {tipo_cobranca}<br>"

    contrato_html = ""
    if contrato_info:
        contrato_html = f"<br>Contrato: {contrato_info}<br>"

    duracao_str = _fmt_duration(minutos_arred)

    return f"""
<br>
Olá {nome_cliente},<br><br>

Segue o resumo da assistência:<br><br>

Projeto: {projeto}<br>
Técnico: {tecnico}<br>
Tipo: {tipo}<br>
Data: {data_str}<br>
Duração: {duracao_str}<br>
{kms_html}
{cobranca_html}
{contrato_html}
<br>
Descrição:<br><br>
{desc_html}
"""


# ----------------------------
# Clockify APIs
# ----------------------------


def clockify_fetch_entries(
    api_key: str, workspace_id: str, start_dt: datetime, end_dt: datetime
) -> list[dict]:
    url = CLOCKIFY_DETAILED_REPORT_URL.format(wid=workspace_id)
    headers = {"X-Api-Key": api_key, "Content-Type": "application/json"}

    page = 1
    page_size = 500
    out: list[dict] = []

    while True:
        body = {
            "dateRangeStart": iso_z(start_dt),
            "dateRangeEnd": iso_z(end_dt),
            "detailedFilter": {"page": page, "pageSize": page_size},
        }
        r = _request_with_retry(
            "POST",
            url,
            headers=headers,
            json_body=body,
            timeout_s=45,
            max_attempts=6,
            retry_statuses=(429, 502, 503, 504),
        )
        data = r.json()
        items = data.get("timeentries", []) or []
        out.extend(items)

        if len(items) < page_size:
            break
        page += 1

    return out


def clockify_list_clients(api_key: str, workspace_id: str, api_base: str) -> list[dict]:
    url = f"{api_base}/workspaces/{workspace_id}/clients"
    headers = {"X-Api-Key": api_key}
    r = _request_with_retry(
        "GET",
        url,
        headers=headers,
        timeout_s=45,
        max_attempts=6,
        retry_statuses=(429, 502, 503, 504),
    )
    return r.json() or []


def get_entry_id(entry: dict) -> str:
    for k in ("id", "_id", "timeEntryId", "timeentryId", "uid"):
        v = entry.get(k)
        if v:
            return str(v).strip()

    ti = entry.get("timeInterval") or {}
    start = ti.get("start") or ""
    end = ti.get("end") or ""
    user_id = entry.get("userId") or ""
    proj_id = entry.get("projectId") or ""
    desc = (entry.get("description") or "").strip()
    if start and end and (user_id or proj_id or desc):
        return f"synthetic|{user_id}|{start}|{end}|{proj_id}|{desc}"
    return ""


def tag_names(entry: dict) -> list[str]:
    tags = entry.get("tags") or []
    if not tags:
        return []
    if isinstance(tags, list):
        if tags and isinstance(tags[0], str):
            return [str(t).strip() for t in tags if str(t).strip()]
        if tags and isinstance(tags[0], dict):
            out = []
            for t in tags:
                name = (t.get("name") or "").strip()
                if name:
                    out.append(name)
            return out
    return []


def get_client(entry: dict) -> tuple[str, str]:
    c = entry.get("client")
    if isinstance(c, dict):
        cid = (c.get("id") or "").strip()
        cname = (c.get("name") or "").strip()
        if cid:
            return cid, cname

    cid = (entry.get("clientId") or "").strip()
    cname = (entry.get("clientName") or "").strip()
    return cid, cname


def get_project_name(entry: dict) -> str:
    p = entry.get("project")
    if isinstance(p, dict):
        name = (p.get("name") or "").strip()
        if name:
            return name
    return (entry.get("projectName") or "").strip()


def get_user_name(entry: dict) -> str:
    u = entry.get("user")
    if isinstance(u, dict):
        name = (u.get("name") or "").strip()
        if name:
            return name
    return (entry.get("userName") or "").strip()


def get_time_interval(entry: dict) -> tuple[datetime | None, datetime | None, int]:
    ti = entry.get("timeInterval") or {}
    s = parse_dt(ti.get("start"))
    e = parse_dt(ti.get("end"))
    dur = ti.get("duration")
    dur_min = int(dur) // 60 if dur is not None else 0
    return s, e, dur_min


# ----------------------------
# Microsoft Graph / SharePoint Lists
# ----------------------------


def graph_token() -> str:
    tenant_id = os.environ["M365_TENANT_ID"]
    client_id = os.environ["M365_CLIENT_ID"]
    client_secret = os.environ["M365_CLIENT_SECRET"]

    app = msal.ConfidentialClientApplication(
        client_id=client_id,
        client_credential=client_secret,
        authority=f"https://login.microsoftonline.com/{tenant_id}",
    )
    result = app.acquire_token_for_client(
        scopes=["https://graph.microsoft.com/.default"]
    )
    if "access_token" not in result:
        raise RuntimeError(f"Falha a obter token Graph: {result}")
    return result["access_token"]


def gh(token: str, prefer: str | None = None) -> dict:
    h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    if prefer:
        h["Prefer"] = prefer
    return h


def graph_request(
    method: str,
    url: str,
    token: str,
    *,
    json_body: dict | None = None,
    params: dict | None = None,
    prefer: str | None = None,
):
    return _request_with_retry(
        method,
        url,
        headers=gh(token, prefer=prefer),
        json_body=json_body,
        params=params,
        timeout_s=45,
        max_attempts=6,
        retry_statuses=(429, 502, 503, 504),
    )


def get_site_id(sp_hostname: str, site_path: str, token: str) -> str:
    site_path = site_path.lstrip("/")
    url = f"{GRAPH}/sites/{sp_hostname}:/{site_path}"
    r = graph_request("GET", url, token)
    return r.json()["id"]


def get_list_id(site_id: str, list_name: str, token: str) -> str:
    url = f"{GRAPH}/sites/{site_id}/lists?$select=id,displayName"
    r = graph_request("GET", url, token)
    for lst in r.json().get("value", []):
        if lst.get("displayName") == list_name:
            return lst["id"]
    raise RuntimeError(f"Lista não encontrada: {list_name}")


def list_items(
    site_id: str, list_id: str, token: str, select_fields: str
) -> list[dict]:
    url = f"{GRAPH}/sites/{site_id}/lists/{list_id}/items?expand=fields($select={select_fields})"
    items: list[dict] = []
    while url:
        r = graph_request("GET", url, token)
        data = r.json()
        items.extend(data.get("value", []))
        url = data.get("@odata.nextLink")
    return items


def refresh_assist_items(site_id: str, list_assist_id: str, token: str) -> list[dict]:
    return list_items(
        site_id,
        list_assist_id,
        token,
        "Title,ClockifyClientId,GrupoContrato,DescontaContrato,EmailEnviado,DataHoraInicio,DataHoraFim,MinutosArredondados",
    )


def create_item(site_id: str, list_id: str, token: str, fields: dict) -> dict:
    url = f"{GRAPH}/sites/{site_id}/lists/{list_id}/items"
    payload = {"fields": fields}
    r = graph_request("POST", url, token, json_body=payload)
    return r.json()


# CORREÇÃO: só argumentos nomeados
def update_item_fields(
    *, site_id: str, list_id: str, item_id: str, token: str, fields: dict
) -> dict:
    url = f"{GRAPH}/sites/{site_id}/lists/{list_id}/items/{item_id}/fields"
    r = graph_request(
        "PATCH", url, token, json_body=fields, prefer="return=representation"
    )
    return r.json() or {}


def list_assistencias_pendentes(
    site_id: str, list_id: str, token: str, max_items: int = 200
) -> list[dict]:
    """
    Lê itens recentes e filtra localmente (não usa $filter em campo não indexado).
    Só devolve itens que ainda NÃO foram enviados.
    """
    url = (
        f"{GRAPH}/sites/{site_id}/lists/{list_id}/items"
        f"?$top={max_items}"
        f"&$orderby=createdDateTime desc"
        f"&expand=fields($select="
        f"Title,ClockifyClientId,ClockifyProjectId,GrupoContrato,TipoCobranca,"
        f"NomeCliente,Projeto,Tecnico,Tipo,DataHoraInicio,DataHoraFim,"
        f"MinutosArredondados,Kms,Descricao,NomeContrato,EmailEnviado,DataHoraEmail)"
    )

    r = graph_request("GET", url, token)
    data = r.json() or {}
    items = data.get("value", []) or []

    pendentes: list[dict] = []
    for it in items:
        f = it.get("fields", {}) or {}

        # Em SharePoint/Graph boolean vem como True/False
        if f.get("EmailEnviado") is True:
            continue

        # Se por algum motivo o boolean falhar mas a data existir, também não é pendente
        if (f.get("DataHoraEmail") or "").strip():
            continue

        pendentes.append(it)

    return pendentes


# ----------------------------
# Clientes sync
# ----------------------------


def sp_index_items_by_field(items: list[dict], field_name: str) -> dict[str, dict]:
    idx = {}
    for it in items:
        f = it.get("fields", {}) or {}
        key = (f.get(field_name) or "").strip()
        if key:
            idx[key] = it
    return idx


def sync_clientes_clockify_to_sharepoint(
    token: str,
    site_id: str,
    list_clientes_id: str,
    api_key: str,
    workspace_id: str,
    api_base: str,
) -> None:
    clockify_clients = clockify_list_clients(api_key, workspace_id, api_base)

    sp_items = list_items(
        site_id,
        list_clientes_id,
        token,
        "Title,ClockifyClientId,Ativo,Contrato,ClockifyEmail,ClockifyCcEmails,Kms",
    )
    sp_by_cid = sp_index_items_by_field(sp_items, "ClockifyClientId")

    created = 0
    updated = 0

    for c in clockify_clients:
        cid = (c.get("id") or "").strip()
        name = (c.get("name") or "").strip()
        if not cid or not name:
            continue

        clockify_email = (c.get("email") or "").strip()

        cc = c.get("ccEmails") or []
        if isinstance(cc, list):
            cc_list = [x.strip() for x in cc if isinstance(x, str) and x.strip()]
        else:
            cc_list = [str(cc).strip()] if str(cc).strip() else []
        clockify_cc = ";".join(cc_list)

        if cid not in sp_by_cid:
            fields = {
                "Title": name,
                "ClockifyClientId": cid,
                "ClockifyEmail": clockify_email,
                "ClockifyCcEmails": clockify_cc,
                "Ativo": True,
                "Contrato": False,
            }
            create_item(site_id, list_clientes_id, token, fields)
            created += 1
        else:
            it = sp_by_cid[cid]
            f = it.get("fields", {}) or {}

            patch = {}
            if (f.get("Title") or "").strip() != name:
                patch["Title"] = name
            if (f.get("ClockifyEmail") or "").strip() != clockify_email:
                patch["ClockifyEmail"] = clockify_email
            if (f.get("ClockifyCcEmails") or "").strip() != clockify_cc:
                patch["ClockifyCcEmails"] = clockify_cc

            if patch:
                update_item_fields(
                    site_id=site_id,
                    list_id=list_clientes_id,
                    item_id=normalize_item_id(it["id"]),
                    token=token,
                    fields=patch,
                )
                updated += 1

    print(
        f"[INFO] Sync clientes Clockify -> SharePoint: criados={created}, atualizados={updated}, total_clockify={len(clockify_clients)}."
    )


# ----------------------------
# Matching logic
# ----------------------------

def contrato_clockify_client_id(
    contrato_fields: dict, clientes_by_item_id: dict[str, dict]
) -> str:
    lookup_id = contrato_fields.get("NomeClienteLookupId")
    if not lookup_id:
        return ""

    cliente = clientes_by_item_id.get(str(lookup_id))
    if not cliente:
        return ""

    return (cliente.get("ClockifyClientId") or "").strip()


def find_cliente(clientes_items: list[dict], clockify_client_id: str) -> dict | None:
    for it in clientes_items:
        f = it.get("fields", {}) or {}
        if (f.get("ClockifyClientId") or "").strip() != clockify_client_id.strip():
            continue
        if f.get("Ativo") is False:
            return None
        return f
    return None


def contract_group_key(f: dict, item_id: str | None = None) -> str:
    g_raw = (f.get("GrupoContrato") or "").strip()
    g = normalize_group_key(g_raw)
    if g:
        return g

    # fallback: sem GrupoContrato, cada linha é o seu próprio "grupo"
    if item_id:
        return f"item:{str(item_id).strip()}"

    return normalize_group_key((f.get("Title") or "").strip()) or "item:unknown"

def normalize_contratos_grupocontrato(
    *,
    site_id: str,
    list_contratos_id: str,
    token: str,
    contratos_items: list[dict],
) -> int:
    """
    Normaliza o campo GrupoContrato na lista Contratos (PATCH) quando necessário.
    Devolve quantos itens foram atualizados.
    """
    updated = 0

    for it in contratos_items:
        item_id = normalize_item_id(it.get("id"))
        f = it.get("fields", {}) or {}

        raw = (f.get("GrupoContrato") or "").strip()
        if not raw:
            continue  # por defeito vazio, como queres

        norm = normalize_group_key(raw)
        if not norm or norm == raw:
            continue  # já está ok

        update_item_fields(
            site_id=site_id,
            list_id=list_contratos_id,
            item_id=item_id,
            token=token,
            fields={"GrupoContrato": norm},
        )
        updated += 1

    if updated:
        print(f"[INFO] Normalização GrupoContrato (Contratos): atualizados={updated}")

    return updated

def normalize_group_key(v: str) -> str:
    """
    Normaliza a chave do GrupoContrato para reduzir erros humanos.
    Ex:
      'Anglotex 2026' -> 'anglotex2026'
      'anglotex-2026' -> 'anglotex2026'
    """
    s = (v or "").strip().lower()
    if not s:
        return ""
    # remove espaços, hífens e underscores (podes ajustar se quiseres manter algum)
    s = re.sub(r"[\s\-_]+", "", s)
    return s

def find_contract_group_for_entry(
    contratos_items: list[dict],
    clientes_by_item_id: dict[str, dict],
    clockify_client_id: str,
    clockify_project_id: str,
    assist_start: datetime,
) -> tuple[str, datetime | None, datetime | None, list[dict]]:

    """
    Devolve (grupo, di, df, linhas_do_grupo)
    Regras:
      - Ativo true
      - DataInicio/DataFim contêm assist_start
      - Preferir contratos com ClockifyProjectId == clockify_project_id
      - Se ClockifyProjectId vazio, considera “genérico”
    """
    candidates: list[dict] = []
    for it in contratos_items:
        f = it.get("fields", {}) or {}
        contrato_cid = contrato_clockify_client_id(f, clientes_by_item_id)
        if contrato_cid != clockify_client_id.strip():
            continue

        if f.get("Ativo") is not True:
            continue

        di = parse_dt(f.get("DataInicio"))
        df = parse_dt(f.get("DataFim"))
        if not di or not df:
            continue

        if not (di <= assist_start <= (df + timedelta(days=1) - timedelta(seconds=1))):
            continue

        pid = (f.get("ClockifyProjectId") or "").strip()
        candidates.append({"fields": f, "item": it, "di": di, "df": df, "pid": pid})

    if not candidates:
        return "", None, None, []

    # Preferência 1: match exato por projectId
    exact = [c for c in candidates if c["pid"] and c["pid"] == clockify_project_id]
    pool = exact if exact else candidates

    # Agrupar por GrupoContrato
    groups: dict[str, list[dict]] = {}
    for c in pool:
        g = contract_group_key(c["fields"], c["item"].get("id"))
        groups.setdefault(g, []).append(c)


    if len(groups) == 1:
        g = next(iter(groups.keys()))
        rows = groups[g]
        di = min(r["di"] for r in rows)
        df = max(r["df"] for r in rows)
        return g, di, df, [r["item"] for r in rows]

    # Ambíguo: escolhe grupo com DataInicio mais recente (e loga)
    best_g = None
    best_di = None
    for g, rows in groups.items():
        di = min(r["di"] for r in rows)
        if best_di is None or di > best_di:
            best_di = di
            best_g = g

    rows = groups[best_g]
    di = min(r["di"] for r in rows)
    df = max(r["df"] for r in rows)
    print(
        f"[WARN] Vários contratos candidatos. Escolhido grupo={best_g} por DataInicio mais recente."
    )
    return best_g, di, df, [r["item"] for r in rows]


def sum_contract_total_minutes(contract_group_items: list[dict]) -> int:
    total = 0
    for it in contract_group_items:
        f = it.get("fields", {}) or {}
        horas = float(f.get("HorasContratadas") or 0)
        total += int(round(horas * 60))
    return total


def sum_used_minutes_for_group(
    assist_items: list[dict],
    clockify_client_id: str,
    group_key: str,
    di: datetime,
    df: datetime,
) -> int:
    total = 0
    for it in assist_items:
        f = it.get("fields", {}) or {}
        if (f.get("ClockifyClientId") or "").strip() != clockify_client_id.strip():
            continue
        if normalize_group_key(f.get("GrupoContrato")) != normalize_group_key(group_key):
            continue

        if f.get("DescontaContrato") is not True:
            continue

        s = parse_dt(f.get("DataHoraInicio"))
        if not s:
            continue
        if not (di <= s <= df):
            continue

        total += int(f.get("MinutosArredondados") or 0)
    return total


def update_contract_group_audit(
    *,
    site_id: str,
    list_contratos_id: str,
    token: str,
    contract_group_items: list[dict],
    used_min: int,
    total_min: int,
) -> None:
    rem_min = max(0, total_min - used_min)
    horas_disp = round(rem_min / 60.0, 2)

    patch = {
        "MinutosUsados": int(used_min),
        "MinutosDisponiveis": int(rem_min),
        "HorasDisponiveis": float(horas_disp),
    }

    for it in contract_group_items:
        item_id = normalize_item_id(it.get("id"))
        update_item_fields(
            site_id=site_id,
            list_id=list_contratos_id,
            item_id=item_id,
            token=token,
            fields=patch,
        )


def find_contrato_ativo_record(
    contratos_items: list[dict],
    clientes_by_item_id: dict[str, dict],
    clockify_client_id: str,
    assist_start: datetime,
) -> dict | None:
    candidatos: list[dict] = []
    target = clockify_client_id.strip()

    for it in contratos_items:
        f = it.get("fields", {}) or {}

        contrato_cid = contrato_clockify_client_id(f, clientes_by_item_id)
        if contrato_cid != target:
            continue

        if f.get("Ativo") is not True:
            continue

        di = parse_dt(f.get("DataInicio"))
        df = parse_dt(f.get("DataFim"))
        if not di or not df:
            continue

        if di <= assist_start <= (df + timedelta(days=1) - timedelta(seconds=1)):
            candidatos.append(f)

    if not candidatos:
        return None

    candidatos.sort(key=lambda x: (x.get("DataInicio") or ""))
    return candidatos[-1]


def sum_used_minutes_for_contract(
    assist_items: list[dict], clockify_client_id: str, di: datetime, df: datetime
) -> int:
    total = 0
    for it in assist_items:
        f = it.get("fields", {}) or {}

        if (f.get("ClockifyClientId") or "").strip() != clockify_client_id.strip():
            continue

        # Só conta se já foi “fechada” (email enviado)
        if f.get("EmailEnviado") is not True:
            continue

        s = parse_dt(f.get("DataHoraInicio"))
        if not s:
            continue

        if not (di <= s <= df):
            continue

        total += int(f.get("MinutosArredondados") or 0)

    return total


def update_contrato_auditoria(
    site_id: str,
    list_contratos_id: str,
    token: str,
    contratos_items: list[dict],
    clientes_by_item_id: dict[str, dict],
    assist_items: list[dict],
    clockify_client_id: str,
    assist_start: datetime,
) -> None:
    contrato_item = None
    target = clockify_client_id.strip()

    for it in contratos_items:
        f = it.get("fields", {}) or {}

        contrato_cid = contrato_clockify_client_id(f, clientes_by_item_id)
        if contrato_cid != target:
            continue

        if f.get("Ativo") is not True:
            continue

        di = parse_dt(f.get("DataInicio"))
        df = parse_dt(f.get("DataFim"))
        if not di or not df:
            continue

        if di <= assist_start <= (df + timedelta(days=1) - timedelta(seconds=1)):
            contrato_item = it

    if not contrato_item:
        return


    f = contrato_item.get("fields", {}) or {}
    contrato_item_id = normalize_item_id(contrato_item.get("id"))

    di = parse_dt(f.get("DataInicio"))
    df = parse_dt(f.get("DataFim"))
    horas = float(f.get("HorasContratadas") or 0)

    if not di or not df or horas <= 0:
        return

    total_min = int(horas * 60)
    used_min = sum_used_minutes_for_contract(assist_items, clockify_client_id, di, df)
    rem_min = max(0, total_min - used_min)

    patch = {
        "MinutosUsados": int(used_min),
        "MinutosDisponiveis": int(rem_min),
    }

    # Se tiveres esta coluna criada
    patch["DataHoraAuditoria"] = iso_z(utc_now())

    update_item_fields(
        site_id=site_id,
        list_id=list_contratos_id,
        item_id=contrato_item_id,
        token=token,
        fields=patch,
    )

    print(
        "[INFO] Auditoria contrato atualizada:",
        {
            "ClockifyClientId": clockify_client_id,
            "Contrato": (f.get("Title") or "").strip(),
            "MinutosUsados": used_min,
            "MinutosDisponiveis": rem_min,
        },
    )


def already_exists_assistencia(
    assist_items: list[dict], clockify_assist_id: str
) -> dict | None:
    for it in assist_items:
        f = it.get("fields", {}) or {}
        if (f.get("Title") or "").strip() == str(clockify_assist_id).strip():
            return it
    return None


def pick_nearest_deslocacao(
    deslocacoes: list[dict],
    clockify_client_id: str,
    tecnico: str,
    assist_start: datetime,
    used_ids: set[str],
    window_minutes: int,
) -> dict | None:
    best = None
    best_diff = None

    for d in deslocacoes:
        did = get_entry_id(d)
        if not did or did in used_ids:
            continue

        cid, _ = get_client(d)
        if cid.strip() != clockify_client_id.strip():
            continue

        if tecnico and get_user_name(d) and get_user_name(d) != tecnico:
            continue

        sd, _, _ = get_time_interval(d)
        if not sd:
            continue

        diff = minutes_diff(sd, assist_start)
        if diff <= window_minutes and (best_diff is None or diff < best_diff):
            best = d
            best_diff = diff

    if best:
        used_ids.add(get_entry_id(best))
    return best


# ----------------------------
# Main loop
# ----------------------------


def main():
    cfg = load_json(CFG_PATH)
    state = load_json(STATE_PATH) if os.path.exists(STATE_PATH) else {}

    clockify_api_key = os.environ["CLOCKIFY_API_KEY"]
    clockify_workspace_id = os.environ["CLOCKIFY_WORKSPACE_ID"]

    sp_hostname = cfg["m365"]["sharepoint_hostname"]
    site_path = cfg["m365"]["site_path"]

    list_clientes_name = cfg["m365"]["list_clientes"]
    list_contratos_name = cfg["m365"]["list_contratos"]
    list_assist_name = cfg["m365"]["list_assistencias"]

    loop_seconds = int(cfg.get("loop_seconds", 300))
    lookback_hours = int(cfg["clockify"].get("lookback_hours", 48))

    tag_ass = cfg["clockify"]["tag_assistencia"]
    tag_des = cfg["clockify"]["tag_deslocacao"]
    tag_remota = cfg["clockify"]["tag_remota"]
    tag_local = cfg["clockify"]["tag_local"]
    tag_garantia = cfg["clockify"]["tag_garantia"]
    tag_faturar = cfg["clockify"]["tag_faturar"]
    tag_contrato = cfg["clockify"]["tag_contrato"]

    block_remota = int(cfg["clockify"]["billing_block_minutes_remota"])
    block_local = int(cfg["clockify"]["billing_block_minutes_local"])
    window = int(cfg["clockify"].get("assoc_window_minutes", 240))

    clients_sync_minutes = int(cfg.get("clients_sync_minutes", 30))
    clockify_api_base = cfg.get("clockify_api_base", "https://api.clockify.me/api/v1")

    debug_clockify = env_flag("DEBUG_CLOCKIFY", "0")

    print("[INFO] Serviço iniciado.")

    while True:
        try:
            state_prune_processed(state, keep_days=7)
            state_prune_sent_emails(state, keep_days=90)

            token = graph_token()
            site_id = get_site_id(sp_hostname, site_path, token)

            list_clientes_id = get_list_id(site_id, list_clientes_name, token)
            list_contratos_id = get_list_id(site_id, list_contratos_name, token)
            list_assist_id = get_list_id(site_id, list_assist_name, token)

            last_sync = (
                parse_dt(state.get("last_clients_sync_utc"))
                if state.get("last_clients_sync_utc")
                else None
            )
            need_sync = (last_sync is None) or (
                (utc_now() - last_sync).total_seconds() >= clients_sync_minutes * 60
            )

            if need_sync:
                sync_clientes_clockify_to_sharepoint(
                    token=token,
                    site_id=site_id,
                    list_clientes_id=list_clientes_id,
                    api_key=clockify_api_key,
                    workspace_id=clockify_workspace_id,
                    api_base=clockify_api_base,
                )
                state["last_clients_sync_utc"] = iso_z(utc_now())
                save_json(STATE_PATH, state)

            clientes_items = list_items(
                site_id,
                list_clientes_id,
                token,
                "Title,ClockifyClientId,Ativo,Contrato,ClockifyEmail,ClockifyCcEmails,Kms",
            )

            # Indexar clientes por item id (para resolver lookups dos contratos)
            clientes_by_item_id = {
                normalize_item_id(it["id"]): (it.get("fields", {}) or {})
                for it in clientes_items
            }

            contratos_items = list_items(
                site_id,
                list_contratos_id,
                token,
                "Title,NomeClienteLookupId,ClockifyProjectId,GrupoContrato,DataInicio,DataFim,HorasContratadas,Ativo,MinutosUsados,MinutosDisponiveis,HorasDisponiveis",
            )
            # Normalizar visualmente o GrupoContrato na lista Contratos
            changed = normalize_contratos_grupocontrato(
                site_id=site_id,
                list_contratos_id=list_contratos_id,
                token=token,
                contratos_items=contratos_items,
            )

            if changed:
                # reler para garantir que o resto do ciclo já usa os valores normalizados
                contratos_items = list_items(
                    site_id,
                    list_contratos_id,
                    token,
                    "Title,NomeClienteLookupId,ClockifyProjectId,GrupoContrato,DataInicio,DataFim,HorasContratadas,Ativo,MinutosUsados,MinutosDisponiveis,HorasDisponiveis",
                )
            assist_items = list_items(
                site_id,
                list_assist_id,
                token,
                "Title,ClockifyClientId,ClockifyProjectId,GrupoContrato,DescontaContrato,EmailEnviado,DataHoraInicio,DataHoraFim,MinutosArredondados",
            )

            # Enviar emails pendentes e marcar colunas
            pendentes = list_assistencias_pendentes(site_id, list_assist_id, token)

            if pendentes:
                print(f"[INFO] Assistências pendentes de email: {len(pendentes)}")

            for it in pendentes:
                f = it.get("fields", {}) or {}
                item_id = normalize_item_id(it.get("id"))

                tipo_cobranca = (f.get("TipoCobranca") or "").strip()

                # LOCK: se já enviaste recentemente este item, não reenviar
                if state_is_email_sent_recent(state, item_id, within_days=30):
                    continue

                assist_id = (f.get("Title") or "").strip()
                clockify_client_id = (f.get("ClockifyClientId") or "").strip()

                cliente_sp = find_cliente(clientes_items, clockify_client_id)
                if not cliente_sp:
                    continue

                to_addr = (cliente_sp.get("ClockifyEmail") or "").strip()
                if not to_addr:
                    continue

                cc_addrs = split_emails(
                    (cliente_sp.get("ClockifyCcEmails") or "").strip()
                )

                tipo_txt = (f.get("Tipo") or "").strip()
                nome_cliente = (
                    f.get("NomeCliente") or cliente_sp.get("Title") or ""
                ).strip()
                projeto = (f.get("Projeto") or "").strip()
                tecnico = (f.get("Tecnico") or "").strip()
                descricao = (f.get("Descricao") or "").strip()

                assist_start = parse_dt(f.get("DataHoraInicio"))
                assist_end = parse_dt(f.get("DataHoraFim"))
                if not assist_start or not assist_end:
                    continue

                min_arred = int(f.get("MinutosArredondados") or 0)
                kms = float(f.get("Kms") or 0.0)

                # Informação de contrato no email (apenas quando TipoCobranca == "Contrato")
                contrato_info = ""

                if tipo_cobranca == "Contrato":
                    # opcional: validar também que o cliente está marcado como contrato no SP
                    if cliente_sp.get("Contrato") is True:
                        c = find_contrato_ativo_record(
                            contratos_items, clientes_by_item_id, clockify_client_id, assist_start
                        )
                        if c:
                            nome_contrato = (c.get("Title") or "").strip()
                            di = parse_dt(c.get("DataInicio"))
                            df = parse_dt(c.get("DataFim"))
                            horas = float(c.get("HorasContratadas") or 0)

                            if di and df and horas > 0:
                                used_min = sum_used_minutes_for_contract(
                                    assist_items, clockify_client_id, di, df
                                )
                                total_min = int(horas * 60)
                                rem_min = max(0, total_min - used_min)

                                contrato_info = (
                                    f"{nome_contrato} "
                                    f"({horas:.1f} h contratadas, {used_min/60:.1f} h usadas, {rem_min/60:.1f} h disponíveis)"
                                )
                            else:
                                contrato_info = nome_contrato

                subject = f"Assistência {tipo_txt} {nome_cliente} {assist_start.astimezone().strftime('%d/%m/%Y')}"
                html = format_email(
                    nome_cliente,
                    projeto,
                    tecnico,
                    tipo_txt,
                    assist_start,
                    assist_end,
                    min_arred,
                    kms,
                    descricao,
                    contrato_info=contrato_info,
                    tipo_cobranca=tipo_cobranca,
                )

                # 1) Envia email
                send_email_smtp(to_addr, cc_addrs, subject, html)

                # 2) Marca item no SharePoint
                patched = update_item_fields(
                    site_id=site_id,
                    list_id=list_assist_id,
                    item_id=item_id,
                    token=token,
                    fields={"EmailEnviado": True, "DataHoraEmail": iso_z(utc_now())},
                )

                print(
                    "[INFO] Email pendente enviado e marcado como enviado:",
                    {
                        "ClockifyId": assist_id,
                        "item_id": item_id,
                        "EmailEnviado": patched.get("EmailEnviado"),
                        "DataHoraEmail": patched.get("DataHoraEmail"),
                    },
                )

                # 3) Se ficou marcado, faz LOCK e auditoria
                if patched.get("EmailEnviado") is True:
                    state_mark_email_sent(state, item_id)
                    save_json(STATE_PATH, state)

                    # refresh para auditoria correta (incluir este item já fechado)
                    assist_items = refresh_assist_items(site_id, list_assist_id, token)

                    tipo_cobranca = (f.get("TipoCobranca") or "").strip()
                    grupo_contrato = (f.get("GrupoContrato") or "").strip()
                    clockify_project_id = (f.get("ClockifyProjectId") or "").strip()

                    if tipo_cobranca == "Contrato" and grupo_contrato:
                        # Reencontrar o grupo para obter as linhas do contrato (base + topups) e datas
                        grupo, di, df, group_items = find_contract_group_for_entry(
                            contratos_items=contratos_items,
                            clientes_by_item_id=clientes_by_item_id,
                            clockify_client_id=clockify_client_id,
                            clockify_project_id=clockify_project_id,
                            assist_start=assist_start,
                        )
                        if grupo and di and df and group_items:
                            total_min = sum_contract_total_minutes(group_items)
                            used_min = sum_used_minutes_for_group(
                                assist_items=assist_items,
                                clockify_client_id=clockify_client_id,
                                group_key=grupo,
                                di=di,
                                df=df,
                            )
                            update_contract_group_audit(
                                site_id=site_id,
                                list_contratos_id=list_contratos_id,
                                token=token,
                                contract_group_items=group_items,
                                used_min=used_min,
                                total_min=total_min,
                            )

            end_dt = utc_now()
            start_dt = end_dt - timedelta(hours=lookback_hours)

            entries = clockify_fetch_entries(
                clockify_api_key, clockify_workspace_id, start_dt, end_dt
            )

            if debug_clockify and entries:
                s = entries[0]
                print("[DEBUG] Clockify sample keys:", sorted(list(s.keys())))
                print(
                    "[DEBUG] Clockify sample id candidates:",
                    {
                        "id": s.get("id"),
                        "_id": s.get("_id"),
                        "timeEntryId": s.get("timeEntryId"),
                        "clientId": s.get("clientId"),
                        "projectId": s.get("projectId"),
                        "userId": s.get("userId"),
                        "tags": s.get("tags"),
                        "tagIds": s.get("tagIds"),
                    },
                )

            assistencias = []
            deslocacoes = []
            for e in entries:
                tags = tag_names(e)
                if tag_des in tags:
                    deslocacoes.append(e)
                if tag_ass in tags:
                    assistencias.append(e)

            used_desloc_ids: set[str] = set()

            for a in assistencias:
                assist_id = get_entry_id(a)
                if not assist_id:
                    if debug_clockify:
                        print(
                            f"[WARN] Assistência sem ID (keys={sorted(list(a.keys()))}). Ignorando."
                        )
                    continue

                # cache local
                if state_is_processed_recent(state, assist_id, within_days=7):
                    continue

                if already_exists_assistencia(assist_items, assist_id):
                    state_mark_processed(state, assist_id)
                    continue

                tags = tag_names(a)
                is_local = tag_local in tags
                is_remota = tag_remota in tags
                is_contrato = tag_contrato in tags
                is_garantia = tag_garantia in tags
                is_faturar = tag_faturar in tags

                if not is_local and not is_remota:
                    continue

                billing_flags = [is_contrato, is_faturar, is_garantia]
                if sum(1 for x in billing_flags if x) != 1:
                    print(
                        f"[WARN] Assistência {assist_id} sem (ou com múltiplas) tags de cobrança. "
                        f"Obrigatório: {tag_contrato} OU {tag_faturar} OU {tag_garantia}. Ignorando."
                    )
                    continue

                assist_start, assist_end, min_real = get_time_interval(a)
                if not assist_start or not assist_end:
                    continue

                clockify_client_id, client_name = get_client(a)
                if not clockify_client_id:
                    if debug_clockify:
                        print(
                            f"[WARN] Assistência {assist_id} sem clientId. Ignorando."
                        )
                    continue

                clockify_project_id = (a.get("projectId") or "").strip()

                cliente_sp = find_cliente(clientes_items, clockify_client_id)
                if not cliente_sp:
                    continue

                to_addr = (cliente_sp.get("ClockifyEmail") or "").strip()
                if not to_addr:
                    continue

                cc_addrs = split_emails(
                    (cliente_sp.get("ClockifyCcEmails") or "").strip()
                )

                tecnico = get_user_name(a) or ""
                projeto = get_project_name(a) or ""
                descricao = a.get("description") or ""

                block = block_local if is_local else block_remota
                min_arred = round_up_to_block(min_real, block)

                kms = 0.0
                min_desloc_real = 0

                if is_local:
                    # 1) tenta obter kms da deslocação do Clockify (override)
                    best_desl = pick_nearest_deslocacao(
                        deslocacoes=deslocacoes,
                        clockify_client_id=clockify_client_id,
                        tecnico=tecnico,
                        assist_start=assist_start,
                        used_ids=used_desloc_ids,
                        window_minutes=window,
                    )
                    if best_desl:
                        kms = safe_float_km(best_desl.get("description"))
                        _, _, min_desloc_real = get_time_interval(best_desl)

                    # 2) se não veio kms do Clockify, usa kms base do cliente no SharePoint
                    if kms <= 0:
                        kms_base = cliente_sp.get("Kms")
                        kms_base_val = safe_float_km(kms_base)
                        if kms_base_val > 0:
                            kms = kms_base_val

                # ----------------------------
                # Tipo de cobrança + contrato por grupo (base + topups)
                # ----------------------------
                grupo_contrato = ""
                desconta_contrato = False
                tipo_cobranca = ""
                contrato_info = ""
                nome_contrato = ""

                if is_garantia:
                    tipo_cobranca = "Garantia"

                elif is_faturar:
                    tipo_cobranca = "Faturar"

                elif is_contrato:
                    tipo_cobranca = "Contrato"
                    # Contrato passa a ser explícito e obrigatório
                    if cliente_sp.get("Contrato") is not True:
                        print(
                            f"[WARN] Assistência {assist_id} marcada como Contrato mas cliente não está como Contrato no SP. Ignorando."
                        )
                        continue

                    grupo, di, df, group_items = find_contract_group_for_entry(
                        contratos_items=contratos_items,
                        clientes_by_item_id=clientes_by_item_id,
                        clockify_client_id=clockify_client_id,
                        clockify_project_id=clockify_project_id,
                        assist_start=assist_start,
                    )

                    if not (grupo and di and df and group_items):
                        print(
                            f"[WARN] Assistência {assist_id} marcada como Contrato mas não encontrei contrato válido (grupo/data/projeto). Ignorando."
                        )
                        continue

                    grupo_contrato = grupo
                    nome_contrato = (
                        group_items[0].get("fields", {}).get("Title") or ""
                    ).strip()
                    desconta_contrato = True

                    total_min = sum_contract_total_minutes(group_items)
                    used_min = sum_used_minutes_for_group(
                        assist_items=assist_items,
                        clockify_client_id=clockify_client_id,
                        group_key=grupo,
                        di=di,
                        df=df,
                    )
                    rem_min = max(0, total_min - used_min)
                    contrato_info = (
                        f"{nome_contrato} "
                        f"({total_min/60:.1f} h contratadas, {used_min/60:.1f} h usadas, {rem_min/60:.1f} h disponíveis)"
                    )

                tipo_txt = "Local" if is_local else "Remota"

                fields = {
                    "Title": str(assist_id),
                    "ClockifyClientId": clockify_client_id,
                    "ClockifyProjectId": clockify_project_id,
                    "NomeCliente": client_name or (cliente_sp.get("Title") or ""),
                    "Projeto": projeto,
                    "Tecnico": tecnico,
                    "Tipo": tipo_txt,
                    "DataHoraInicio": iso_z(assist_start),
                    "DataHoraFim": iso_z(assist_end),
                    "MinutosReais": int(min_real),
                    "MinutosArredondados": int(min_arred),
                    "MinutosDeslocacao": int(min_desloc_real),
                    "Kms": float(kms),
                    "NomeContrato": nome_contrato,
                    "EmailEnviado": False,
                    "Descricao": descricao,
                    "TipoCobranca": tipo_cobranca,
                    "GrupoContrato": grupo_contrato,
                    "DescontaContrato": bool(desconta_contrato),
                }

                created = create_item(site_id, list_assist_id, token, fields)
                item_id = normalize_item_id(created.get("id"))
                print(
                    f"[INFO] Criado item Assistencias (ClockifyId={assist_id}, item_id={item_id})."
                )

                subject = f"Assistência {tipo_txt} {fields['NomeCliente']} {assist_start.astimezone().strftime('%d/%m/%Y')}"
                html = format_email(
                    fields["NomeCliente"],
                    projeto,
                    tecnico,
                    tipo_txt,
                    assist_start,
                    assist_end,
                    min_arred,
                    kms,
                    descricao,
                    contrato_info=contrato_info,
                    tipo_cobranca=tipo_cobranca,
                )

                send_email_smtp(to_addr, cc_addrs, subject, html)

                patched = update_item_fields(
                    site_id=site_id,
                    list_id=list_assist_id,
                    item_id=item_id,
                    token=token,
                    fields={"EmailEnviado": True, "DataHoraEmail": iso_z(utc_now())},
                )
                print(
                    "[INFO] Email enviado e item atualizado:",
                    {
                        "ClockifyId": assist_id,
                        "item_id": item_id,
                        "EmailEnviado": patched.get("EmailEnviado"),
                        "DataHoraEmail": patched.get("DataHoraEmail"),
                    },
                )

                state_mark_processed(state, assist_id)

            state["last_run_utc"] = iso_z(utc_now())
            save_json(STATE_PATH, state)

        except Exception as ex:
            print(f"[ERRO] {ex}")

        time.sleep(loop_seconds)


if __name__ == "__main__":
    main()
