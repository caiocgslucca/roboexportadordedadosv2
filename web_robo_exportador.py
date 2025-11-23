# app_robo_exportador_multi.py
import os, json, threading, getpass, time, uuid, tkinter as tk, shutil
from tkinter import filedialog
from datetime import datetime, timedelta, time as dtime
from flask import Flask, request, redirect, url_for, render_template_string, jsonify
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException, StaleElementReferenceException
from collections import deque
from calendar import monthrange

APP = Flask(__name__)
CRED_FILE = "credenciais.txt"
ROBOS_FILE = "robos.json"
LOG_FILE = "Log"
KEY_SAP = "SAP_Path"
NEW_NAME = "Acompanhamento_Produção.xlsx"
MAX_TENTATIVAS = 10
SLEEP_ENTRE_TENTATIVAS = 12

if not os.path.exists(LOG_FILE):
    open(LOG_FILE, "w", encoding="utf-8").close()
if not os.path.exists(ROBOS_FILE):
    with open(ROBOS_FILE, "w", encoding="utf-8") as f:
        json.dump({"robos": []}, f, ensure_ascii=False, indent=2)

state = {"queue": {}, "scheduler_on": True, "fifo": deque(), "runner_on": True}

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"[{ts}] {msg}\n")
    except Exception:
        pass

def carregar_config():
    creds, paths = {}, {}
    if os.path.exists(CRED_FILE):
        with open(CRED_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split(",", 2)
                if len(parts) < 2:
                    continue
                if parts[0] == "PATH":
                    if len(parts) == 3:
                        paths[parts[1]] = parts[2]
                else:
                    if len(parts) == 3:
                        sistema, matricula, senha = parts
                        creds[sistema] = {"matricula": matricula, "senha": senha}
    return creds, paths

def salvar_config(creds, paths):
    try:
        with open(CRED_FILE, "w", encoding="utf-8") as f:
            for sistema, d in creds.items():
                mat = d.get("matricula", "")
                sen = d.get("senha", "")
                f.write(f"{sistema},{mat},{sen}\n")
            for key, path in paths.items():
                f.write(f"PATH,{key},{path}\n")
    except Exception as e:
        log(f"Erro salvar config: {e}")

def salvar_credenciais(matricula, senha):
    creds, paths = carregar_config()
    creds["SAP"] = {"matricula": matricula, "senha": senha}
    salvar_config(creds, paths)

def salvar_caminho(path_value):
    creds, paths = carregar_config()
    paths[KEY_SAP] = path_value
    salvar_config(creds, paths)

def ler_robos():
    try:
        with open(ROBOS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"robos": []}

def salvar_robos(data):
    with open(ROBOS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def _desktop():
    try:
        return os.path.join(os.path.expanduser("~"), "Desktop")
    except Exception:
        return os.getcwd()

def _snapshot(dirpath, ext_list=(".xlsx", ".xls")):
    res = {}
    try:
        for fname in os.listdir(dirpath):
            if fname.lower().endswith(ext_list) or fname.lower().endswith(".crdownload"):
                full = os.path.join(dirpath, fname)
                try:
                    res[full] = os.path.getmtime(full)
                except FileNotFoundError:
                    pass
    except FileNotFoundError:
        pass
    return res

def _tam_estavel(path):
    try:
        t1 = os.path.getsize(path)
        time.sleep(1)
        t2 = os.path.getsize(path)
        return t1 == t2
    except Exception:
        return False

def _esperar_inicio(download_dir, timeout=6):
    inicio = time.time()
    antes = _snapshot(download_dir)
    while time.time() - inicio < timeout:
        time.sleep(0.5)
        depois = _snapshot(download_dir)
        novos = [f for f in depois.keys() if f not in antes]
        if novos:
            return True
        antes = depois
    return False

def _esperar_download(download_dir, timeout=240):
    inicio = time.time()
    antes = _snapshot(download_dir)
    while time.time() - inicio < timeout:
        time.sleep(1)
        depois = _snapshot(download_dir)
        novos = [f for f in depois.keys() if f not in antes]
        if novos:
            finalizados = [f for f in novos if not f.lower().endswith(".crdownload")]
            if finalizados:
                candidato = sorted(finalizados, key=lambda p: depois[p], reverse=True)[0]
                if _tam_estavel(candidato):
                    return candidato
            else:
                for f in novos:
                    if f.lower().endswith(".crdownload"):
                        base = f[:-11]
                        if os.path.exists(base) and _tam_estavel(base):
                            return base
        antes = depois
    return None

def _scroll_into_view(driver, el):
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    except Exception:
        pass

def _aguarda_ajax(driver, timeout=120):
    fim = time.time() + timeout
    while time.time() < fim:
        try:
            a = driver.execute_script("return (window.PrimeFaces ? PrimeFaces.ajax.Queue.isEmpty() : true);")
            b = driver.execute_script("return (window.jQuery ? jQuery.active === 0 : true);")
            if a and b:
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False

def _clicar(driver, wait, by, selector):
    elem = wait.until(EC.element_to_be_clickable((by, selector)))
    try:
        elem.click()
    except (ElementClickInterceptedException, StaleElementReferenceException):
        try:
            ActionChains(driver).move_to_element(elem).click().perform()
        except Exception:
            driver.execute_script("arguments[0].click();", elem)

def _clicar_alerta_relatorio(driver, timeout=120):
    try:
        alerta = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.XPATH, "//*[contains(normalize-space(.),'O relatório') and contains(normalize-space(.),'está disponível')]"))
        )
        try:
            link = alerta.find_element(By.XPATH, ".//a[contains(normalize-space(.),'Clique aqui')]")
        except Exception:
            link = driver.find_element(By.XPATH, "//a[contains(normalize-space(.),'Clique aqui')]")
        try:
            link.click()
        except Exception:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", link)
            driver.execute_script("arguments[0].click();", link)
        return True
    except TimeoutException:
        return False
    except Exception:
        return False

def _clicar_exportar_excel(driver, wait, download_dir, tentativas=4):
    seletores = [
        (By.CSS_SELECTOR, "button[title='Exportar para Excel']"),
        (By.XPATH, "//button[@title='Exportar para Excel' and contains(@class,'ui-button')]"),
        (By.XPATH, "//button[.//span[contains(@class,'custom-icon-toolbar-excel')]]"),
        (By.XPATH, "//button[.//span[normalize-space()='Exportar para Excel']]"),
    ]
    for _ in range(tentativas):
        for by, sel in seletores:
            try:
                btn = wait.until(EC.presence_of_element_located((by, sel)))
                WebDriverWait(driver, 10).until(
                    lambda d: btn.is_displayed()
                    and btn.is_enabled()
                    and (btn.get_attribute("aria-disabled") in (None, "false"))
                )
                _scroll_into_view(driver, btn)
                time.sleep(0.2)
                try:
                    btn.click()
                except Exception:
                    driver.execute_script("arguments[0].click();", btn)
                if _esperar_inicio(download_dir, timeout=6):
                    return True
                if _clicar_alerta_relatorio(driver, timeout=120):
                    return True
            except TimeoutException:
                continue
            except StaleElementReferenceException:
                continue
    return False

def _slug_filename(name):
    base = "".join(c if c.isalnum() else "_" for c in (name or "").strip()).strip("_")
    return base or "Robo"

def _mover_renomear(origem, destino_dir, final_name):
    """
    Move arquivo mesmo entre discos diferentes.
    Se destino for outra unidade, usa copy+remove.
    """
    try:
        os.makedirs(destino_dir, exist_ok=True)
        final = os.path.join(destino_dir, final_name)

        if os.path.exists(final):
            try:
                os.remove(final)
            except Exception:
                pass

        try:
            os.replace(origem, final)
            return final
        except OSError:
            shutil.copy2(origem, final)
            try:
                os.remove(origem)
            except Exception:
                pass
            return final
    except Exception as e:
        log(f"Erro mover/renomear (copy2): {e}")
        return None

def _chrome_options(hidden=False):
    usuario = getpass.getuser()
    download_dir = os.path.join(f"C:\\Users\\{usuario}\\Downloads")
    opts = Options()
    if hidden:
        opts.add_argument("--headless=new")
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument("--disable-gpu")
    opts.add_argument("--start-maximized")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-logging")
    opts.add_argument("--log-level=3")
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "profile.default_content_settings.popups": 0
    }
    opts.add_experimental_option("prefs", prefs)
    return opts, download_dir

def _mask_to_strftime(mask):
    s = (mask or "").strip().lower()
    if not s:
        return ""
    out = ""
    i = 0
    after_hh = False
    while i < len(s):
        if s.startswith("aaaa", i):
            out += "%Y"; i += 4; continue
        if s.startswith("aa", i):
            out += "%y"; i += 2; continue
        if s.startswith("dd", i):
            out += "%d"; i += 2; continue
        if s.startswith("hh", i):
            out += "%H"; i += 2; after_hh = True; continue
        if s.startswith("mm", i):
            out += "%M" if after_hh else "%m"; i += 2; continue
        if s[i] in "/-_: .":
            out += s[i]; i += 1; continue
        out += s[i]; i += 1
    return out

def _fill_input(drv, el, text):
    try:
        el.clear()
    except Exception:
        pass
    try:
        el.send_keys(text)
        time.sleep(0.15)
    except Exception:
        pass
    try:
        val = el.get_attribute("value") or ""
    except Exception:
        val = ""
    if val.strip() != text.strip():
        try:
            drv.execute_script("""
                const e = arguments[0], v = arguments[1];
                e.value = v;
                e.dispatchEvent(new Event('input',{bubbles:true}));
                e.dispatchEvent(new Event('change',{bubbles:true}));
            """, el, text)
            time.sleep(0.15)
        except Exception:
            pass
    return True

def _preencher_parametros_modal(driver, wait, data_inicio_fmt, data_fim_fmt, parametro):
    try:
        dlg = wait.until(EC.presence_of_element_located((
            By.XPATH,
            "//div[contains(@class,'ui-dialog') and (contains(@style,'display: block') or @aria-hidden='false')][.//span[contains(.,'Parâmetros')]]"
        )))
    except Exception:
        return False

    di = (data_inicio_fmt or "").strip()
    df = (data_fim_fmt or "").strip()
    param = (parametro or "").strip()

    def _find_input_by_label(lbl):
        try:
            return dlg.find_element(By.XPATH, f".//label[normalize-space()='{lbl}']/following::input[1]")
        except Exception:
            return None

    inp_ini = _find_input_by_label("data_inicio")
    inp_fim = _find_input_by_label("data_fim")

    if not inp_ini or not inp_fim:
        try:
            inputs = dlg.find_elements(By.XPATH, ".//input[@type='text' or @type='search']")
        except Exception:
            inputs = []
        if len(inputs) >= 2:
            inp_ini = inp_ini or inputs[0]
            inp_fim = inp_fim or inputs[1]

    def _type_force(el, txt):
        if not el or not txt:
            return False
        try:
            el.clear()
        except Exception:
            pass
        try:
            el.send_keys(txt)
            time.sleep(0.15)
        except Exception:
            pass
        try:
            val = (el.get_attribute("value") or "").strip()
        except Exception:
            val = ""
        if val != txt.strip():
            try:
                driver.execute_script("""
                    const e=arguments[0], v=arguments[1];
                    e.value=v;
                    e.dispatchEvent(new Event('input',{bubbles:true}));
                    e.dispatchEvent(new Event('change',{bubbles:true}));
                """, el, txt)
                time.sleep(0.15)
                val = (el.get_attribute("value") or "").strip()
            except Exception:
                pass
        return val == txt.strip()

    ok_ini = _type_force(inp_ini, di) if di else True
    ok_fim = _type_force(inp_fim, df) if df else True

    if param:
        alvo = None
        try:
            alvo = dlg.find_element(
                By.XPATH,
                ".//label[contains(translate(normalize-space(),'ÂÁÀÃÉÊÍÓÔÕÚÇ','aaaaeeiooouc'),'param') or "
                "contains(translate(normalize-space(),'ÂÁÀÃÉÊÍÓÔÕÚÇ','aaaaeeiooouc'),'filtro')]/following::input[1]"
            )
        except Exception:
            try:
                inputs = dlg.find_elements(By.XPATH, ".//input[@type='text' or @type='search']")
                for e in inputs:
                    if e is not inp_ini and e is not inp_fim:
                        alvo = e
                        break
            except Exception:
                pass
        ok_param = _type_force(alvo, param) if alvo else True
    else:
        ok_param = True

    if not (ok_ini and ok_fim and ok_param):
        return False

    try:
        btn_conf = dlg.find_element(By.XPATH, ".//button[.//span[normalize-space()='Confirmar'] or .//span[contains(@class,'ui-button-text') and contains(.,'Confirmar')]]")
    except Exception:
        return False

    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn_conf)
        time.sleep(0.1)
        try:
            btn_conf.click()
        except Exception:
            driver.execute_script("arguments[0].click();", btn_conf)
    except Exception:
        return False

    try:
        WebDriverWait(driver, 8).until(EC.invisibility_of_element(dlg))
    except Exception:
        pass

    return True

def _validar_login(driver, wait):
    try:
        wait.until(EC.presence_of_element_located((
            By.XPATH,
            "//*[contains(@class,'ui-accordion') or contains(@class,'ui-panelmenu')]"
        )))
        return True
    except Exception:
        return False

def _fazer_login(driver, wait, matricula, senha):
    try:
        try:
            wait.until(EC.presence_of_element_located((
                By.XPATH,
                "//input[@id='frm:idFuncionario' or contains(@placeholder,'ódigo')]"
            )))
        except Exception:
            log("LOGIN → Página de login não carregou corretamente.")
            return False

        campo_matricula = None
        try:
            campo_matricula = driver.find_element(By.ID, "frm:idFuncionario")
        except Exception:
            try:
                campo_matricula = driver.find_element(
                    By.XPATH, "//input[contains(@placeholder,'ódigo') or contains(@aria-label,'ódigo')]"
                )
            except Exception:
                log("LOGIN → Campo de código/matrícula não encontrado.")
                return False

        try:
            campo_matricula.clear()
        except Exception:
            pass
        campo_matricula.send_keys(matricula)

        campo_senha = None
        try:
            campo_senha = driver.find_element(By.ID, "frm:senha")
        except Exception:
            try:
                campo_senha = driver.find_element(
                    By.XPATH, "//input[@type='password' or contains(@placeholder,'enha')]"
                )
            except Exception:
                log("LOGIN → Campo senha não encontrado.")
                return False

        try:
            campo_senha.clear()
        except Exception:
            pass
        campo_senha.send_keys(senha)

        try:
            combo = driver.find_element(
                By.XPATH,
                "//*[@id='frm:idEmpresa_label' or contains(@id,'idEmpresa_label') or contains(.,'Selecione Empresa')]"
            )
            combo.click()
            time.sleep(0.3)
            try:
                opt = driver.find_element(
                    By.XPATH,
                    "//li[@data-label='1500' or normalize-space(text())='1500']"
                )
                opt.click()
            except Exception:
                log("LOGIN → Empresa 1500 não encontrada, seguindo sem selecionar.")
        except Exception:
            log("LOGIN → Combo de empresa não encontrado (pode não ser obrigatório).")

        btn_login = None
        try:
            btn_login = driver.find_element(By.ID, "frm:entrar")
        except Exception:
            try:
                btn_login = driver.find_element(
                    By.XPATH,
                    "//button[@id='frm:entrar' or .//span[normalize-space()='Autenticar'] or normalize-space(text())='Autenticar']"
                )
            except Exception:
                log("LOGIN → Botão Autenticar não encontrado.")
                return False

        try:
            btn_login.click()
        except Exception:
            try:
                driver.execute_script("arguments[0].click();", btn_login)
            except Exception:
                log("LOGIN → Falha ao clicar no botão Autenticar.")
                return False

        time.sleep(1)
        try:
            alerta_erro = driver.find_element(
                By.XPATH,
                "//*[contains(text(),'Digite o código do usuário') or contains(text(),'código do usuário')]"
            )
            if alerta_erro.is_displayed():
                log("LOGIN → Alerta 'Digite o código do usuário' detectado. Login inválido.")
                return False
        except Exception:
            pass

        if not _validar_login(driver, wait):
            log("LOGIN → Não foi possível validar a entrada no sistema (menu não carregou).")
            return False

        log("LOGIN → OK.")
        return True

    except Exception as e:
        log(f"LOGIN → Erro inesperado: {e}")
        return False

def exportar_link(matricula, senha, link, data_inicio, data_fim, parametro, pasta_destino, final_name, headless=True):
    opts, download_dir = _chrome_options(hidden=headless)
    driver = webdriver.Chrome(options=opts)
    wait = WebDriverWait(driver, 30)
    try:
        try:
            driver.get("https://leomprd2.seniorcloud.com.br/WisStandard/login.xhtml")
        except Exception as e:
            log(f"ERRO SITE → Não conseguiu abrir página de login: {e}")
            return ""

        if not _fazer_login(driver, wait, matricula, senha):
            log("ERRO LOGIN → Falha no login, encerrando tentativa para recomeçar do zero.")
            return ""

        try:
            driver.get(link)
        except Exception as e:
            log(f"ERRO LINK → Não conseguiu abrir a URL da consulta: {e}")
            return ""

        abriu_param = False
        try:
            if (data_inicio and data_fim) or parametro:
                preenchido = _preencher_parametros_modal(driver, wait, data_inicio, data_fim, parametro)
                if preenchido:
                    abriu_param = True
                    _aguarda_ajax(driver, timeout=120)
                else:
                    log("PARÂMETROS → Falha ao preencher parâmetros, abortando tentativa.")
                    return ""
        except Exception as e:
            log(f"PARÂMETROS → Erro ao preencher parâmetros: {e}")
            abriu_param = False

        if not abriu_param:
            try:
                btn_exec = WebDriverWait(driver, 20).until(
                    EC.element_to_be_clickable((
                        By.XPATH,
                        "//button[contains(@id,'frmSQL') and .//span[contains(text(),'Executar')]] "
                        "| //button[.//span[normalize-space()='Executar']]"
                    ))
                )
                _scroll_into_view(driver, btn_exec)
                try:
                    btn_exec.click()
                except Exception:
                    driver.execute_script("arguments[0].click();", btn_exec)
            except Exception as e:
                log(f"EXECUTAR → Botão Executar não encontrado/clicado: {e}")

        try:
            wait.until(EC.presence_of_element_located((
                By.XPATH,
                "//*[contains(@class,'ui-datatable') or contains(@class,'ui-datagrid') "
                "or contains(text(),'registro') or contains(text(),'Nenhum registro')]"
            )))
        except TimeoutException:
            log("EXECUTAR → Tabela não apareceu, tentando exportar mesmo assim.")

        export_ok = _clicar_exportar_excel(driver, wait, download_dir, tentativas=6)

        if not export_ok:
            log("EXPORTAR → Botão exportar falhou ou não foi encontrado, mas tentativa de download seguirá se algo iniciou.")

        baixado = _esperar_download(download_dir, timeout=600)

        if not baixado:
            log("DOWNLOAD → Nenhum arquivo detectado. Tentativa será refeita.")
            return ""

        final = _mover_renomear(baixado, pasta_destino, final_name)
        if final:
            log(f"[OK] Arquivo final salvo: {final}")
            return final
        else:
            log("[ERRO] Download detectado mas não foi possível mover/renomear.")
            return ""

    except Exception as e:
        log(f"exportar_link → Erro geral: {e}")
        return ""
    finally:
        try:
            driver.quit()
        except Exception:
            pass

def _hhmm_ok(s, default):
    s = (s or "").strip()
    if ":" not in s:
        return default
    try:
        h, m = s.split(":", 1)
        h = int(h); m = int(m)
        if 0 <= h <= 23 and 0 <= m <= 59:
            return f"{h:02d}:{m:02d}"
        return default
    except Exception:
        return default

def _within_window(r, dt):
    hi = r.get("h_ini", "00:00"); hf = r.get("h_fim", "23:59")
    hi_t = dtime(int(hi[:2]), int(hi[3:5])); hf_t = dtime(int(hf[:2]), int(hf[3:5]))
    t = dt.time()
    if hi_t <= hf_t:
        return hi_t <= t <= hf_t
    return not (hf_t < t < hi_t)

def _should_run_now(r):
    now = datetime.now()
    wd = now.weekday()
    dias = r.get("dias", {})
    mapa = [
        dias.get("seg", True),
        dias.get("ter", True),
        dias.get("qua", True),
        dias.get("qui", True),
        dias.get("sex", True),
        dias.get("sab", True),
        dias.get("dom", True),
    ]
    if not mapa[wd]:
        return False
    return _within_window(r, now)

def _next_run_ts_display(r, last_override: str | None = None):
    try:
        intervalo = int(r.get("intervalo_min", 10))
    except Exception:
        intervalo = 10

    last_str = (last_override or r.get("ultima_exec") or "").strip()
    now = datetime.now()

    if last_str:
        try:
            last_dt = datetime.strptime(last_str, "%Y-%m-%d %H:%M")
        except Exception:
            last_dt = now
    else:
        last_dt = now

    cand = last_dt + timedelta(minutes=intervalo)
    if cand < now:
        cand = now + timedelta(minutes=intervalo)
    return cand.strftime("%Y-%m-%d %H:%M")

def _mask_default():
    return "%d%m%Y%H%M"

def _periodo_simples(r, agora):
    fmt_mask = (r.get("fmt_datahora") or "").strip()
    fmt = _mask_to_strftime(fmt_mask) or _mask_default()
    modo = r.get("tipo_data", "MES_ATUAL")
    if modo == "MES_ATUAL":
        ini = agora.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        fim = agora
    elif modo == "ULTIMOS_N":
        n = int(r.get("n_dias", 1))
        ini = (agora - timedelta(days=n)).replace(hour=0, minute=0, second=0, microsecond=0)
        fim = agora
    else:
        ini = agora.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        fim = agora
    return ini, fim, fmt

def _montar_periodos(r, destino_base):
    """
    Monta lista de períodos para o robô.

    - tipo_arquivo == HIST: 1 período simples.
    - tipo_arquivo == NOVO:
        * semana == True  → semanas do mês vigente (S1..S4) com lógica de 2 dias.
        * semana == False & ano_ref vazio → mês anterior + mês atual.
        * semana == False & ano_ref preenchido → ano inteiro até mês vigente.
    """
    agora = datetime.now()
    _ini_dummy, _fim_dummy, fmt = _periodo_simples(r, agora)
    nome_base = _slug_filename(r.get("nome", "Robo"))
    tipo_arq = r.get("tipo_arquivo", "NOVO")
    semana_on = bool(r.get("semana", False))
    ano_ref_str = (r.get("ano_ref") or "").strip() if r.get("ano_ref") is not None else ""
    periodos = []

    # HISTÓRICO → 1 período só
    if tipo_arq == "HIST":
        di, df, fmt = _periodo_simples(r, agora)
        di_str = di.strftime(fmt)
        df_str = df.strftime(fmt)
        label = f"{di.strftime('%d/%m/%Y')} - {df.strftime('%d/%m/%Y')}"
        final_name = f"{nome_base}_{datetime.now().strftime('%Y%m%d%H%M%S')}.xlsx"
        periodos.append(
            {"di_str": di_str, "df_str": df_str, "label": label, "final_name": final_name}
        )
        return periodos

    # SEMANA = SIM → quebra por semanas do mês atual
    if semana_on:
        ano = agora.year
        mes = agora.month
        last_day = monthrange(ano, mes)[1]
        hoje_dia = agora.day
        semanas_cfg = [(1, 7), (8, 14), (15, 21), (22, last_day)]

        for idx, (ini_d, fim_d) in enumerate(semanas_cfg, start=1):
            if ini_d > hoje_dia:
                break
            real_fim_d = min(fim_d, hoje_dia)

            di_date = datetime(ano, mes, ini_d, 0, 0)
            df_date = datetime(ano, mes, real_fim_d, 23, 59)
            if real_fim_d == hoje_dia:
                df_date = agora

            limite_refazer = df_date + timedelta(days=2)
            final_name = f"{nome_base}_{ano}{mes:02d}_S{idx}.xlsx"
            caminho_final = os.path.join(destino_base, final_name)

            if os.path.exists(caminho_final) and agora > limite_refazer:
                # Semana já consolidada há mais de 2 dias → não refaz
                log(
                    f"{r.get('nome','')} → Semana {idx} já consolidada, "
                    f"pulando período {di_date.strftime('%d/%m/%Y')} - {df_date.strftime('%d/%m/%Y')}."
                )
                continue

            di_str = di_date.strftime(fmt)
            df_str = df_date.strftime(fmt)
            label = f"{di_date.strftime('%d/%m/%Y')} - {df_date.strftime('%d/%m/%Y')}"
            periodos.append(
                {"di_str": di_str, "df_str": df_str, "label": label, "final_name": final_name}
            )

        # Garantia: se por algum motivo não montou nada, monta mês inteiro até hoje
        if not periodos:
            di_date = datetime(ano, mes, 1, 0, 0)
            df_date = agora
            di_str = di_date.strftime(fmt)
            df_str = df_date.strftime(fmt)
            final_name = f"{nome_base}_{ano}{mes:02d}_S1.xlsx"
            label = f"{di_date.strftime('%d/%m/%Y')} - {df_date.strftime('%d/%m/%Y')}"
            periodos.append(
                {"di_str": di_str, "df_str": df_str, "label": label, "final_name": final_name}
            )

        return periodos

    # SEMANA = NÃO
    # Ano não preenchido → mês anterior inteiro + mês atual até hoje
    if not ano_ref_str:
        ano = agora.year
        mes_atual = agora.month

        if mes_atual == 1:
            mes_ant = 12
            ano_ant = ano - 1
        else:
            mes_ant = mes_atual - 1
            ano_ant = ano

        # Mês anterior
        di1 = datetime(ano_ant, mes_ant, 1, 0, 0)
        last_ant = monthrange(ano_ant, mes_ant)[1]
        df1 = datetime(ano_ant, mes_ant, last_ant, 23, 59)

        # Mês atual
        di2 = datetime(ano, mes_atual, 1, 0, 0)
        df2 = agora

        for (di, df, a_p, m_p) in [(di1, df1, ano_ant, mes_ant), (di2, df2, ano, mes_atual)]:
            di_str = di.strftime(fmt)
            df_str = df.strftime(fmt)
            final_name = f"{nome_base}_{a_p}{m_p:02d}.xlsx"
            label = f"{di.strftime('%d/%m/%Y')} - {df.strftime('%d/%m/%Y')}"
            periodos.append(
                {"di_str": di_str, "df_str": df_str, "label": label, "final_name": final_name}
            )

        return periodos

    # Ano preenchido → ano_ref mês a mês até mês vigente
    try:
        ano_ref = int(ano_ref_str)
    except Exception:
        ano_ref = agora.year

    if ano_ref < agora.year:
        ultimo_mes = 12
    elif ano_ref > agora.year:
        ultimo_mes = 12
    else:
        ultimo_mes = agora.month

    for mes in range(1, ultimo_mes + 1):
        di = datetime(ano_ref, mes, 1, 0, 0)
        last_m = monthrange(ano_ref, mes)[1]
        if ano_ref == agora.year and mes == ultimo_mes:
            df = agora
        else:
            df = datetime(ano_ref, mes, last_m, 23, 59)

        di_str = di.strftime(fmt)
        df_str = df.strftime(fmt)
        final_name = f"{nome_base}_{ano_ref}{mes:02d}.xlsx"
        label = f"{di.strftime('%d/%m/%Y')} - {df.strftime('%d/%m/%Y')}"
        periodos.append(
            {"di_str": di_str, "df_str": df_str, "label": label, "final_name": final_name}
        )

    return periodos

def _run_robo(rid):
    data = ler_robos()
    robos = data.get("robos", [])
    r = next((x for x in robos if x.get("id") == rid), None)
    if not r:
        return
    if state["queue"].get(rid):
        return

    creds, paths = carregar_config()
    matricula = creds.get("SAP", {}).get("matricula", "")
    senha = creds.get("SAP", {}).get("senha", "")
    if not matricula or not senha:
        log(f"Robo {r.get('nome','')} sem credenciais.")
        return
    destino = r.get("destino") or paths.get(KEY_SAP, _desktop()) or _desktop()

    parametro = (r.get("parametro", "")).strip()
    link = (r.get("link") or "").strip()
    headless = bool(r.get("headless", True))

    periodos = _montar_periodos(r, destino)

    def _work():
        state["queue"][rid] = True
        ok_geral = True

        for periodo in periodos:
            di_str = periodo["di_str"]
            df_str = periodo["df_str"]
            final_name = periodo["final_name"]
            label = periodo["label"]
            sucesso_periodo = False

            for tentativa in range(1, MAX_TENTATIVAS + 1):
                log(
                    f"Execução {r.get('nome','')} [{label}] tentativa "
                    f"{tentativa}/{MAX_TENTATIVAS}"
                )
                saida = ""
                try:
                    saida = exportar_link(
                        matricula,
                        senha,
                        link,
                        di_str,
                        df_str,
                        parametro,
                        destino,
                        final_name,
                        headless=headless,
                    )
                except Exception as e:
                    log(f"Erro {r.get('nome','')} no período {label}: {e}")

                alvo = os.path.join(destino, final_name)
                if (saida and os.path.exists(saida)) or os.path.exists(alvo):
                    sucesso_periodo = True
                    log(f"{r.get('nome','')} OK no período {label}")
                    break

                time.sleep(SLEEP_ENTRE_TENTATIVAS)

            if not sucesso_periodo:
                ok_geral = False
                log(f"{r.get('nome','')} FALHA no período {label}")

        state["queue"].pop(rid, None)

        keep_enabled = bool(r.get("habilitado", False))
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
        patch = {
            "ultima_exec": now_str,
            "proxima_exec": _next_run_ts_display(r, last_override=now_str) if keep_enabled else "",
            "habilitado": keep_enabled,
        }
        _update_robo_fields_atomic(rid, patch)

        if ok_geral:
            log(f"{r.get('nome','')} OK (todos períodos)")
        else:
            log(f"{r.get('nome','')} FALHA (um ou mais períodos)")

    threading.Thread(target=_work, daemon=True).start()

def _update_robo_fields_atomic(rid, patch: dict):
    data_atual = ler_robos()
    lista = data_atual.get("robos", [])
    for rr in lista:
        if rr.get("id") == rid:
            rr.update(patch)
            break
    salvar_robos({"robos": lista})

def enqueue_run(rid):
    if rid in state["queue"]:
        return
    if rid in state["fifo"]:
        return
    state["fifo"].append(rid)

def runner_loop():
    while state["runner_on"]:
        try:
            if state["fifo"]:
                rid = state["fifo"].popleft()
                if state["queue"].get(rid):
                    continue
                _run_robo(rid)
                while state["queue"].get(rid):
                    time.sleep(1)
            else:
                time.sleep(0.5)
        except Exception as e:
            log(f"runner: {e}")
            time.sleep(1)

def scheduler_loop():
    while state["scheduler_on"]:
        try:
            data = ler_robos()
            robos = data.get("robos", [])
            for r in robos:
                if not r.get("habilitado"):
                    continue
                if not _should_run_now(r):
                    continue
                if state["queue"].get(r["id"]):
                    continue
                last = r.get("ultima_exec")
                now = datetime.now()
                ready = False
                if not last:
                    ready = True
                else:
                    try:
                        last_dt = datetime.strptime(last, "%Y-%m-%d %H:%M")
                    except Exception:
                        last_dt = now - timedelta(days=2)
                    if now >= last_dt + timedelta(minutes=int(r.get("intervalo_min", 10))):
                        ready = True
                if ready:
                    enqueue_run(r["id"])
        except Exception as e:
            log(f"scheduler: {e}")
        time.sleep(5)

threading.Thread(target=scheduler_loop, daemon=True).start()
threading.Thread(target=runner_loop, daemon=True).start()

PAGE_HOME = """
<!doctype html><html lang="pt-br"><head><meta charset="utf-8">
<title>Robô Exportador – Web</title><meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  :root{
    --bg:#eef2f6; --card:#ffffff; --line:#d1d5db; --field:#f8fafc; --field-b:#cbd5e1;
    --txt:#1e293b; --txt2:#334155; --title:#0f172a;
    --accent:#16a34a; --accent-2:#64748b; --danger:#dc2626;
  }
  *{box-sizing:border-box}
  body{font-family:"Inter",Arial,Helvetica,sans-serif;background:var(--bg);color:var(--txt);margin:0;padding:24px}
  .container{max-width:1100px;margin:0 auto}
  .card{
    background:var(--card);border:1px solid var(--line);border-radius:16px;
    padding:20px;margin-bottom:24px;
    box-shadow:0 3px 10px rgba(0,0,0,.08);
  }
  .header-line{display:flex;align-items:center;justify-content:space-between;gap:12px}
  h1{margin:0 0 14px;font-size:22px;color:var(--title)}
  label{display:block;font-size:14px;margin:6px 0;color:var(--txt2)}
  input[type=text], input[type=password], select{
    width:100%;padding:12px 14px;border-radius:10px;border:1px solid var(--field-b);
    background:var(--field);color:var(--title);outline:none
  }
  input[type=text]:focus, input[type=password]:focus, select:focus{
    border-color:var(--accent);background:#fff;box-shadow:0 0 0 2px rgba(22,163,74,.12)
  }
  .btn{
    display:inline-block;padding:10px 16px;border-radius:10px;border:1px solid var(--accent);
    background:var(--accent);color:#fff;cursor:pointer;text-decoration:none;font-size:13px;transition:.2s
  }
  .btn:hover{background:#15803d;border-color:#15803d}
  .btn.gray{border-color:#64748b;background:#64748b}
  .btn.gray:hover{background:#475569}
  .btn.red{border-color:#dc2626;background:#dc2626}
  .btn.red:hover{background:#b91c1c}
  .btn.orange{border-color:#d97706;background:#d97706}
  .btn.orange:hover{background:#b45309}
  .badge{display:inline-block;padding:4px 10px;border-radius:999px;background:#e2e8f0;border:1px solid #cbd5e1;color:#475569;margin-left:8px}
  .card-robo{
    display:flex;justify-content:space-between;align-items:center;gap:8px;flex-wrap:wrap;
    padding:12px 16px;border:1px solid #e2e8f0;border-radius:12px;background:#f9fafb;margin-top:10px
  }
  .card-robo:hover{background:#f1f5f9}
  .info-line{font-size:14px;display:flex;flex-wrap:wrap;gap:8px;align-items:center;color:#334155}
  .info-line span{margin-right:6px}
  a.btnlink{color:#0d9488;text-decoration:none} a.btnlink:hover{text-decoration:underline}
  .row{display:flex;gap:16px;flex-wrap:wrap}
  .col{flex:1 1 260px;min-width:240px}
</style></head><body>
<div class="container">

  <div class="card">
    <div class="header-line">
      <h1>Robô Exportador – Web</h1>
      <a class="btn" href="{{ url_for('novo_robo') }}">+ Botão</a>
    </div>

    <form method="post" action="{{ url_for('salvar_creds') }}" style="margin-top:10px">
      <div class="row">
        <div class="col">
          <label>Matrícula</label>
          <input type="text" name="matricula" value="{{ matricula or '' }}">
        </div>
        <div class="col">
          <label>Senha</label>
          <input type="password" name="senha" value="{{ senha or '' }}">
        </div>
      </div>

      <div style="margin-top:12px;display:flex;align-items:center;gap:10px;flex-wrap:wrap">
        <button class="btn" type="submit">Salvar</button>
        <a class="btn gray" href="{{ url_for('logs') }}" target="_blank">Abrir Logs</a>
      </div>
    </form>
  </div>

  <div class="card">
    <div class="header-line">
      <h1>Meus Robôs</h1>
      <span class="badge">Total: {{ robos|length }}</span>
    </div>

    {% if robos|length == 0 %}
      <div style="margin-top:8px">
        Nenhum robô cadastrado. Clique em
        <a class="btnlink" href="{{ url_for('novo_robo') }}">+ Botão</a>.
      </div>
    {% else %}
      {% for r in robos %}
        <div class="card-robo" data-rid="{{ r.id }}">
          <div class="info-line">
            <span style="font-weight:600;font-size:15px">{{ r.nome }}</span>
            <span>| Janela: {{ r.h_ini }} – {{ r.h_fim }}</span>
            <span>| Cada: {{ r.intervalo_min or 10 }} min</span>
            <span>| Dias:
              {% set d = r.dias %}
              {% if d.seg %}Seg {% endif %}{% if d.ter %}Ter {% endif %}{% if d.qua %}Qua {% endif %}
              {% if d.qui %}Qui {% endif %}{% if d.sex %}Sex {% endif %}{% if d.sab %}Sab {% endif %}{% if d.dom %}Dom{% endif %}
            </span>
            <span>| Arquivo: <strong>{{ 'Histórico' if r.tipo_arquivo=='HIST' else 'Novo' }}</strong></span>
            {% if r.parametro %}<span>| Parâmetro: <strong>{{ r.parametro }}</strong></span>{% endif %}
            {% if r.fmt_datahora %}<span>| Formato: <strong>{{ r.fmt_datahora }}</strong></span>{% endif %}
            {% if r.semana %}<span>| Semana: <strong>Sim</strong></span>{% else %}<span>| Semana: <strong>Não</strong></span>{% endif %}
            {% if r.ano_ref %}<span>| Ano: <strong>{{ r.ano_ref }}</strong></span>{% endif %}
            <span>| Última: <strong class="ult">{{ r.ultima_exec or '-' }}</strong></span>
            <span>| Próxima: <strong class="prox">{{ r.proxima_exec or '-' }}</strong></span>
            {% if r.id in running_ids %}<span style="color:#b45309">| Rodando…</span>{% endif %}
          </div>

          <div style="display:flex;gap:6px;flex-wrap:wrap">
            <form method="post" action="{{ url_for('toggle_robo', rid=r.id) }}">
              <button class="btn {{ 'orange' if r.habilitado else '' }}" type="submit">{{ 'Desligar' if r.habilitado else 'Ligar' }}</button>
            </form>
            <form method="get" action="{{ url_for('editar_robo', rid=r.id) }}">
              <button class="btn gray" type="submit">Editar</button>
            </form>
            <form method="post" action="{{ url_for('excluir_robo', rid=r.id) }}">
              <button class="btn red" type="submit" onclick="return confirm('Excluir {{ r.nome }}?')">Excluir</button>
            </form>
            <form method="post" action="{{ url_for('executar_robo', rid=r.id) }}">
              <button class="btn" type="submit">Executar</button>
            </form>
          </div>
        </div>
      {% endfor %}
    {% endif %}
  </div>
</div>

<script>
let snap = {};
function takeSnapshot(){
  document.querySelectorAll('.card-robo').forEach(card=>{
    const rid = card.getAttribute('data-rid');
    const ult = card.querySelector('.ult')?.textContent.trim() || '';
    const prox = card.querySelector('.prox')?.textContent.trim() || '';
    snap[rid] = {ult, prox};
  });
}
takeSnapshot();

async function poll(){
  try{
    const r = await fetch('{{ url_for("status") }}',{cache:'no-store'});
    const j = await r.json();
    if(!j || !j.ok){ return; }
    let changed = false;
    j.items.forEach(it=>{
      const prev = snap[it.id] || {};
      const wasRunning = !!it.was_running;
      const nowRunning = !!it.running;
      if (prev.ult !== it.ultima_exec || (wasRunning && !nowRunning)){
        changed = true;
      }
    });
    if(changed){ location.reload(); }
  }catch(e){}
}
setInterval(poll, 7000);
</script>
</body></html>
"""

PAGE_FORM = """
<!doctype html><html lang="pt-br"><head><meta charset="utf-8"><title>{{ 'Editar' if edit else 'Novo' }} Robô</title><meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  :root{
    --bg:#eef2f6; --card:#ffffff; --border:#d1d5db; --field:#f8fafc; --field-b:#cbd5e1;
    --txt:#1e293b; --txt-2:#334155; --title:#0f172a;
    --accent:#16a34a; --accent-2:#64748b; --danger:#dc2626;
  }
  body{font-family:Inter,Arial,Helvetica,sans-serif;background:var(--bg);color:var(--txt);margin:0;padding:24px}
  .container{max-width:1000px;margin:0 auto}
  .card{background:var(--card);border:1px solid var(--border);border-radius:16px;padding:20px;margin-bottom:16px;box-shadow:0 3px 10px rgba(0,0,0,.08)}
  h1{margin:0 0 14px;font-size:22px;color:var(--title)}
  label{display:block;font-size:14px;margin:6px 0;color:var(--txt-2)}
  input[type=text],select{
    width:100%;padding:10px;border-radius:10px;border:1px solid var(--field-b);background:var(--field);color:var(--title);outline:none;box-sizing:border-box
  }
  input[type=text]:focus,select:focus{border-color:var(--accent);background:#fff}
  .btn{display:inline-block;padding:10px 14px;border:1px solid var(--accent);background:var(--accent);color:#fff;border-radius:10px;cursor:pointer;text-decoration:none;transition:.2s}
  .btn:hover{background:#15803d;border-color:#15803d}
  .btn.gray{border-color:var(--accent-2);background:var(--accent-2)}
  .btn.gray:hover{background:#475569}
  .grid{display:grid;grid-template-columns:repeat(12,1fr);gap:12px}
  .col-12{grid-column:span 12}.col-9{grid-column:span 9}.col-6{grid-column:span 6}.col-3{grid-column:span 3}
  .switch{display:flex;align-items:center;gap:8px;padding-top:28px}
  .pickline{display:grid;grid-template-columns:1fr auto;gap:8px}
  @media (max-width:900px){.col-9,.col-6,.col-3{grid-column:span 12}.switch{padding-top:0}}
</style></head><body>
<div class="container">
  <div class="card">
    <h1>{{ 'Editar' if edit else 'Novo' }} Robô</h1>
    <form method="post">
      <div class="grid">
        <div class="col-9">
          <label>Nome do botão</label>
          <input type="text" name="nome" value="{{ r.nome or '' }}" required>
        </div>
        <div class="col-3 switch">
          <input type="checkbox" id="habilitado" name="habilitado" {% if r.habilitado %}checked{% endif %}>
          <label for="habilitado">Ligar</label>
        </div>

        <div class="col-9">
          <label>Link da consulta</label>
          <input type="text" name="link" value="{{ r.link or '' }}" required>
        </div>
        <div class="col-3 switch">
          <input type="checkbox" id="headless" name="headless" {% if r.headless %}checked{% endif %}>
          <label for="headless">Rodar oculto</label>
        </div>

        <div class="col-9">
          <label>Pasta destino</label>
          <div class="pickline">
            <input type="text" name="destino" id="destino_form" value="{{ r.destino or destino }}">
            <button class="btn gray" type="button" onclick="pickFolder('destino_form')">Procurar pasta</button>
          </div>
        </div>
        <div class="col-3">
          <label>Com data?</label>
          <select name="precisa_data">
            <option value="nao" {% if not r.precisa_data %}selected{% endif %}>Não</option>
            <option value="sim" {% if r.precisa_data %}selected{% endif %}>Sim</option>
          </select>
        </div>

        <div class="col-6">
          <label>Tipo de período</label>
          <select name="tipo_data">
            <option value="MES_ATUAL" {% if r.tipo_data=='MES_ATUAL' %}selected{% endif %}>Mês atual até hoje</option>
            <option value="ULTIMOS_N" {% if r.tipo_data=='ULTIMOS_N' %}selected{% endif %}>Últimos N dias</option>
          </select>
        </div>
        <div class="col-6">
          <label>N dias (se Últimos N)</label>
          <input type="text" name="n_dias" value="{{ r.n_dias or '40' }}">
        </div>

        <div class="col-6">
          <label>Semana?</label>
          <select name="semana">
            <option value="nao" {% if not r.semana %}selected{% endif %}>Não</option>
            <option value="sim" {% if r.semana %}selected{% endif %}>Sim</option>
          </select>
        </div>
        <div class="col-6">
          <label>Ano (opcional)</label>
          <input type="text" name="ano_ref" value="{{ r.ano_ref or '' }}" placeholder="ex.: 2025">
        </div>

        <div class="col-6">
          <label>Hora início</label>
          <input type="text" name="h_ini" value="{{ r.h_ini or '08:00' }}">
        </div>
        <div class="col-6">
          <label>Hora fim</label>
          <input type="text" name="h_fim" value="{{ r.h_fim or '22:00' }}">
        </div>

        <div class="col-6">
          <label>Rodar a cada (min)</label>
          <input type="text" name="intervalo_min" value="{{ r.intervalo_min or '10' }}">
        </div>

        <div class="col-6">
          <label>Salvar como</label>
          <select name="tipo_arquivo">
            <option value="NOVO" {% if r.tipo_arquivo!='HIST' %}selected{% endif %}>Arquivo Novo</option>
            <option value="HIST" {% if r.tipo_arquivo=='HIST' %}selected{% endif %}>Histórico de Arquivo</option>
          </select>
        </div>

        <div class="col-9">
          <label>Parâmetro (opcional)</label>
          <input type="text" name="parametro" value="{{ r.parametro or '' }}">
        </div>
        <div class="col-3">
          <label>Formato data/hora (opcional)</label>
          <input type="text" name="fmt_datahora" placeholder="ex.: ddmmaaahhmm" value="{{ r.fmt_datahora or '' }}">
        </div>

        <div class="col-12">
          <div class="grid" style="grid-template-columns:repeat(7,1fr);gap:10px">
            <label class="switch"><input type="checkbox" id="seg" name="seg" {% if r.dias.seg %}checked{% endif %}><span>Seg</span></label>
            <label class="switch"><input type="checkbox" id="ter" name="ter" {% if r.dias.ter %}checked{% endif %}><span>Ter</span></label>
            <label class="switch"><input type="checkbox" id="qua" name="qua" {% if r.dias.qua %}checked{% endif %}><span>Qua</span></label>
            <label class="switch"><input type="checkbox" id="qui" name="qui" {% if r.dias.qui %}checked{% endif %}><span>Qui</span></label>
            <label class="switch"><input type="checkbox" id="sex" name="sex" {% if r.dias.sex %}checked{% endif %}><span>Sex</span></label>
            <label class="switch"><input type="checkbox" id="sab" name="sab" {% if r.dias.sab %}checked{% endif %}><span>Sab</span></label>
            <label class="switch"><input type="checkbox" id="dom" name="dom" {% if r.dias.dom %}checked{% endif %}><span>Dom</span></label>
          </div>
        </div>

        <div class="col-12" style="display:flex;gap:8px;margin-top:14px">
          <button class="btn" type="submit">Salvar</button>
          <a class="btn gray" href="{{ url_for('home') }}">Voltar</a>
        </div>
      </div>
    </form>
  </div>
</div>
<script>
async function pickFolder(targetId){
  try{
    const r = await fetch('{{ url_for("pick_folder_native") }}');
    const j = await r.json();
    if(j && j.ok && j.path){
      const el = document.getElementById(targetId);
      if(el){ el.value = j.path; }
    }else if(j && j.error){
      alert(j.error);
    }
  }catch(e){}
}
</script>
</body></html>
"""

@APP.route("/", methods=["GET"])
def home():
    creds, paths = carregar_config()
    data = ler_robos()
    robos = data.get("robos", [])

    for rr in robos:
        if "habilitado" not in rr:
            rr["habilitado"] = False
        if "tipo_arquivo" not in rr:
            rr["tipo_arquivo"] = "NOVO"
        if "parametro" not in rr:
            rr["parametro"] = ""
        if "fmt_datahora" not in rr:
            rr["fmt_datahora"] = ""
        if "semana" not in rr:
            rr["semana"] = False
        if "ano_ref" not in rr:
            rr["ano_ref"] = ""

    running_ids = [rid for rid, v in state["queue"].items() if v]
    return render_template_string(
        PAGE_HOME,
        matricula=creds.get("SAP", {}).get("matricula", ""),
        senha=creds.get("SAP", {}).get("senha", ""),
        destino=paths.get(KEY_SAP, _desktop()),
        robos=robos,
        running_ids=running_ids,
    )

@APP.get("/logs", endpoint="logs")
def view_logs():
    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            content = f.read()
    except Exception:
        content = ""
    return f"<pre>{content}</pre>"

@APP.get("/status", endpoint="status")
def status():
    data = ler_robos()
    items = []
    for r in data.get("robos", []):
        rid = r.get("id")
        items.append(
            {
                "id": rid,
                "ultima_exec": r.get("ultima_exec") or "",
                "proxima_exec": r.get("proxima_exec") or "",
                "running": bool(state["queue"].get(rid)),
                "was_running": bool(state["queue"].get(rid)),
            }
        )
    return jsonify({"ok": True, "items": items, "pending": len(state["fifo"])})

@APP.route("/salvar_creds", methods=["POST"])
def salvar_creds():
    m = request.form.get("matricula", "").strip()
    s = request.form.get("senha", "").strip()
    d = request.form.get("destino", "").strip()
    if m or s:
        salvar_credenciais(m, s)
    if d:
        salvar_caminho(d)
    log("Credenciais/pasta salvas.")
    return redirect(url_for("home"))

@APP.route("/novo", methods=["GET", "POST"])
def novo_robo():
    creds, paths = carregar_config()
    if request.method == "GET":
        r = {
            "id": "",
            "nome": "",
            "link": "",
            "destino": paths.get(KEY_SAP, _desktop()),
            "precisa_data": True,
            "tipo_data": "MES_ATUAL",
            "n_dias": 40,
            "h_ini": "08:00",
            "h_fim": "22:00",
            "intervalo_min": 10,
            "dias": {
                "seg": True,
                "ter": True,
                "qua": True,
                "qui": True,
                "sex": True,
                "sab": True,
                "dom": True,
            },
            "headless": True,
            "habilitado": False,
            "ultima_exec": "",
            "proxima_exec": "",
            "tipo_arquivo": "NOVO",
            "parametro": "",
            "fmt_datahora": "",
            "semana": False,
            "ano_ref": "",
        }
        return render_template_string(
            PAGE_FORM, r=type("R", (), r)(), destino=paths.get(KEY_SAP, _desktop()), edit=False
        )

    nome = request.form.get("nome", "").strip()
    link = request.form.get("link", "").strip()
    destino = request.form.get("destino", "").strip()
    precisa_data = request.form.get("precisa_data", "nao") == "sim"
    tipo_data = request.form.get("tipo_data", "MES_ATUAL")
    n_dias = request.form.get("n_dias", "40")
    h_ini = _hhmm_ok(request.form.get("h_ini", "08:00"), "08:00")
    h_fim = _hhmm_ok(request.form.get("h_fim", "22:00"), "22:00")
    intervalo_min = request.form.get("intervalo_min", "10")
    tipo_arquivo = request.form.get("tipo_arquivo", "NOVO")
    parametro = request.form.get("parametro", "").strip()
    fmt_datahora = request.form.get("fmt_datahora", "").strip()
    semana = request.form.get("semana", "nao") == "sim"
    ano_ref = request.form.get("ano_ref", "").strip()

    dias = {
        "seg": request.form.get("seg") == "on",
        "ter": request.form.get("ter") == "on",
        "qua": request.form.get("qua") == "on",
        "qui": request.form.get("qui") == "on",
        "sex": request.form.get("sex") == "on",
        "sab": request.form.get("sab") == "on",
        "dom": request.form.get("dom") == "on",
    }
    headless = request.form.get("headless") == "on"
    habilitado = request.form.get("habilitado") == "on"
    if not nome or not link:
        return redirect(url_for("novo_robo"))

    data = ler_robos()
    rid = str(uuid.uuid4())
    robo = {
        "id": rid,
        "nome": nome,
        "link": link,
        "destino": destino,
        "precisa_data": precisa_data,
        "tipo_data": tipo_data,
        "n_dias": int(n_dias) if n_dias.isdigit() else 40,
        "h_ini": h_ini,
        "h_fim": h_fim,
        "intervalo_min": int(intervalo_min) if intervalo_min.isdigit() else 10,
        "dias": dias,
        "headless": headless,
        "habilitado": habilitado,
        "ultima_exec": "",
        "proxima_exec": _next_run_ts_display(
            {"intervalo_min": int(intervalo_min) if intervalo_min.isdigit() else 10}
        )
        if habilitado
        else "",
        "tipo_arquivo": "HIST" if tipo_arquivo == "HIST" else "NOVO",
        "parametro": parametro,
        "fmt_datahora": fmt_datahora,
        "semana": semana,
        "ano_ref": ano_ref,
    }
    data["robos"].append(robo)
    salvar_robos(data)
    log(f"Criado robô {nome}")
    return redirect(url_for("home"))

@APP.route("/editar/<rid>", methods=["GET", "POST"])
def editar_robo(rid):
    data = ler_robos()
    robos = data.get("robos", [])
    r = next((x for x in robos if x["id"] == rid), None)
    if not r:
        return redirect(url_for("home"))
    creds, paths = carregar_config()
    if request.method == "GET":
        if "tipo_arquivo" not in r:
            r["tipo_arquivo"] = "NOVO"
        if "parametro" not in r:
            r["parametro"] = ""
        if "fmt_datahora" not in r:
            r["fmt_datahora"] = ""
        if "semana" not in r:
            r["semana"] = False
        if "ano_ref" not in r:
            r["ano_ref"] = ""
        return render_template_string(
            PAGE_FORM,
            r=type("R", (), r)(),
            destino=paths.get(KEY_SAP, _desktop()),
            edit=True,
        )

    nome = request.form.get("nome", "").strip()
    link = request.form.get("link", "").strip()
    destino = request.form.get("destino", "").strip()
    precisa_data = request.form.get("precisa_data", "nao") == "sim"
    tipo_data = request.form.get("tipo_data", "MES_ATUAL")
    n_dias = request.form.get("n_dias", "40")
    h_ini = _hhmm_ok(request.form.get("h_ini", "08:00"), "08:00")
    h_fim = _hhmm_ok(request.form.get("h_fim", "22:00"), "22:00")
    intervalo_min = request.form.get("intervalo_min", "10")
    tipo_arquivo = request.form.get("tipo_arquivo", "NOVO")
    parametro = request.form.get("parametro", "").strip()
    fmt_datahora = request.form.get("fmt_datahora", "").strip()
    semana = request.form.get("semana", "nao") == "sim"
    ano_ref = request.form.get("ano_ref", "").strip()

    dias = {
        "seg": request.form.get("seg") == "on",
        "ter": request.form.get("ter") == "on",
        "qua": request.form.get("qua") == "on",
        "qui": request.form.get("qui") == "on",
        "sex": request.form.get("sex") == "on",
        "sab": request.form.get("sab") == "on",
        "dom": request.form.get("dom") == "on",
    }
    headless = request.form.get("headless") == "on"
    habilitado = request.form.get("habilitado") == "on"

    if nome:
        r["nome"] = nome
    if link:
        r["link"] = link
    r["destino"] = destino
    r["precisa_data"] = precisa_data
    r["tipo_data"] = tipo_data
    r["n_dias"] = int(n_dias) if n_dias.isdigit() else r.get("n_dias", 40)
    r["h_ini"] = h_ini
    r["h_fim"] = h_fim
    r["intervalo_min"] = int(intervalo_min) if intervalo_min.isdigit() else r.get("intervalo_min", 10)
    r["dias"] = dias
    r["headless"] = headless
    r["habilitado"] = habilitado
    r["tipo_arquivo"] = "HIST" if tipo_arquivo == "HIST" else "NOVO"
    r["parametro"] = parametro
    r["fmt_datahora"] = fmt_datahora
    r["semana"] = semana
    r["ano_ref"] = ano_ref
    r["proxima_exec"] = _next_run_ts_display(r) if habilitado else ""

    salvar_robos({"robos": robos})
    log(f"Editado robô {r.get('nome','')}")
    return redirect(url_for("home"))

@APP.route("/toggle/<rid>", methods=["POST"])
def toggle_robo(rid):
    data = ler_robos()
    robos = data.get("robos", [])
    r = next((x for x in robos if x["id"] == rid), None)
    if r:
        r["habilitado"] = not r.get("habilitado", False)
        r["proxima_exec"] = _next_run_ts_display(r) if r["habilitado"] else ""
        salvar_robos({"robos": robos})
        log(f"{'Ligado' if r['habilitado'] else 'Desligado'} robô {r.get('nome','')}")
        if r["habilitado"]:
            enqueue_run(r["id"])
    return redirect(url_for("home"))

@APP.route("/excluir/<rid>", methods=["POST"])
def excluir_robo(rid):
    data = ler_robos()
    robos = data.get("robos", [])
    robos = [x for x in robos if x["id"] != rid]
    salvar_robos({"robos": robos})
    log(f"Excluído robô {rid}")
    return redirect(url_for("home"))

@APP.route("/executar/<rid>", methods=["POST"])
def executar_robo(rid):
    enqueue_run(rid)
    log(f"Enfileirado manual robô {rid}")
    return redirect(url_for("home"))

@APP.get("/pick_folder_native")
def pick_folder_native():
    try:
        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        path = filedialog.askdirectory(title="Selecione a pasta de ORIGEM (servidor)")
        root.destroy()
        if not path:
            return jsonify({"ok": False, "error": "Seleção cancelada."})
        return jsonify({"ok": True, "path": path})
    except Exception as e:
        return jsonify({"ok": False, "error": f"Não foi possível abrir o seletor nativo: {e}"}), 500

def create_app():
    return APP

if __name__ == "__main__":
    APP.run(host="127.0.0.1", port=5000, debug=True, use_reloader=False)
