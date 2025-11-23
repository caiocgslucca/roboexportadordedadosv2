# login.py
import os
from datetime import datetime, timedelta
from flask import Flask, request, redirect, url_for, render_template_string, flash, session
from jinja2 import DictLoader
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from conexao import conectar_banco
from web_robo_exportador import create_app as create_robo_app  # importa sem rodar servidor

app = Flask(__name__)
app.secret_key = os.environ.get("APP_SECRET", "chave-fixa-teste")
app.permanent_session_lifetime = timedelta(hours=12)

# monta o robô em /app
APP_ROBO = create_robo_app()
app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {"/app": APP_ROBO})

# ---------------------- Templates ----------------------
TEMPLATES = {
"base.html": """
<!doctype html><html lang="pt-br"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{{ title or 'Robô Exportador' }}</title>
<style>
:root{
  --bg:#e9edf3;--bg2:#cdd5df;--card:#ffffff;--line:#d7dde7;
  --head:#2f3e51;--head-t:#f3f6fb;--btn:#2f3e51;--btn2:#233041;
}
*{box-sizing:border-box} html,body{height:100%}
body{
  margin:0;font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu;
  background:radial-gradient(1200px 700px at 30% 0%,var(--bg) 0,var(--bg2) 80%,#bdc7d3 100%);
  display:grid;place-items:center;
  padding:24px;
}
.card{
  width:480px;max-width:92vw;
  border:1px solid var(--line);border-radius:14px;overflow:hidden;
  box-shadow:0 12px 28px rgba(0,0,0,.10),0 2px 6px rgba(0,0,0,.06);
  background:var(--card);
}
.head{
  background:var(--head);color:var(--head-t);padding:14px 18px;
  display:flex;gap:12px;align-items:center
}
.logo{
  width:38px;height:38px;border-radius:50%;display:grid;place-items:center;
  background:#394b62;color:#fff;font-weight:700
}
.body{padding:18px}
label{display:block;font-size:13px;margin:12px 0 6px;font-weight:600}
input{
  display:block;width:100%;padding:12px 14px;border-radius:10px;
  border:1px solid #cfd6e3;background:#f9fbfe;font-size:14px;outline:none
}
input:focus{border-color:#86a7d9;box-shadow:0 0 0 3px rgba(134,167,217,.18)}
button{
  width:100%;padding:12px;border-radius:10px;border:none;margin-top:16px;
  font-weight:700;cursor:pointer;background:var(--btn);color:#fff
}
button:hover{background:var(--btn2)}
.link{
  display:block;text-align:center;margin-top:10px;font-size:13px;
  color:#2563eb;text-decoration:none
}
.link:hover{text-decoration:underline}
.alert{margin:10px 0;padding:10px;border-radius:10px;font-size:13px}
.alert.danger{background:#fee2e2;color:#7f1d1d}
.alert.success{background:#dcfce7;color:#14532d}
</style></head><body>
  <div class="card">
    <div class="head"><div class="logo">RB</div><div><b>Robô Exportador</b></div></div>
    <div class="body">
      {% with msgs=get_flashed_messages(with_categories=true) %}
        {% for cat,msg in msgs %}<div class="alert {{cat}}">{{msg}}</div>{% endfor %}
      {% endwith %}
      {% block content %}{% endblock %}
    </div>
  </div>
</body></html>
""",

"login.html": """
{% extends "base.html" %}{% block content %}
<form method="post" autocomplete="off">
  <label>Matrícula</label>
  <input name="matricula" required autofocus>
  <label>Senha</label>
  <input name="senha" type="password" required>
  <button type="submit">Entrar</button>
  <a class="link" href="{{ url_for('cadastro') }}">Cadastrar novo usuário</a>
</form>
{% endblock %}
""",

"cadastro.html": """
{% extends "base.html" %}{% block content %}
<form method="post">
  <label>CD</label><input name="cd" required>
  <label>Matrícula</label><input name="matricula" required>
  <label>Usuário</label><input name="usuario" required>
  <label>Senha</label><input name="senha" type="password" required>
  <button type="submit">Salvar</button>
  <a class="link" href="{{ url_for('login') }}">Voltar</a>
</form>
{% endblock %}
"""
}
app.jinja_loader = DictLoader(TEMPLATES)

TABLE = "usuario"

def _db_one(sql, params=()):
    conn = conectar_banco(); cur = conn.cursor(dictionary=True)
    cur.execute(sql, params); row = cur.fetchone()
    cur.close(); conn.close(); return row

def _db_exec(sql, params=()):
    conn = conectar_banco(); cur = conn.cursor()
    cur.execute(sql, params); conn.commit()
    cur.close(); conn.close()

# ---------------------- Rotas ----------------------
@app.route("/", methods=["GET","POST"])
def login():
    if request.method == "GET":
        return render_template_string(app.jinja_loader.get_source(app.jinja_env,"login.html")[0])

    m = (request.form.get("matricula") or "").strip()
    s = (request.form.get("senha") or "").strip()
    row = _db_one(f"SELECT * FROM `{TABLE}` WHERE matricula=%s LIMIT 1", (m,))
    if not row or row.get("senha") != s:
        flash("Matrícula ou senha inválidos.", "danger")
        return redirect(url_for("login"))
    session["user"] = row
    return redirect("/app/")  # entra no robô na mesma aba

@app.route("/cadastro", methods=["GET","POST"])
def cadastro():
    if request.method == "GET":
        return render_template_string(app.jinja_loader.get_source(app.jinja_env,"cadastro.html")[0])
    cd = (request.form.get("cd") or "").strip()
    matricula = (request.form.get("matricula") or "").strip()
    usuario = (request.form.get("usuario") or "").strip()
    senha = (request.form.get("senha") or "").strip()
    if not (cd and matricula and usuario and senha):
        flash("Preencha todos os campos.", "danger")
        return redirect(url_for("cadastro"))
    if _db_one(f"SELECT id FROM `{TABLE}` WHERE matricula=%s",(matricula,)):
        flash("Matrícula já cadastrada.", "danger")
        return redirect(url_for("cadastro"))
    _db_exec(
        f"INSERT INTO `{TABLE}` (cd,matricula,usuario,senha,data_cadastro) VALUES (%s,%s,%s,%s,%s)",
        (cd,matricula,usuario,senha,datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    )
    flash("Usuário cadastrado com sucesso! Faça login.", "success")
    return redirect(url_for("login"))

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True, use_reloader=False)
