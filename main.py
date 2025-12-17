import os
import sys
import pyodbc
from dotenv import load_dotenv
import csv
from datetime import datetime, date

# Carica .env dalla directory corrente (se presente)
load_dotenv()

# Contratto (input/output):
#!/usr/bin/env python3
"""Orchestratore: esegue `orario.dipendenti.py` e poi `orario.gestione_utenti.py` in cascata.

Comportamento:
- Esegue `orario.dipendenti.py` con lo stesso interprete Python; se nel suo stdout appare "$$$" stampa
  "orario.dipendenti.py creato correttamente" e procede con `orario.gestione_utenti.py`.
- Se invece non appare "$$$" stampa "Errore in orario.dipendenti.py" e si ferma.
- Per `orario.gestione_utenti.py` analogo comportamento e messaggi.
"""
import sys
import subprocess
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = [
    ('nuovi.utenti.py', 'nuovi.utenti.py creato correttamente', 'Errore in nuovi.utenti.py'),
    ('orario.dipendenti.py', 'orario.dipendenti.py creato correttamente', 'Errore in orario.dipendenti.py'),
    ('orario.gestione_utenti.py', 'orario.gestione_utenti.py creato correttamente', 'Errore in orario.gestione_utenti.py'),
]

def run_script(script_name):
    path = os.path.join(BASE_DIR, script_name)
    if not os.path.exists(path):
        print(f"{script_name} non trovato")
        return False
    # usa lo stesso interprete Python usato per main
    cmd = [sys.executable, path]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    except Exception as e:
        print(f"Errore esecuzione {script_name}: {e}")
        return False

    stdout = proc.stdout or ''
    stderr = proc.stderr or ''
    # debug prints (short)
    if '$$$' in stdout:
        return True
    else:
        # se stderr o stdout contengono info utili, le mostriamo brevi
        return False

def main():
    for script, success_msg, err_msg in SCRIPTS:
        ok = run_script(script)
        if ok:
            print(success_msg)
        else:
            print(err_msg)
            break

if __name__ == '__main__':
    main()
    
