#!/usr/bin/env python3
"""Esegue via SSH una query MySQL remota e salva i risultati in CSV.

Si aspetta variabili nel file .env nella stessa cartella:
SSH_HOST, SSH_PORT, SSH_USER, DB_USER, DB_PASSWORD, DB_NAME

Scrive il file: ./csv/nuovi.utenti.csv
"""
import os
import shlex
import subprocess
import logging
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None


ROOT = Path(__file__).resolve().parent
ENV_PATH = ROOT / '.env'
CSV_DIR = ROOT / 'csv'
CSV_OUT = CSV_DIR / 'nuovi.utenti.csv'


def load_env():
    if load_dotenv:
        load_dotenv(dotenv_path=str(ENV_PATH))
    else:
        # minimal .env loader if python-dotenv not installed
        if ENV_PATH.exists():
            with ENV_PATH.open() as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#') or '=' not in line:
                        continue
                    k, v = line.split('=', 1)
                    k = k.strip()
                    v = v.strip().strip('"').strip("'")
                    os.environ.setdefault(k, v)


def main():
    load_env()

    ssh_host = os.getenv('SSH_HOST')
    ssh_port = os.getenv('SSH_PORT', '22')
    ssh_user = os.getenv('SSH_USER')

    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME')

    if not all([ssh_host, ssh_user, db_user, db_name]):
        print('Mancano variabili richieste in .env (SSH_HOST/SSH_USER/DB_USER/DB_NAME).')
        raise SystemExit(2)

    # configure simple logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

    logging.info(f"Preparando connessione SSH a {ssh_user}@{ssh_host}:{ssh_port}")

    # Costruisci la query. -B per output tab-separated, -N per no headers
    query = "SELECT old_id FROM gestione_utenti;"


    # Comando mysql da eseguire sul server remoto via ssh
    # Proteggiamo la password con quoting
    mysql_cmd = f"mysql -u{shlex.quote(db_user)} -p{shlex.quote(db_password)} -D {shlex.quote(db_name)} -B -N -e {shlex.quote(query)}"

    ssh_command = [
        'ssh',
        '-o', 'BatchMode=yes',
        '-o', 'ConnectTimeout=15',
        '-p', str(ssh_port),
        f"{ssh_user}@{ssh_host}",
        mysql_cmd,
    ]

    logging.info('Connessione SSH: avvio comando remoto per eseguire la query MySQL')
    try:
        proc = subprocess.run(ssh_command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        logging.info('Comando remoto eseguito con successo; ricevuti risultati dal DB')
        output = proc.stdout
    except subprocess.CalledProcessError as e:
        stderr = (e.stderr or '').strip()
        logging.error('Errore eseguendo il comando remoto via SSH')
        if stderr:
            logging.error(stderr)

        # Se il DB non esiste, proviamo a listare i database disponibili e cercare 'orari'
        if 'Unknown database' in stderr or 'ERROR 1049' in stderr:
            logging.info('Database sconosciuto: provo a elencare i database remoti per trovare un candidato')
            show_cmd = [
                'ssh',
                '-o', 'BatchMode=yes',
                '-o', 'ConnectTimeout=15',
                '-p', str(ssh_port),
                f"{ssh_user}@{ssh_host}",
                f"mysql -u{shlex.quote(db_user)} -p{shlex.quote(db_password)} -B -N -e 'SHOW DATABASES;'",
            ]
            try:
                show_proc = subprocess.run(show_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                dbs = [d.strip() for d in show_proc.stdout.splitlines() if d.strip()]
                logging.info(f'Database remoti trovati: {dbs}')
                # preferiamo esattamente 'orari' se presente, altrimenti proviamo a trovare nome simile
                candidate = None
                if 'orari' in dbs:
                    candidate = 'orari'
                else:
                    # cerca un DB che contenga la radice di DB_NAME
                    env_db = os.getenv('DB_NAME', '')
                    for d in dbs:
                        if env_db and env_db in d:
                            candidate = d
                            break
                if candidate:
                    logging.info(f'Riprovo la query usando il database: {candidate}')
                    # ricostruisci il comando mysql con DB scelto
                    mysql_cmd2 = f"mysql -u{shlex.quote(db_user)} -p{shlex.quote(db_password)} -D {shlex.quote(candidate)} -B -N -e {shlex.quote(query)}"
                    ssh_command2 = ['ssh', '-p', str(ssh_port), f"{ssh_user}@{ssh_host}", mysql_cmd2]
                    proc2 = subprocess.run(ssh_command2, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                    output = proc2.stdout
                else:
                    logging.error('Nessun database candidato trovato per il fallback.')
                    raise
            except subprocess.CalledProcessError as e2:
                logging.error('Errore durante l\'elenco dei database remoti o nel retry')
                if e2.stderr:
                    logging.error(e2.stderr.strip())
                raise
        else:
            raise

    # Assicuriamoci che la cartella CSV esista
    CSV_DIR.mkdir(parents=True, exist_ok=True)

    # mysql -B -N produce righe separate, tab separated columns; qui abbiamo una sola colonna
    # Creiamo un CSV con header "old_id" e salviamo LOCALMENTE (lo stdout proviene dal server remoto ma lo scriviamo qui)
    logging.info(f'Salvo i risultati localmente in: {CSV_OUT}')
    with CSV_OUT.open('w', encoding='utf-8') as f:
        f.write('old_id\n')
        for line in output.splitlines():
            val = line.strip()
            if val:
                val = val.split('\t')[0]
                if ',' in val or '"' in val or '\n' in val:
                    val = '"' + val.replace('"', '""') + '"'
                f.write(val + '\n')

    logging.info('CSV scritto correttamente sul filesystem locale')


if __name__ == '__main__':
    try:
        main()
    except Exception:
        logging.exception('Errore non gestito durante l\'esecuzione')
        # stampo marker di errore richiesto
        print('XXX')
        sys.exit(1)
    else:
        # stampo marker di successo richiesto
        print('$$$')
        sys.exit(0)
