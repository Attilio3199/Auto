#!/usr/bin/env python3
import os
import csv
import sys
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

MSSQL_HOST = os.getenv('MSSQL_HOST')
MSSQL_PORT = os.getenv('MSSQL_PORT', '1433')
MSSQL_USER = os.getenv('MSSQL_USER')
MSSQL_PASS = os.getenv('MSSQL_PASS')
MSSQL_DB = os.getenv('MSSQL_DB')
MSSQL_DRIVER = os.getenv('MSSQL_DRIVER')

def main():
    try:
        if not MSSQL_HOST or not MSSQL_DB:
            print('XXX')
            sys.exit(1)

        try:
            import pyodbc
        except Exception:
            print('XXX')
            sys.exit(1)

        SELECT_BASE = """
SELECT
    Codice AS old_id,
    REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(Nome)), '  ', ' '), '  ', ' '), '  ', ' ') AS Nome,
    REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(Cognome)), '  ', ' '), '  ', ' '), '  ', ' ') AS Cognome,
    REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(Cognome)), '  ', ' '), '  ', ' '), '  ', ' ')
        + ' '
        +
    REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(Nome)), '  ', ' '), '  ', ' '), '  ', ' ') AS nome,
    REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(Nome)), '  ', ' '), '  ', ' '), '  ', ' ')
        + ' '
        +
    REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(Cognome)), '  ', ' '), '  ', ' '), '  ', ' ') AS username,
    'AAA123' AS VecchiaPasswd,
    NULL AS NuovaPasswd,
    RifCommPref AS negozio
FROM TK_TabDipendenti
"""

        # Build a set of codes to exclude from the INSERTs by reading the
        # existing new-users CSV and (optionally) the older dump. The goal is
        # to only INSERT users whose Codice (old_id) is NOT present in that list.
        exclude_codes = set()

        # Read existing new users CSV to exclude them: csv/nuovi.utenti.csv
        new_users_file = os.path.join(os.path.dirname(__file__), 'csv', 'nuovi.utenti.csv')
        if os.path.exists(new_users_file):
            try:
                with open(new_users_file, newline='', encoding='utf-8') as nf:
                    reader = csv.reader(nf)
                    first = next(reader, None)
                    if first:
                        if not (len(first) == 1 and first[0].strip().lower() == 'old_id'):
                            # first row is data
                            exclude_codes.add(first[0].strip())
                    for row in reader:
                        if not row:
                            continue
                        val = row[0].strip()
                        if val:
                            exclude_codes.add(val)
            except Exception:
                exclude_codes = set()

        # Optionally also read older dump to build a whitelist (IN list)
        dump_codes = None
        dump_file = os.path.join(os.path.dirname(__file__), 'dump', 'orari.dipendenti.sql')
        if os.path.exists(dump_file):
            try:
                with open(dump_file, 'r', encoding='utf-8') as df:
                    codes = set()
                    for line in df:
                        line = line.strip()
                        if not line.upper().startswith('INSERT INTO DIPENDENTI'):
                            continue
                        idx = line.find('VALUES')
                        if idx == -1:
                            continue
                        vals_part = line[idx+6:].strip()
                        if vals_part.startswith('(') and vals_part.endswith(');'):
                            vals_part = vals_part[1:-2]
                        elif vals_part.startswith('(') and vals_part.endswith(')'):
                            vals_part = vals_part[1:-1]
                        parts = []
                        cur = ''
                        in_quote = False
                        escape = False
                        for ch in vals_part:
                            if ch == "'" and not escape:
                                in_quote = not in_quote
                                cur += ch
                                continue
                            if ch == ',' and not in_quote:
                                parts.append(cur.strip())
                                cur = ''
                                continue
                            if ch == '\\' and in_quote:
                                escape = True
                                cur += ch
                                continue
                            cur += ch
                            escape = False
                        if cur:
                            parts.append(cur.strip())
                        if len(parts) >= 4:
                            codice_raw = parts[3]
                            codice = codice_raw.strip()
                            if codice.startswith("'") and codice.endswith("'"):
                                codice = codice[1:-1]
                            if codice and codice.upper() != 'NULL':
                                codes.add(codice)
                    if codes:
                        dump_codes = sorted(codes)
            except Exception:
                dump_codes = None

        # Build SELECT_SQL combining old whitelist (dump_codes) and CSV exclusion
        if dump_codes and exclude_codes:
            quoted_in = ", ".join([f"'{c.replace("'","''")}'" for c in dump_codes])
            quoted_not = ", ".join([f"'{c.replace("'","''")}'" for c in sorted(exclude_codes)])
            SELECT_SQL = SELECT_BASE + f"\nWHERE Codice IN ({quoted_in}) AND Codice NOT IN ({quoted_not})\n"
        elif dump_codes:
            quoted_in = ", ".join([f"'{c.replace("'","''")}'" for c in dump_codes])
            SELECT_SQL = SELECT_BASE + f"\nWHERE Codice IN ({quoted_in})\n"
        elif exclude_codes:
            quoted_not = ", ".join([f"'{c.replace("'","''")}'" for c in sorted(exclude_codes)])
            SELECT_SQL = SELECT_BASE + f"\nWHERE Codice NOT IN ({quoted_not})\n"
        else:
            SELECT_SQL = SELECT_BASE

        candidates = []
        if MSSQL_DRIVER:
            candidates.append(MSSQL_DRIVER)
        candidates.extend(['ODBC Driver 17 for SQL Server', 'ODBC Driver 13 for SQL Server', 'FreeTDS', 'SQL Server'])

        conn = None
        last_err = None
        for drv in candidates:
            try:
                conn_str = f"DRIVER={{{drv}}};SERVER={MSSQL_HOST},{MSSQL_PORT};DATABASE={MSSQL_DB};"
                if MSSQL_USER:
                    conn_str += f"UID={MSSQL_USER};PWD={MSSQL_PASS};"
                else:
                    conn_str += "Trusted_Connection=yes;"
                conn = pyodbc.connect(conn_str, timeout=10)
                break
            except Exception as e:
                last_err = e

        if conn is None:
            print('XXX')
            sys.exit(1)

        cur = conn.cursor()
        cur.execute(SELECT_SQL)
        rows = cur.fetchall()
        colnames = [c[0] for c in cur.description]

        out_csv_dir = os.path.join(os.path.dirname(__file__), 'csv')
        out_dump_dir = os.path.join(os.path.dirname(__file__), 'dump')
        os.makedirs(out_csv_dir, exist_ok=True)
        os.makedirs(out_dump_dir, exist_ok=True)

        csv_path = os.path.join(out_csv_dir, 'orari.gestione_utenti.csv')
        sql_path = os.path.join(out_dump_dir, 'orari.gestione_utenti.sql')

        # CSV headers as requested
        csv_headers = ['id', 'old_id', 'nome', 'username', 'VecchiaPasswd', 'NuovaPasswd', 'ruolo', 'negozio', 'AbilitaInsOrari']

        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=csv_headers)
            writer.writeheader()
            for r in rows:
                old_id = getattr(r, 'old_id') if 'old_id' in colnames else r[0]
                nome = getattr(r, 'nome') if 'nome' in colnames else ''
                username = getattr(r, 'username') if 'username' in colnames else ''
                negozio = getattr(r, 'negozio') if 'negozio' in colnames else None
                writer.writerow({
                    'id': '',
                    'old_id': old_id if old_id is not None else '',
                    'nome': nome if nome is not None else '',
                    'username': username if username is not None else '',
                    'VecchiaPasswd': 'AAA123',
                    'NuovaPasswd': '',
                    'ruolo': 'Dipendente',
                    'negozio': negozio if negozio is not None else '',
                    'AbilitaInsOrari': ''
                })

        with open(sql_path, 'w', encoding='utf-8') as f:
            f.write('-- Dump generato da orario.gestione_utenti.py\n')
            for r in rows:
                old_id = getattr(r, 'old_id') if 'old_id' in colnames else r[0]
                nome = getattr(r, 'nome') if 'nome' in colnames else ''
                username = getattr(r, 'username') if 'username' in colnames else ''
                negozio = getattr(r, 'negozio') if 'negozio' in colnames else None

                def sql_quote(val):
                    if val is None:
                        return 'NULL'
                    s = str(val)
                    s = s.replace("'", "''")
                    return f"'{s}'"

                id_val = 'NULL'
                old_id_sql = sql_quote(old_id)
                nome_sql = sql_quote(nome)
                username_sql = sql_quote(username)
                vecchia_sql = sql_quote('AAA123')
                nuova_sql = 'NULL'
                ruolo_sql = sql_quote('Dipendente')
                negozio_sql = sql_quote(negozio) if negozio not in (None, '') else 'NULL'
                abil_sql = 'NULL'

                line = (
                    'INSERT INTO orari.gestione_utenti '
                    '(id, old_id, nome, username, VecchiaPasswd, NuovaPasswd, ruolo, negozio, AbilitaInsOrari) VALUES '
                    f'({id_val}, {old_id_sql}, {nome_sql}, {username_sql}, {vecchia_sql}, {nuova_sql}, {ruolo_sql}, {negozio_sql}, {abil_sql});\n'
                )
                f.write(line)

        cur.close()
        conn.close()

        print('$$$')
    except Exception:
        print('XXX')
        sys.exit(1)

if __name__ == '__main__':
    main()
