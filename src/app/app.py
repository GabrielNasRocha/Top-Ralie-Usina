import os
from flask import Flask, jsonify
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

sql_select = """
        SELECT codceg, sigufprincipal, dscorigemcombustivel, nomempreendimento, mdapotenciaoutorgadakw, numcnpjempresaconexao, dscsistema, dscsituacaocronograma 
            FROM public.ralie_usina
            ORDER BY mdapotenciaoutorgadakw DESC
            LIMIT 5
    """
autoload = load_dotenv()

app = Flask(__name__)

def get_connection():

    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "postgres"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASS", "admin"),
    )


@app.route("/top-usinas", methods=["GET"])
def listar_dados():

    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql_select)
            resultados = cur.fetchall()
    return jsonify(resultados)

if __name__ == "__main__":

    app.run(debug=True, host="localhost", port=8000)