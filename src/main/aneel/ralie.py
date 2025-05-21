import requests
import psycopg2
from io import StringIO

class DadosAneel:
    def __init__(self):
        self.base_url = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search"
        self.resource_id = "4a615df8-4c25-48fa-bbea-873a36a79518"
        self.limit = 10000

    def coletar_dados(self):
        self.limpar_tabela()  # Apaga a tabela antes de começar a inserir dados

        for offset in range(0, 400001, self.limit):
            url = f"{self.base_url}?resource_id={self.resource_id}&limit={self.limit}&offset={offset}"
            print(f"Requisitando: {url}")
            response = requests.get(url)
            data = response.json()

            records = data.get("result", {}).get("records", [])
            if not records:
                print(f"Nenhum dado retornado no offset {offset}")
                break

            ultimo_id = records[-1].get("_id", 0)
            limite = offset - self.limit

            if ultimo_id < limite:
                print(f"Parando a coleta, _id {ultimo_id} < {limite}")
                break

            self.incluir_bd(records)

        print("Todos os lotes foram processados com sucesso.")

    def limpar_tabela(self):
        try:
            conn = psycopg2.connect(
                host="localhost",
                dbname="postgres",
                user="postgres",
                password="admin"
            )
            with conn.cursor() as cursor:
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS public.ralie_usina (
    id SERIAL PRIMARY KEY,  
    datralie DATE,
    idenucleoceg VARCHAR(50),
    codceg VARCHAR(50),
    sigufprincipal CHAR(2),
    dscorigemcombustivel VARCHAR(100),
    sigtipogeracao VARCHAR(50),
    nomempreendimento VARCHAR(255),
    mdapotenciaoutorgadakw NUMERIC(12,3),
    dscpropriregimepariticipacao VARCHAR(1024),
    dsctipoconexao VARCHAR(100),
    nomconexao VARCHAR(100),
    mdatensaoconexao NUMERIC(10,2),
    nomempresaconexao VARCHAR(255),
    numcnpjempresaconexao CHAR(14),
    dscviabilidade VARCHAR(255),
    dscsituacaoobra VARCHAR(255),
    dscjustificativaprevisao TEXT,
    dsccomercializacaoenergia VARCHAR(255),
    dscsistema VARCHAR(100),
    datconclusaotransporterealizado DATE,
    dscsituacaocronograma VARCHAR(255),
    idccomplexo VARCHAR(50),
    nomcomplexo VARCHAR(255),
    dscsituacaolp VARCHAR(100),
    dscsituacaoli VARCHAR(100),
    dscsituacaolo VARCHAR(100),
    nomsituacaoparacesso VARCHAR(255),
    dscsitccd VARCHAR(100),
    dscsitcct VARCHAR(100),
    dscsituacaocusd VARCHAR(100),
    dscsitcust VARCHAR(100),
    dscatooutorga VARCHAR(100),
    dscnumeroato VARCHAR(100),
    nomorgaooutorgante VARCHAR(255),
    dsctipooutorga VARCHAR(100)
)
""")
            conn.commit()
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM ralie_usina;")
            conn.commit()
            print("Tabela limpa com sucesso antes da inserção.")
        except Exception as e:
            print("Erro ao limpar tabela:", e)
        finally:
            conn.close()

    def incluir_bd(self, records):
        conn = psycopg2.connect(
            host="localhost",
            dbname="postgres",
            user="postgres",
            password="admin"
        )
        try:
            buffer = StringIO()

            for record in records:
                buffer.write(
                    "\t".join([
                        self.format_field(record.get("DatRalie")),
                        self.format_field(record.get("IdeNucleoCEG")),
                        self.format_field(record.get("CodCEG")),
                        self.format_field(record.get("SigUFPrincipal")),
                        self.format_field(record.get("DscOrigemCombustivel")),
                        self.format_field(record.get("SigTipoGeracao")),
                        self.format_field(record.get("NomEmpreendimento")),
                        self.format_numeric_field(record.get("MdaPotenciaOutorgadaKw")),
                        self.format_field(record.get("DscPropriRegimePariticipacao")),
                        self.format_field(record.get("DscTipoConexao")),
                        self.format_field(record.get("NomConexao")),
                        self.format_numeric_field(record.get("MdaTensaoConexao")),
                        self.format_field(record.get("NomEmpresaConexao")),
                        self.format_field(record.get("NumCnpjEmpresaConexao")),
                        self.format_field(record.get("DscViabilidade")),
                        self.format_field(record.get("DscSituacaoObra")),
                        self.format_field(record.get("DscJustificativaPrevisao")),
                        self.format_field(record.get("DscComercializacaoEnergia")),
                        self.format_field(record.get("DscSistema")),
                        self.format_field(record.get("DatConclusaoTransporteRealizado")),
                        self.format_field(record.get("DscSituacaoCronograma")),
                        self.format_field(record.get("IdcComplexo")),
                        self.format_field(record.get("NomComplexo")),
                        self.format_field(record.get("DscSituacaoLP")),
                        self.format_field(record.get("DscSituacaoLI")),
                        self.format_field(record.get("DscSituacaoLO")),
                        self.format_field(record.get("NomSituacaoParaAcesso")),
                        self.format_field(record.get("DscSitCCD")),
                        self.format_field(record.get("DscSitCCT")),
                        self.format_field(record.get("DscSituacaoCUSD")),
                        self.format_field(record.get("DscSitCUST")),
                        self.format_field(record.get("DscAtoOutorga")),
                        self.format_field(record.get("DscNumeroAto")),
                        self.format_field(record.get("NomOrgaoOutorgante")),
                        self.format_field(record.get("DscTipoOutorga")),
                    ]) + "\n"
                )

            buffer.seek(0)

            with conn.cursor() as cursor:
                cursor.copy_expert(
                    sql="""
                        COPY ralie_usina (
                            DatRalie, IdeNucleoCEG, CodCEG, SigUFPrincipal, DscOrigemCombustivel, SigTipoGeracao,
                            NomEmpreendimento, MdaPotenciaOutorgadaKw, DscPropriRegimePariticipacao, DscTipoConexao,
                            NomConexao, MdaTensaoConexao, NomEmpresaConexao, NumCnpjEmpresaConexao, DscViabilidade,
                            DscSituacaoObra, DscJustificativaPrevisao, DscComercializacaoEnergia, DscSistema,
                            DatConclusaoTransporteRealizado, DscSituacaoCronograma, IdcComplexo, NomComplexo,
                            DscSituacaoLP, DscSituacaoLI, DscSituacaoLO, NomSituacaoParAcesso, DscSitCCD, DscSitCCT,
                            DscSituacaoCUSD, DscSitCUST, DscAtoOutorga, DscNumeroAto, NomOrgaoOutorgante, DscTipoOutorga
                        ) FROM STDIN WITH (FORMAT text)
                    """,
                    file=buffer
                )
            conn.commit()
            print("Lote inserido com sucesso!")
        except Exception as e:
            print("Erro ao inserir lote:", e)
        finally:
            conn.close()

    def format_field(self, value):
        if not value or str(value).strip() == "":
            return "\\N"
        return self.escape_csv(str(value))

    def format_numeric_field(self, value):
        if not value or str(value).strip() == "":
            return "\\N"
        return str(value).replace(",", ".")

    def escape_csv(self, value):
        return value.replace("\t", " ").replace("\n", " ").replace("\r", " ")



