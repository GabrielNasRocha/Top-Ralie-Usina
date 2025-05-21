import asyncio
from src.main.aneel.ralie import DadosAneel

print("Iniciando o projeto de Ralie")
class Main:
    @staticmethod
    def execute():
        app = DadosAneel()
        app.coletar_dados()

    @staticmethod
    async def agendar_periodicamente(intervalo_em_segundos, func):
        while True:
            func()
            await asyncio.sleep(intervalo_em_segundos)

    @staticmethod
    def main():
        asyncio.run(agendar_periodicamente(600, minha_funcao))

if __name__ == "__main__":
    Main().execute()