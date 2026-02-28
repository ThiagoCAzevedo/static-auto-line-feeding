import os
from os import listdir
from os.path import isfile, join
from .base import FilesBase


class ListExcelFiles(FilesBase):
    def __init__(self):
        super().__init__()
        self.log.info("Inicializando ListExcelFiles")

    def execute(self):
        path = self.get_excel_path()
        self.log.info(f"Listando arquivos no diretório: {path}")

        try:
            files = [f for f in listdir(path) if isfile(join(path, f))]
            self.log.info(f"Arquivos encontrados: {len(files)}")
            return files

        except Exception:
            self.log.error("Erro ao listar arquivos no EXCEL_PATH", exc_info=True)
            raise


class UploadFiles(FilesBase):
    def __init__(self):
        super().__init__()
        self.log.info("Inicializando UploadFiles")

    def execute(self, file):
        path = self.get_excel_path()
        target_path = f"{path}/{file.filename}"

        self.log.info(f"Iniciando upload de arquivo: {file.filename}")

        try:
            content = file.file.read()

            with open(target_path, "wb") as f:
                f.write(content)

            self.log.info(f"Upload concluído: {file.filename} ({len(content)} bytes)")
            return {"filename": file.filename, "size": len(content)}

        except Exception:
            self.log.error(f"Erro ao fazer upload do arquivo {file.filename}", exc_info=True)
            raise


class DeleteFiles(FilesBase):
    def __init__(self):
        super().__init__()
        self.log.info("Inicializando DeleteFiles")

    def execute(self, filename: str):
        path = self.get_excel_path()
        file_path = os.path.join(path, filename)

        self.log.info(f"Solicitado delete do arquivo: {filename}")

        try:
            os.remove(file_path)
            self.log.info(f"Arquivo removido com sucesso: {filename}")
            return {"message": f"File '{filename}' succesfully removed."}

        except FileNotFoundError:
            self.log.error(f"Arquivo '{filename}' não encontrado para remoção", exc_info=True)
            raise

        except Exception:
            self.log.error(f"Erro ao remover arquivo '{filename}'", exc_info=True)
            raise
