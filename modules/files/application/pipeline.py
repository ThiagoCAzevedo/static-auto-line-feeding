from common.logger import logger
from modules.files.infrastructure.service import ListExcelFiles, UploadFiles, DeleteFiles


class FilesPipeline:
    def __init__(self):
        self.log = logger("files")

    def list_files(self):
        self.log.info("Executing list files pipeline")
        return ListExcelFiles().execute()

    def upload_file(self, file):
        self.log.info("Executing upload file pipeline")
        return UploadFiles().execute(file)

    def delete_file(self, filename: str):
        self.log.info("Executing delete file pipeline")
        return DeleteFiles().execute(filename)
