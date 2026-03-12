from .base import FilesBase
import os


class DeleteFiles(FilesBase):
    def __init__(self):
        super().__init__()

    def execute(self, filename: str) -> dict:
        path = self.get_excel_path()
        file_path = os.path.join(path, filename)

        self.log.debug(f"Deleting file: {filename} ({file_path})")

        try:
            if not os.path.exists(file_path):
                self.log.warning(f"File not found: {filename}")
                raise FileNotFoundError(f"File '{filename}' does not exist")

            os.remove(file_path)
            self.log.debug(f"File deleted from disk")
            return {"message": f"File '{filename}' successfully removed."}

        except FileNotFoundError as e:
            self.log.error(f"File not found: {filename}", exc_info=True)
            raise
        except OSError as e:
            self.log.error(f"OS error deleting file {filename}: {str(e)}", exc_info=True)
            raise
        except Exception as e:
            self.log.error(f"Failed to delete file {filename}: {str(e)}", exc_info=True)
            raise