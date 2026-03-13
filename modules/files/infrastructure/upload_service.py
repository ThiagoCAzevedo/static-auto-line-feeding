from .base import FilesBase
import os


class UploadFiles(FilesBase):
    def __init__(self):
        super().__init__()

    def execute(self, file) -> dict:
        path = self.get_excel_path()
        target_path = os.path.join(path, file.filename)

        self.log.debug(f"Uploading file: {file.filename} → {target_path}")

        try:
            content = file.file.read()
            file_size = len(content)

            with open(target_path, "wb") as f:
                f.write(content)

            self.log.debug(f"File written to disk ({file_size} bytes)")
            return {"filename": file.filename, "size": file_size}

        except IOError as e:
            self.log.error(f"I/O error writing file {file.filename}: {str(e)}", exc_info=True)
            raise
        except Exception as e:
            self.log.error(f"Failed to upload file {file.filename}: {str(e)}", exc_info=True)
            raise