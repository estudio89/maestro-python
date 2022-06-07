from django.apps import AppConfig


class CoreConfig(AppConfig):
    name = 'core'

    def ready(self):
        import firebase_admin
        from firebase_admin import credentials
        from pathlib import Path
        import os

        # Build paths inside the project like this: BASE_DIR / 'subdir'.
        BASE_DIR = Path(__file__).resolve().parent.parent
        os.environ["FIRESTORE_EMULATOR_HOST"] = "0.0.0.0:7070"

        directory = os.path.dirname(__file__)
        cred = credentials.Certificate(
            BASE_DIR / "serviceAccountKey.json"
        )
        firebase_admin.initialize_app(cred)
