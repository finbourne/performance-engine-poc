from pathlib import Path

from lusid.utilities import ApiClientFactory

secrets_file = Path(__file__).parent.parent.joinpath("secrets.json")

api_factory = ApiClientFactory(
        api_secrets_filename=secrets_file,
        api_url="https://fbn-ci.lusid.com/api",
        app_name="PerformanceDataSetPersistence")
