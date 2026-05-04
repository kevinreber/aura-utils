from aura_utils import AsyncHTTPClient, __version__


def test_public_api_imports() -> None:
    assert AsyncHTTPClient is not None
    assert __version__ == "0.0.1"
