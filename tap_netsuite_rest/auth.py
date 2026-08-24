import secrets
from oauthlib import oauth1

def _generate_oauth_nonce() -> str:
    """Return a unique OAuth1 nonce that does not depend on the request timestamp."""
    return secrets.token_hex(16)


class NetsuiteOAuth1Client(oauth1.Client):
    """Mint a fresh oauth_nonce on every sign so retries and concurrent requests never reuse one."""

    def get_oauth_params(self, request):
        pinned_nonce = self.nonce
        if pinned_nonce is None:
            self.nonce = _generate_oauth_nonce()
        try:
            return super().get_oauth_params(request)
        finally:
            self.nonce = pinned_nonce