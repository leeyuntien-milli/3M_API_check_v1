import base64
import datetime
import uuid
from typing import Optional, Tuple
import jwt
import requests
from cryptography import x509
from cryptography.hazmat.primitives import hashes

# list of constants
BASE_URL_3M_GPCS: str = 'http://gpcs.3m.com' # should use http
OATH_URL_3M_GPCS: str = 'https://gpcs.3m.com/GpcsSts/oauth/token' # should use secured http
BASE_PAYLOAD_FIELD_3M_GPCS: Tuple[str, str, str, str] = ('jti', 'iat', 'exp', 'aud')
BASE_HEADER_FIELD_3M_GPCS: Tuple[str, str] = ('x5t', 'kid')
OATH_FIELD_3M_GPCS: Tuple[str, str] = ('grant_type', 'assertion')
OATH_GRANT_TYPE_3M_GPCS: str = 'jwt-bearer'
HTTP_RES_SUCC_CODE: int = 200
OATH_EXCEPTION_MSG: str = 'STS authentication error'
OATH_RES_FIELD_3M_GPCS: str = 'access_token'
ENCRYPT_ALGO_3M_GPCS: str = 'RS256'
MAX_AUTH_DUR_MIN: int = 10
AUTH_PUB_SUF: str = '_cert.pem'
AUTH_PRI_SUF: str = '_key.pem'
AUTH_CODESET: str = 'utf-8'

def generate_gpcs_auth_token(
    public_key: bytes,
    private_key: bytes,
    duration_minutes: Optional[int] = MAX_AUTH_DUR_MIN
) -> str:
    """This method returns an authorization token from 3M GPCS, given the public and private key pair.
    
    Input Arguments: public_key, byte stream from a certificate file that corresponds to a key file, with default encryption algorithm RS256
                     private_key, byte stream from a key file that corresponds to a certificate file, with default encryption algorithm RS256
                     [optional] duration_minutes, default to MAX_AUTH_DUR_MIN minutes, integer specifying how long the authorization key is valid
    Returns: authorization token, string stream representing an authorization token
    """
    cert = x509.load_pem_x509_certificate(public_key)
    fingerprint = cert.fingerprint(hashes.SHA1())
    kid = fingerprint.hex()
    x5t = base64.urlsafe_b64encode(fingerprint).decode(AUTH_CODESET)

    now_time = datetime.datetime.utcnow()
    payload: dict = dict(zip(
        BASE_PAYLOAD_FIELD_3M_GPCS,
        (str(uuid.uuid4()),
            now_time,
            now_time + datetime.timedelta(minutes=duration_minutes),
            BASE_URL_3M_GPCS
        )
    ))
    headers: dict = dict(zip(
        BASE_HEADER_FIELD_3M_GPCS,
        (x5t, kid)
    ))
    auth_token: str = jwt.encode(
        payload=payload,
        key=private_key,
        algorithm=ENCRYPT_ALGO_3M_GPCS,
        headers=headers
    )

    return auth_token

def generate_gpcs_auth_token_from_cert_files(
    cert_basedir: str,
    cert_basename: str,
    duration_minutes: Optional[int] = MAX_AUTH_DUR_MIN
) -> str:
    """This method returns an authorization token from 3M GPCS to use locally, given path and file name prefix of the public and private key pair.
    
    Input Arguments: cert_basedir, string specifying path of the files of certificates
                     cert_basename, string specifying file name prefix of certificates
                     [optional] duration_minutes, default to MAX_AUTH_DUR_MIN minutes, integer specifying how long the authorization key is valid
    Returns: authorization token, string stream representing an authorization token
    """
    cert_prefix: str = cert_basedir + '/' + cert_basename
    private_key: bytes = bytes()
    public_key: bytes = bytes()
    with open(cert_prefix + AUTH_PRI_SUF, mode='rb') as private_key_file:
        private_key = private_key_file.read()
    with open(cert_prefix + AUTH_PUB_SUF, mode='rb') as public_key_file:
        public_key = public_key_file.read()

    auth_token: str = generate_gpcs_auth_token(
        public_key,
        private_key,
        duration_minutes
    )
    return auth_token

def get_gpcs_oath_access_token(auth_token: str) -> str:
    """This method returns an OATH token from 3M GPCS, given the authorization token.
    
    Input Arguments: auth_token, string stream from an authorization token
    Returns: OATH token, string stream representing an OATH token
    """
    response = requests.post(
        OATH_URL_3M_GPCS,
        json = dict(zip(
            OATH_FIELD_3M_GPCS,
            (OATH_GRANT_TYPE_3M_GPCS, auth_token)
        ))
    )
    if response.status_code != HTTP_RES_SUCC_CODE:
        raise Exception(OATH_EXCEPTION_MSG, response.status_code)
    access_token = response.json()[OATH_RES_FIELD_3M_GPCS]
    return access_token
