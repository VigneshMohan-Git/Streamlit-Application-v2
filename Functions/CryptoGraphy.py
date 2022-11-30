import base64
import zlib
from pathlib import Path

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization,hashes
from cryptography.hazmat.primitives.asymmetric import padding

from cryptography.fernet import Fernet

def generateFernetKey(keyPath):
    key = Fernet.generate_key()
    with open(keyPath, "wb") as key_file:
        key_file.write(key)

def load_Fernetkey(keyPath):
    """
    Loads the key from the current directory named `key.key`
    """
    return open(keyPath, "rb").read()

def encryptFile(key,file_path):
    f = Fernet(key)

    # with open(file_path, 'rb') as original_file:
    #     original = original_file.read()

    encrypted = f.encrypt(file_path)

    return encrypted

    # with open(temp_path, 'wb') as encrypted_file:
    #     encrypted_file.write(encrypted)


def file_decrypt(key, encrypted):
    f = Fernet(key)
    decrypted = f.decrypt(encrypted)
    return decrypted



def readKey(path,key_Type):
    with open(path, "rb") as key_file:
        if key_Type=='private':
           private_key = serialization.load_pem_private_key(
           key_file.read(),
           password=None,
           backend=default_backend()
           )
           return private_key
        if key_Type=='public':
           public_key=serialization.load_pem_public_key(key_file.read(),
           backend=default_backend())
           return public_key

def encrypt(unencrypted_file, public_key,tempPath,chunk_size=100):
    unencrypted_path = Path(unencrypted_file)
    # unencrypted_sufix = unencrypted_path.with_suffix('.dat')
    unencrypted_file_bytes = unencrypted_path.read_bytes()
    public_key = Path(public_key)
    publicKey=readKey(public_key,'public')

    #blob = zlib.compress(unencrypted_file_bytes)

    #chunk_size = 470
    offset = 0
    end_loop = False
    encrypted = bytearray()

    while not end_loop:
        chunk = unencrypted_file_bytes[offset:offset + chunk_size]

        if len(chunk) % chunk_size != 0:
            end_loop = True
            # chunk += b" " * (chunk_size - len(chunk))
            chunk += bytes(chunk_size - len(chunk))
        encrypted += publicKey.encrypt(
                     chunk,
                     padding.OAEP(
                     mgf=padding.MGF1(algorithm=hashes.SHA256()),
                    algorithm=hashes.SHA256(),
                    label=None
                     ))

        offset += chunk_size

        with open(tempPath, 'wb') as encrypted_file:
            encrypted_file.write(base64.b64encode(encrypted))


def decrypt_blob(encrypted_file_path, private_key, file_save_path, filename,file_type,chunk_size=64*1024):
    encrypted_file_path = Path(encrypted_file_path)
    private_key = Path(private_key)

    publicKey = readKey(private_key, 'private')

    with open(encrypted_file_path) as f:
        data = f.read()

    encrypted_blob = base64.b64decode(data)
    offset = 0
    decrypted = bytearray()

    while offset < len(encrypted_blob):
        chunk = encrypted_blob[offset: offset + chunk_size]
        decrypted += private_key.decrypt(
                     chunk,
                     padding.OAEP(
                     mgf=padding.MGF1(algorithm=hashes.SHA256()),
                     algorithm=hashes.SHA256(),
                     label=None
                     )
                      )
        offset += chunk_size

    data = zlib.decompress(decrypted)
    with open(file_save_path + filename + '.'+file_type, 'wb') as decrypted_file:
        decrypted_file.write(data)