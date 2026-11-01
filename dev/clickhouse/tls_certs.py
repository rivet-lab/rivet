"""Write the stand ClickHouse's private test CA and its HTTPS server certificate to dev/clickhouse/tls/."""

import datetime
import ipaddress
import pathlib

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

OUT = pathlib.Path(__file__).resolve().parent / "tls"
START = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
END = datetime.datetime(2126, 1, 1, tzinfo=datetime.timezone.utc)


def name(cn: str) -> x509.Name:
    """A subject holding one common name."""
    return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])


def pem_key(key: ec.EllipticCurvePrivateKey) -> bytes:
    """The key as unencrypted PKCS#8 PEM."""
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


def main() -> None:
    """Issue the CA and the server certificate and write ca.pem, server.pem and server.key."""
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca = (
        x509.CertificateBuilder()
        .subject_name(name("rivet stand test CA"))
        .issuer_name(name("rivet stand test CA"))
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(START)
        .not_valid_after(END)
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), critical=False)
        .sign(ca_key, hashes.SHA256())
    )
    key = ec.generate_private_key(ec.SECP256R1())
    server = (
        x509.CertificateBuilder()
        .subject_name(name("localhost"))
        .issuer_name(ca.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(START)
        .not_valid_after(END)
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
            ),
            critical=False,
        )
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), critical=False
        )
        .sign(ca_key, hashes.SHA256())
    )
    OUT.mkdir(exist_ok=True)
    (OUT / "ca.pem").write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    (OUT / "server.pem").write_bytes(server.public_bytes(serialization.Encoding.PEM))
    (OUT / "server.key").write_bytes(pem_key(key))


if __name__ == "__main__":
    main()
