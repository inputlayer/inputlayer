#!/bin/bash
# Generate TLS certificates for InputLayer RPC
#
# This script creates a self-signed CA and server/client certificates
# for local development and testing.
#
# Usage:
#   cd certs && ./generate.sh

set -e

DAYS=365
KEY_SIZE=2048
CERT_DIR="$(dirname "$0")"
cd "$CERT_DIR"

echo "Generating InputLayer TLS Certificates"
echo "======================================="
echo

# 1. Generate CA private key and certificate
echo "1. Generating CA certificate..."
openssl genrsa -out ca.key $KEY_SIZE
openssl req -new -x509 -days $DAYS -key ca.key -out ca.pem \
    -subj "/C=US/ST=California/L=San Francisco/O=InputLayer/OU=Development/CN=InputLayer CA"

# 2. Generate server private key and CSR
echo "2. Generating server certificate..."
openssl genrsa -out server.key $KEY_SIZE
openssl req -new -key server.key -out server.csr \
    -subj "/C=US/ST=California/L=San Francisco/O=InputLayer/OU=Server/CN=localhost"

# Create server extensions file for SAN (Subject Alternative Names)
cat > server_ext.cnf << EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = *.localhost
IP.1 = 127.0.0.1
IP.2 = ::1
EOF

# Sign server certificate with CA
openssl x509 -req -days $DAYS -in server.csr -CA ca.pem -CAkey ca.key \
    -CAcreateserial -out server.pem -extfile server_ext.cnf

# 3. Generate client private key and CSR (optional, for mTLS)
echo "3. Generating client certificate..."
openssl genrsa -out client.key $KEY_SIZE
openssl req -new -key client.key -out client.csr \
    -subj "/C=US/ST=California/L=San Francisco/O=InputLayer/OU=Client/CN=inputlayer-client"

# Create client extensions file
cat > client_ext.cnf << EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
EOF

# Sign client certificate with CA
openssl x509 -req -days $DAYS -in client.csr -CA ca.pem -CAkey ca.key \
    -CAcreateserial -out client.pem -extfile client_ext.cnf

# 4. Clean up temporary files
echo "4. Cleaning up..."
rm -f server.csr client.csr server_ext.cnf client_ext.cnf ca.srl

# 5. Set permissions
chmod 600 *.key
chmod 644 *.pem

echo
echo "Certificates generated successfully!"
echo
echo "Files created:"
echo "  ca.pem      - CA certificate (use for client verification)"
echo "  ca.key      - CA private key (keep secure!)"
echo "  server.pem  - Server certificate"
echo "  server.key  - Server private key"
echo "  client.pem  - Client certificate (for mTLS)"
echo "  client.key  - Client private key (for mTLS)"
echo
echo "Usage:"
echo "  Server: --cert certs/server.pem --key certs/server.key"
echo "  Client: --cert certs/ca.pem"
