KEY_DIR="."
PRIVATE_KEY="$KEY_DIR/private_key.pem"
PUBLIC_KEY="$KEY_DIR/public_key.pem"

openssl genpkey \
    -algorithm RSA \
    -out "$PRIVATE_KEY" \
    -pkeyopt rsa_keygen_bits:2048

openssl rsa \
    -pubout \
    -in "$PRIVATE_KEY" \
    -out "$PUBLIC_KEY"

chmod 600 "$PRIVATE_KEY"
chmod 644 "$PUBLIC_KEY"
