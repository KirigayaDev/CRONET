## Создание RSA ключей для подписи JWT токенов
```bash
openssl genpkey -algorithm RSA -out ./crypt_keys/jwt_keys/private_key.pem -pkeyopt rsa_keygen_bits:2048
openssl rsa -pubout -in ./crypt_keys/jwt_keys/private_key.pem -out ./crypt_keys/jwt_keys/public_key.pem
```