FROM python:3.12.15-alpine

#для healthcheck
RUN apk add --no-cache curl


WORKDIR /auth_service

# Оптимизация пересборки проекта в случаях когда библиотеки не изменились
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY . .

RUN addgroup -g 1000 auth_service && \
    adduser -u 1000 -G auth_service -D -s /bin/sh auth_service && \
    chown -R auth_service:auth_service /auth_service &&  chmod -R 775 /auth_service

USER auth_service

EXPOSE 443

CMD ["python3", "main.py"]