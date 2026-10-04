import redis.asyncio as redis

from configurations import redis_settings

redis_client: redis.Redis = redis.Redis(password=redis_settings.password,
                                        port=6379,
                                        host='redis',
                                        decode_responses=True,
                                        )
