"""队列与协调状态客户端：优先真实 Redis；本地无 Redis 时退化为进程内 fakeredis。

fakeredis 支持 rpush/blpop/incr/set/get 等全部用到的命令，行为与单节点 Redis 一致；
生产 Compose 仍注入 REDIS_URL，走真实 Redis（AOF 持久化）。
"""
import logging

from src.config.settings import REDIS_URL

if REDIS_URL:
    import redis as _redis
    queue = _redis.Redis.from_url(REDIS_URL, decode_responses=True)
    queue.ping()
else:
    import fakeredis
    queue = fakeredis.FakeStrictRedis(decode_responses=True)
    logging.getLogger('queue').warning('REDIS_URL 未配置，使用进程内 fakeredis（仅本地演示/测试）')

# 统一 decode_responses=True：所有取值均为 str，业务层不再处理 bytes
