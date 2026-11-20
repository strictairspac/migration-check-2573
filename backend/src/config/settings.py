import os

def _split_urls(value):
 parts=[p.strip() for p in value.split(',') if p.strip()]
 if not parts:raise RuntimeError('缺少数据库连接配置')
 return parts

# 主库（当前写库）与副库（候选写库）。生产由 Compose 注入两个 PostgreSQL DSN；
# 本地无 Docker 时用 sqlite 双文件模拟两台独立数据库节点。
_db=os.getenv('DATABASE_URL','postgresql+psycopg://migrate:migratepass@db:5432/migrationcheck')
_standby=os.getenv('STANDBY_DATABASE_URL','')
if _standby:
 PRIMARY_DATABASE_URL,STANDBY_DATABASE_URL=_db,_standby
else:
 _dir=os.getenv('SQLITE_DATA_DIR','/tmp/migration-check-db')
 os.makedirs(_dir,exist_ok=True)
 PRIMARY_DATABASE_URL='sqlite:///'+os.path.join(_dir,'primary.db')
 STANDBY_DATABASE_URL='sqlite:///'+os.path.join(_dir,'standby.db')

REDIS_URL=os.getenv('REDIS_URL','')
JWT_SECRET=os.environ.get('JWT_SECRET','migration-check-local-demo-secret-change-before-use')
