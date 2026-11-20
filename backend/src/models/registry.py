"""可复制业务表注册表：应用级逻辑复制按此重放到副库。"""
from src.models.identity import User, Audit
from src.models.batch import Batch
from src.models.mapping import Mapping
from src.models.run import Run, TargetRow
from src.config.database import SwitchOrder

REGISTRY = {
    User.__tablename__: User,
    Audit.__tablename__: Audit,
    Batch.__tablename__: Batch,
    Mapping.__tablename__: Mapping,
    Run.__tablename__: Run,
    TargetRow.__tablename__: TargetRow,
    SwitchOrder.__tablename__: SwitchOrder,
}
