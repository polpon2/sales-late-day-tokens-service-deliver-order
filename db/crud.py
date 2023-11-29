from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.expression import func
from sqlalchemy.exc import NoResultFound
from . import models


async def create_inventory(db: AsyncSession, username:str, amount: int, status: str):
    db_deliver = models.Deliver(username=username, total_amount=amount, status=status)
    db.add(db_deliver)
    await db.flush()
    return db_deliver


async def change_status(db: AsyncSession, deliver_id: int, status: str):
    result = await db.execute(models.Deliver.__table__.select().where(models.Deliver.id == deliver_id))
    delivery = result.fetchone()
    if delivery:
        await db.execute(models.Deliver.__table__.update().where(models.Deliver.id == deliver_id).values({'status': status}))
        await db.flush()
        return True
    return False
