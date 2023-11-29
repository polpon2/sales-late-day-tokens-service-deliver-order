from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.expression import func
from sqlalchemy.exc import NoResultFound
from . import models


async def create_inventory(db: AsyncSession, username:str, amount: int, status: str):
    db_deliver = models.Deliver(username=username, total_amount=amount, status=status)
    db.add(db_deliver)
    await db.flush()
    return db_deliver
