from sqlalchemy import Boolean, Column, DateTime, ForeignKey, ForeignKeyConstraint, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from .engine import Base

class Deliver(Base):
    __tablename__ = "Deliver"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(255), index=True)
    total_amount = Column(Integer, default=1)
    status = Column(String(255), default="UNKNOWN")
    order_time = Column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self):
        return f"<Deliver(username={self.username}, total_amount={self.total_amount}, status={self.status}, order_time={self.order_time})>"
