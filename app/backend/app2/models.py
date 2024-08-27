from sqlalchemy import Boolean, Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from .database import Base

class Plan(Base):
    __tablename__ = "plans"
    id = Column(Integer, primary_key=True)
    name = Column(String,unique=True, index = True)
    is_active = Column(Boolean)
    activities  = relationship("Activity", back_populates = "owner")

class Activity(Base):

    __tablename__="activities"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    fk_plan = relationship("Plan",back_populates="plans")


