from pydantic import BaseModel

class Activity(BaseModel):
    id:int
    name : str
    class Config:
        orm_mode = True

class Plan(BaseModel):
    id:int
    activities : list[Activity]=[]
    class Config:
        orm_mode = True



