from fastapi import Depends, FastAPI, HTTPException

from sqlalchemy.orm import Session

from . import crud,models,schemas

from .database import SessionLocal, engine

models.Base.metadata.create_all(bind=engine)

app = FastAPI()


#Dependencia
def get_db():
    db = SessionLocal()
    try: 
        yield db
    finally:
        db.close()

@app.post("/plans/",response_model=schemas.Plan)
def create_plan(plan: schemas.PlanCreate, db:Session = Depends(get_db)):
    db_plan = crud.get_plans(db,)
    if db_plan:
        raise HTTPException(status_code=400, detail="Ya existe registro")
    return crud.create_plan(db,plan)


