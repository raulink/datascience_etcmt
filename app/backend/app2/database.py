from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Crear el engine de SQLAlchemy para conectarse a la base de datos
engine = create_engine('postgresql://postgres:postgres@localhost/simyo')

SesionLocal = sessionmaker(autocomit = False, autoflush=False,bind=engine)

Base = declarative_base()

