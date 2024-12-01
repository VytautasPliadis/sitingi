from datetime import datetime
from typing import Optional, List
from sqlmodel import Field, SQLModel, Relationship

class Objektai(SQLModel, table=True):
    obj_id: int = Field(default=None, primary_key=True)
    obj_numeris: int = Field(unique=True)
    busenos: List["Busenos"] = Relationship(back_populates="objektas")
    planai: List["Planai"] = Relationship(back_populates="objektas")

class Busenos(SQLModel, table=True):
    busena_id: int = Field(default=None, primary_key=True)
    obj_id: int = Field(foreign_key="objektai.obj_id")
    busena: int
    busena_nuo: datetime
    busena_iki: Optional[datetime] = Field(default=None, nullable=True)
    objektas: "Objektai" = Relationship(back_populates="busenos")

class Busena_kodai(SQLModel, table=True):
    busena_kodas: int = Field(default=None, primary_key=True)
    busena_tekstas: str

class Planai(SQLModel, table=True):
    pln_id: int = Field(default=None, primary_key=True)
    obj_id: int = Field(foreign_key="objektai.obj_id")
    pln_galioja_nuo: datetime
    pln_galioja_iki: Optional[datetime] = Field(default=None, nullable=True)
    objektas: "Objektai" = Relationship(back_populates="planai")
