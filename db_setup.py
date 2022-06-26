import datetime
import sqlalchemy as db
from decouple import config


engine = db.create_engine(config("DATABASE_URL"))
connection = engine.connect()
metadata = db.MetaData()
tabla_registros = db.Table("registros", metadata,
                           db.Column("id_registro", db.Integer(), primary_key=True, autoincrement=True),
                           db.Column("fecha", db.DateTime(), default=datetime.datetime.utcnow),
                           db.Column('cod_localidad', db.String(255)),
                           db.Column('id_provincia', db.String(255)),
                           db.Column('id_departamento', db.String(255)),
                           db.Column('id_categoria', db.String(255)),
                           db.Column('provincia', db.String(255)),
                           db.Column('localidad', db.String(255)),
                           db.Column('nombre', db.String(255)),
                           db.Column('domicilio', db.String(255)),
                           db.Column('codigo_postal', db.String(255)),
                           db.Column('numero_tel', db.String(255)),
                           db.Column('mail', db.String(255)),
                           db.Column('web', db.String(255))
                           )

tabla_registros_tot_categoria = db.Table("registros_categoria", metadata,
                                        db.Column("id_registro", db.Integer(), primary_key=True, autoincrement=True),
                                        db.Column("fecha", db.DateTime(), default=datetime.datetime.utcnow),
                                        db.Column('categoria', db.String(255)),
                                        db.Column('cant_total', db.String(255))
)


tabla_registros_cines = db.Table("registros_cines", metadata,
                           db.Column("id_registro", db.Integer(), primary_key=True, autoincrement=True),
                           db.Column("fecha", db.DateTime(), default=datetime.datetime.utcnow),
                           db.Column('Provincia', db.String(255)),
                           db.Column('Pantallas', db.String(255)),
                           db.Column('Butacas', db.String(255)),
                           db.Column('espacio_INCAA', db.String(255))
                           )



metadata.create_all(engine)
