from db.database import Database
from helper.WriteAJson import writeAJson
from dataset.pessoa_dataset import dataset as pessoa_dataset
from dataset.carro_dataset import dataset as carro_dataset

pessoas = Database(
    database="database",
    collection="pessoas",
    dataset=pessoa_dataset
)
pessoas.resetDatabase()

carros = Database(
    database="database",
    collection="carros",
    dataset=carro_dataset
)
carros.resetDatabase()

result1 = db.collection.aggregate([
    {"$lookup":
        {
            "from": "pessoas",
            "localField": "dono_id",
            "foreignField": "_id", 
            "as": "dono"  
        }
     },
    {"$group": {"nome": "$nome","_id": "$cliente_id", "total": {"$sum": "$total"} } },
    {"$sort": {"total": -1} },
    {"$unwind": '$_id'},
    {"$project": {
        "_id": 0,
        "cliente": 1,
        "desconto": {
            "$cond": {"if": {"$gte": ["$total", 10]}, "then": 0.1, "else": 0}
        }
    }}

])

writeAJson(result1, "result1")

