from json import dumps

from fastapi import FastAPI, HTTPException

from mongodantic import connect, models

connect("mongodb://root:password@mongo_database_fastapi:27017", "test")


class Book(models.MongoModel):
    name: str
    author: str
    rating: int = 1


app = FastAPI()


@app.get("/")
async def books():
    books = await Book.AQ.find()
    return books.data


@app.post("/")
async def create_book(book: Book):
    book = await book.save_async()
    return {'id': str(book._id)}


@app.get('/{book_id}')
async def get_book(book_id: str):
    book = await Book.AQ.find_one(_id=book_id)
    if book:
        return book.data
    raise HTTPException(status_code=404, detail=f'Obejct does not exist')


@app.delete('/{book_id}')
async def delete_book(book_id: str):
    await Book.AQ.delete_one(_id=book_id)
    return None
