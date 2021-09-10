from json import dumps

from pydantic import ValidationError

from flask import Flask, Response, request
from mongodantic import connect, models

connect("mongodb://root:password@mongo_database_flask:27017", "test")


class Book(models.MongoModel):
    name: str
    author: str
    rating: int = 1


app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
def books():
    if request.method == 'GET':
        books = Book.Q.find().data
        return books.json
    if request.method == 'POST':
        name = request.form.get('name')
        author = request.form.get('author')
        try:
            book = Book(name=name, author=author)
            book.save()
            return Response(book.json(), headers={'Content-Type': 'json'})
        except ValidationError as e:
            return Response(e.json(), headers={'Content-Type': 'json'})


@app.route('/', methods=['GET', 'DELETE'])
def book_detail(book_id):
    if request.method == 'GET':
        book = Book.Q.find_one(_id=book_id)
        if book:
            return Response(book.json())
        return Response(
            dumps({'message': 'book does not exist.'}),
            headers={'Content-Type': 'json'},
            status=404,
        )
    if request.method == 'DELETE':
        try:
            book = Book.Q.delete_one(_id=book_id)
            return Response(status=204)
        except ValidationError as e:
            return Response(e.json(), headers={'Content-Type': 'json'})
