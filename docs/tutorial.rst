========
Tutorial
========

Basic tutotial for **mongodantic**

Getting started
===============
Before we start, make sure that a copy of MongoDB is running in an accessible
location --- running it locally will be easier, but if that is not an option
then it may be run on a remote server.

At first we  need to tell it how to connect to our
instance of :program:`mongod` For this we use the :func:`~mongodantic.connect`
function. If running locally, the only argument we need to provide is the name
of the MongoDB database to use::

    from mongoengine import *

    connect('mongodb://127.0.0.1:27017", 'test'), 'test_db')

Defining our models
======================

Ticket
---
Define which fields a :class:`Ticket` may have, and what types of data they might store::

    class Ticket(MongoModel):
        name: str
        position: int
        config: dict
