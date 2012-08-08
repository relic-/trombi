import trombi
from tornado.ioloop import IOLoop
from tornado import gen

@gen.engine
def main():
    main.server = trombi.Server('http://localhost:5984')
    print('[*] Getting/ Creating db')
    try:
        db=yield gen.Task(main.server.get, 'testdb', create=True)
        database_created(db)
    except Exception as e:
        print(e)
        IOLoop.instance().stop()

main.db = None
main.server = None

def changes(res):
    print('~Changes', res)

def database_created(db):
    main.db = db
    main.db.changes(changes)
    print('[*] Static view')
    view_get(None)
    #main.db.view('test', 'test', view_get)


def view_get(res):
    print(res)
    print('[*] Temporary view')
    mapfn = 'function(doc) { emit(null, doc);}'
    main.db.temporary_view(temp_view, mapfn)

def temp_view(res):
    print(res)
    print('[*] Creating document')
    main.db.set({'testvalue': 'something', 'listattr': [1,2,3,4,5,6, {'text': 'some kind of test'}]}, doc_created)

def doc_created(doc):
    print(doc)
    print('[*] Getting document')
    main.db.get(doc['id'], doc_get)

def doc_get(doc):
    print(doc)
    doc_get.doc=doc
    print('[*] Adding attachment')
    main.db.set_attachment(doc, 'test.txt', 'this is some random text,l o lo lo', set_attachment, type='text/plain')

def set_attachment(res):
    print(res)
    set_attachment.att = res
    print('[*] Getting attachment')
    main.db.get_attachment(res['id'], 'test.txt', get_attachment)

def get_attachment(res):
    print(res)
    print('[*] Deleting attachment')
    main.db.delete_attachment(dict(_id=set_attachment.att['id'], _rev=set_attachment.att['rev']), 'testss.txt', del_attachment)

def del_attachment(res):
    print(res)
    
    print('[*] Deleting document')
    def _del_att(res):
        print(res)
        main.db.changes(changes)
        main.db.changes(changes)
        delete_database()
    main.db.delete(dict(_id=res['id'], _rev=res['rev']),_del_att)

def delete_database():
    def _del(r):
        print(r)
        ioloop.stop()
    print('[*] Deleting database')
    main.server.delete('testdb', _del)

if __name__ == '__main__':
    ioloop = IOLoop.instance()
    ioloop.add_callback(main)
    ioloop.start()
