import sys, datetime, uuid, re
import pymongo, bson
from bson import Int64, Decimal128, Binary, ObjectId, Timestamp, MinKey, MaxKey, Regex, Code, DBRef, SON
from bson.datetime_ms import DatetimeMS
from bson.decimal128 import Decimal128
import struct
D=lambda ms: DatetimeMS(ms)
NEG0=struct.unpack('<d',struct.pack('<Q',1<<63))[0]
C={
'dbl':[0.0,NEG0,1.5,float('nan'),float('inf'),float('-inf'),5e-324,2.2250738585072014e-308,1.7976931348623157e308,-1.7976931348623157e308,0.1,9007199254740993.0,None],
'i32':[0,1,-1,2147483647,-2147483648,None],
'i64':[Int64(0),Int64(9223372036854775807),Int64(-9223372036854775808),Int64(2147483648),Int64(-2147483649),Int64(9007199254740993),None],
'dec':[Decimal128('9.999999999999999999999999999999999E+6144'),Decimal128('-9.999999999999999999999999999999999E+6144'),Decimal128('1E-6176'),Decimal128('0'),Decimal128('-0'),Decimal128('0E-6176'),Decimal128('NaN'),Decimal128('Infinity'),Decimal128('-Infinity'),Decimal128('1234567890123456789012345678901234'),Decimal128('0.1'),Decimal128('12345678901234567890.12345678901234'),Decimal128('1.0E+40'),None],
'str':['',' ','plain','  lead and trail  ','a\x00b','tab\tnl\ncr\r','q\'"\\,;','\U0001F600\U0001F4A9 \u00e9\u4e2d','x'*1000000,'\ud7ff'+'\ue000',None],
'bin':[Binary(b'',0),Binary(b'\x00',0),Binary(bytes(range(256)),0),Binary(b'\xde\xad\xbe\xef',0),Binary(b'\xff'*100000,0),Binary(uuid.UUID('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380011').bytes,4),Binary(uuid.UUID('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380011').bytes,3),Binary(b'\x01\x02',128),Binary(b'\x01\x02',2),Binary(b'\xaa'*16,5),Binary(b'\x01',9),None],
'oid':[ObjectId('000000000000000000000000'),ObjectId('ffffffffffffffffffffffff'),ObjectId('507f1f77bcf86cd799439011'),None],
'bool':[True,False,None],
'date':[D(0),D(1),D(-1),D(-62135596800000),D(-62135596800001),D(253402300799999),D(253402300800000),D(-8640000000000000),D(8640000000000000),D(9223372036854775807),D(-9223372036854775808),D(-2208988800000),D(-9223372036854),D(9223372036854),D(-1),D(951827696789),D(1583020000123),None],
'ts':[Timestamp(0,1),Timestamp(1,1),Timestamp(4294967295,4294967295),Timestamp(1700000000,5),None],
'arr':[[],[1,2,3],[[1],[2,[3,[4]]]],[1,'a',None,True,1.5],[None],[[]],[{'a':1},{'a':'x'}],[1,2**40],[Decimal128('1.5'),Decimal128('NaN')],[ObjectId('507f1f77bcf86cd799439011')],[float('nan'),float('inf')],['x','y'],[Binary(b'\x01',0)],[D(0)],None],
'doc':[{},{'a':1},{'a':{'b':{'c':{'d':None}}}},{'a':[1,{'b':2}],'c':'x'},{'':1},{'a.b':1} if False else {'k':'v'},{'big':Int64(9223372036854775807),'f':float('nan'),'dec':Decimal128('1E+10'),'dt':D(0),'oid':ObjectId('507f1f77bcf86cd799439011'),'bin':Binary(b'\x00',0)},None],
'rx':[Regex('^a.*b$','imxs'),Regex('',''),Regex('a\\/b','i'),Regex('\U0001F600','u'),None],
'mm':[MinKey(),MaxKey(),None],
'js':[Code('function(){return 1}'),Code('x',{'a':1}),None],
'nul':[None,'x'],
}
mixed=[1,Int64(2),2.5,'s',True,None,{'a':1},[1],Decimal128('1'),D(0),ObjectId('507f1f77bcf86cd799439011'),Binary(b'\x01',0),Timestamp(1,1),Regex('a','i'),float('nan'),Int64(9223372036854775807),2147483647]
from bson.raw_bson import RawBSONDocument
def rawdoc(i,t,name,payload):
    body=bytes([t])+name+b'\x00'+payload
    e=struct.pack('<i',0)
    inner=b'\x10_id\x00'+struct.pack('<i',i)+body+b'\x00'
    return RawBSONDocument(struct.pack('<i',len(inner)+4)+inner)
def dep():
    s=b'sym\x00'
    return [rawdoc(1,0x06,b'v',b''),rawdoc(2,0x0E,b'v',struct.pack('<i',len(s))+s),rawdoc(3,0x0C,b'v',struct.pack('<i',4)+b'ns\x00'+b'\x00'*0+bytes.fromhex('507f1f77bcf86cd799439011')) if False else rawdoc(3,0x0C,b'v',struct.pack('<i',3)+b'ns\x00'+bytes.fromhex('507f1f77bcf86cd799439011'))]
def seed(port):
    c=pymongo.MongoClient(f'mongodb://127.0.0.1:{port}/',serverSelectionTimeoutMS=3000)
    c.drop_database('tc_edge_mongo'); db=c['tc_edge_mongo']
    for n,vals in C.items():
        docs=[{'_id':i+1,'v':v} for i,v in enumerate(vals)]
        if n=='nul':
            docs=[{'_id':1,'v':None},{'_id':2},{'_id':3,'v':'x'}]
        else:
            docs.append({'_id':len(vals)+1})
        db['tc_edge_mongo_'+n].insert_many(docs)
    for n,docs in (('dep',dep()),('keydot',[{'_id':1,'v':{'a.b':1}}]),('keydollar',[{'_id':1,'v':{'$x':1}}]),('big',[{'_id':1,'v':'y'*(16*1024*1024-200)}])):
        try: db['tc_edge_mongo_'+n].insert_many(docs)
        except Exception as e: print(port,n,'INSERT REFUSED',str(e)[:150])
    db['tc_edge_mongo_mixed'].insert_many([{'_id':i+1,'v':v} for i,v in enumerate(mixed)])
    for n in db.list_collection_names(): print(port,n,db[n].count_documents({}))
for p in sys.argv[1:]: seed(int(p))
